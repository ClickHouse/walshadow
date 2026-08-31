#define CHC_IMPLEMENTATION
#define PGCH_IMPLEMENTATION

#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "varatt.h"

#include "clickhouse.h"
#include "pg-clickhouse.h"
#include "pg-clickhouse-encode.h"

#include "walshadow.h"

/* hstore_to_matrix returns Nx2 text arrays accepted as Map entries */
#define WS_HSTORE_EXPANDER "hstore_to_matrix"

/* Minimum per column: source OID, typmod, two length prefixes */
#define WS_COL_META_BYTES	16

typedef struct WsNativeCol
{
	Oid			source_oid;
	int32		source_typmod;
	const char *name;
	int			name_len;
	chc_type   *type;

	int16		typlen;
	bool		typbyval;
	FmgrInfo	infn;
	Oid			ioparam;
	bool		infn_ready;

	FmgrInfo	expander;

	Oid			native_oid;
} WsNativeCol;

typedef struct WsCellContext
{
	const WsNativeCol *col;
	uint32		col_index;
	uint32		row;
} WsCellContext;

typedef struct WsRespIo
{
	chc_io		io;
	StringInfo	out;
} WsRespIo;

static int
ws_resp_write(void *ud, const void *buf, size_t len, chc_err *err)
{
	WsRespIo   *self = (WsRespIo *) ud;

	if (len > (size_t) (WS_MAX_RESPONSE_BYTES - self->out->len))
	{
		snprintf(err->msg, sizeof(err->msg),
				 "Native block exceeds %d byte response cap",
				 WS_MAX_RESPONSE_BYTES);
		return CHC_ERR_IO;
	}
	appendBinaryStringInfo(self->out, buf, (int) len);
	return CHC_OK;
}

static void
ws_cell_errcontext(void *arg)
{
	WsCellContext *ctx = (WsCellContext *) arg;

	if (ctx->col == NULL)
		return;
	errcontext("walshadow oracle column %u (\"%.*s\"), row %u, source type %u",
			   ctx->col_index, ctx->col->name_len, ctx->col->name,
			   ctx->row, ctx->col->source_oid);
}

static const char *
ws_get_lenstr(StringInfo req, int *out_len)
{
	uint32		len = pq_getmsgint(req, 4);

	if (len > (uint32) (req->len - req->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("walshadow string length %u past end of request", len)));
	*out_len = (int) len;
	return pq_getmsgbytes(req, (int) len);
}

/* Rebuild Datum from on-disk body with varlena header stripped */
static Datum
ws_reconstruct_datum(const WsNativeCol *col, const char *body, uint32 len)
{
	if (col->typlen == -1)
	{
		/* Restore varlena header expected by Datum consumers */
		bytea	   *raw = (bytea *) palloc(VARHDRSZ + len);

		SET_VARSIZE(raw, VARHDRSZ + len);
		memcpy(VARDATA(raw), body, len);
		return PointerGetDatum(raw);
	}
	if (col->typlen == -2)
	{
		char	   *s = (char *) palloc(len + 1);

		memcpy(s, body, len);
		s[len] = '\0';
		return PointerGetDatum(s);
	}
	if (col->typlen <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("non-positive typlen %d for oid %u",
						col->typlen, col->source_oid)));
	if (len < (uint32) col->typlen)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("raw body %u shorter than typlen %d for oid %u",
						len, col->typlen, col->source_oid)));
	if (col->typbyval)
	{
		Datum		d = 0;
		Size		n = Min((Size) col->typlen, sizeof(Datum));

		memcpy(&d, body, n);
		return d;
	}
	else
	{
		char	   *p = (char *) palloc(col->typlen);

		memcpy(p, body, col->typlen);
		return PointerGetDatum(p);
	}
}

/* Attribute defaults carry typinput text, not on-disk bodies */
static Datum
ws_input_datum(WsNativeCol *col, const char *body, uint32 len)
{
	char	   *s = pnstrdup(body, len);
	Datum		val;

	if (!col->infn_ready)
	{
		Oid			infunc;

		getTypeInputInfo(col->source_oid, &infunc, &col->ioparam);
		fmgr_info_cxt(infunc, &col->infn, CurrentMemoryContext);
		col->infn_ready = true;
	}
	val = InputFunctionCall(&col->infn, s, col->ioparam, col->source_typmod);
	pfree(s);
	return val;
}

/* Require one expander owned by source type's extension */
static bool
ws_find_hstore_expander(Oid source_oid, Oid *out)
{
	Oid			ext = getExtensionOfObject(TypeRelationId, source_oid);
	CatCList   *list;
	Oid			found = InvalidOid;
	int			nfound = 0;

	if (!OidIsValid(ext))
		return false;
	list = SearchSysCacheList1(PROCNAMEARGSNSP,
							   CStringGetDatum(WS_HSTORE_EXPANDER));
	for (int i = 0; i < list->n_members; i++)
	{
		HeapTuple	tup = &list->members[i]->tuple;
		Form_pg_proc form = (Form_pg_proc) GETSTRUCT(tup);

		if (form->pronargs != 1 || form->proargtypes.values[0] != source_oid)
			continue;
		if (getExtensionOfObject(ProcedureRelationId, form->oid) != ext)
			continue;
		found = form->oid;
		nfound++;
	}
	ReleaseSysCacheList(list);
	if (nfound != 1)
		return false;
	*out = found;
	return true;
}

static void
ws_resolve_expander(WsNativeCol *col)
{
	const chc_type *root = col->type;
	chc_kind	kind;
	Oid			funcid;

	while ((kind = chc_type_kind(root)) == CHC_NULLABLE ||
		   kind == CHC_LOW_CARDINALITY)
		root = chc_type_child(root, 0);

	if (kind == CHC_MAP && ws_find_hstore_expander(col->source_oid, &funcid))
		fmgr_info_cxt(funcid, &col->expander, CurrentMemoryContext);
}

static Datum
ws_expand(WsNativeCol *col, Datum val, Oid *valtype)
{
	if (!OidIsValid(col->expander.fn_oid))
		return val;
	*valtype = TEXTARRAYOID;
	return FunctionCall1(&col->expander, val);
}

/* Default absent values to NULL or empty non-nullable shapes */
static void
ws_append_default(pgch_writer *w, size_t col, const WsNativeCol *c)
{
	const chc_type *root = c->type;
	chc_kind	kind = chc_type_kind(root);

	if (kind == CHC_LOW_CARDINALITY)
	{
		root = chc_type_child(root, 0);
		kind = chc_type_kind(root);
	}
	switch (kind)
	{
		case CHC_NULLABLE:
		case CHC_ARRAY:
		case CHC_MAP:
			pgch_append_datum(w, col, (Datum) 0, c->native_oid, true);
			return;
		case CHC_STRING:
			pgch_append_datum(w, col, PointerGetDatum(cstring_to_text("")),
							  TEXTOID, false);
			return;
		case CHC_JSON:
		case CHC_OBJECT:
			pgch_append_datum(w, col,
							  DirectFunctionCall1(json_in, CStringGetDatum("{}")),
							  JSONOID, false);
			return;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_NOT_NULL_VIOLATION),
					 errmsg("no default value for ClickHouse type \"%s\"",
							chc_type_name(c->type, NULL))));
	}
}

static void
ws_append_cell(pgch_writer *w, size_t col, WsNativeCol *c, StringInfo req)
{
	uint8		tag = pq_getmsgbyte(req);
	const char *body;
	uint32		len;
	Datum		val;
	Oid			valtype = c->source_oid;

	if (tag == WS_CELL_DEFAULT)
	{
		ws_append_default(w, col, c);
		return;
	}
	if (tag != WS_CELL_DISK_RAW && tag != WS_CELL_TEXT && tag != WS_CELL_LITERAL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown walshadow cell tag %u", tag)));
	if (tag != WS_CELL_LITERAL && !OidIsValid(c->source_oid))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("walshadow cell carries a value but declares no source type")));

	len = pq_getmsgint(req, 4);
	if (len > (uint32) (req->len - req->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("walshadow cell length %u past end of request", len)));
	body = pq_getmsgbytes(req, (int) len);

	if (tag == WS_CELL_LITERAL)
	{
		/* Literal bypasses source type conversion */
		pgch_append_datum(w, col,
						  PointerGetDatum(cstring_to_text_with_len(body, len)),
						  TEXTOID, false);
		return;
	}
	val = tag == WS_CELL_DISK_RAW
		? ws_reconstruct_datum(c, body, len)
		: ws_input_datum(c, body, len);
	val = ws_expand(c, val, &valtype);
	pgch_append_datum(w, col, val, valtype, false);
}

void
ws_handle_encode_native(StringInfo req, StringInfo resp)
{
	uint32		n_rows = pq_getmsgint(req, 4);
	uint32		n_cols = pq_getmsgint(req, 4);
	WsNativeCol *cols;
	pgch_col   *decls;
	pgch_writer *w;
	WsCellContext cellctx = {NULL, 0, 0};
	pgch_checkpoint checkpoint = {0};
	ErrorContextCallback errcb;
	WsRespIo	rio;
	chc_block_opts opts = {0};
	chc_err		err = {0};

	if (n_rows == 0 || n_cols == 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("walshadow oracle request has %u rows and %u columns",
						n_rows, n_cols)));
	/* Bound allocations by minimum wire size */
	if ((uint64) n_cols * (WS_COL_META_BYTES + n_rows) >
		(uint64) (req->len - req->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("walshadow oracle request declares %u x %u cells, %d bytes remain",
						n_cols, n_rows, req->len - req->cursor)));

	cols = (WsNativeCol *) palloc0(sizeof(WsNativeCol) * n_cols);
	decls = (pgch_col *) palloc0(sizeof(pgch_col) * n_cols);
	for (uint32 i = 0; i < n_cols; i++)
	{
		WsNativeCol *c = &cols[i];
		const char *type_name;
		int			type_len;

		c->source_oid = (Oid) pq_getmsgint(req, 4);
		c->source_typmod = (int32) pq_getmsgint(req, 4);
		c->name = ws_get_lenstr(req, &c->name_len);
		type_name = ws_get_lenstr(req, &type_len);
		if (c->name_len == 0 || type_len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("walshadow oracle column %u has an empty name or type", i)));

		if (chc_type_parse(type_name, type_len, &pgch_alloc, &c->type, &err) != CHC_OK)
			pgch_raise(&err, ERRCODE_INVALID_PARAMETER_VALUE, "target type: ", NULL);
		c->native_oid = pgch_native_oid_for(c->type, NULL);
		if (OidIsValid(c->source_oid))
		{
			get_typlenbyval(c->source_oid, &c->typlen, &c->typbyval);
			ws_resolve_expander(c);
		}

		decls[i].name = c->name;
		decls[i].name_len = c->name_len;
		decls[i].type = c->type;
	}

	w = pgch_writer_new(CurrentMemoryContext, decls, n_cols);
	/* CH forbids Nullable(Array) and Nullable(Map), default both to empty */
	pgch_writer_set_null_array(w, PGCH_NULL_ARRAY_EMPTY);

	errcb.callback = ws_cell_errcontext;
	errcb.arg = &cellctx;
	errcb.previous = error_context_stack;
	error_context_stack = &errcb;
	for (uint32 r = 0; r < n_rows; r++)
	{
		MemoryContext rowctx = CurrentMemoryContext;

		cellctx.row = r;
		pgch_writer_checkpoint(w, &checkpoint);
		PG_TRY();
		{
			for (uint32 i = 0; i < n_cols; i++)
			{
				cellctx.col = &cols[i];
				cellctx.col_index = i;
				ws_append_cell(w, i, &cols[i], req);
			}
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(rowctx);
			pgch_writer_rollback(w, &checkpoint);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	pgch_checkpoint_free(&checkpoint);
	error_context_stack = errcb.previous;

	if (pgch_writer_rows(w) != n_rows)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("walshadow oracle wrote %zu rows, expected %u",
						pgch_writer_rows(w), n_rows)));

	/* Block occupies response remainder */
	pq_sendbyte(resp, WS_STATUS_OK);
	rio.io.ud = &rio;
	rio.io.read = NULL;
	rio.io.write = ws_resp_write;
	rio.io.check_cancel = NULL;
	rio.out = resp;
	if (chc_block_write(&rio.io, pgch_writer_build(w), &opts, &err) != CHC_OK)
		pgch_raise(&err, ERRCODE_FDW_ERROR, "block write: ", NULL);
}
