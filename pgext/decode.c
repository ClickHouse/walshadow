/*
 * decode.c — on-disk Datum -> typoutput text.
 *
 * Reconstructs a Datum from on-disk bytes (varlena types reuse the caller's
 * 4-byte-header bytea directly; fixed-width types are copied into a Datum
 * slot), then runs the type's typoutput function. Caller — walshadow's
 * decoder running outside shadow — hands over bytes it pulled from source's
 * WAL stream, gets back the same text PG would render from inside.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "varatt.h"

#include "walshadow.h"

char *
ws_decode_datum_text(Oid typoid, bytea *raw)
{
	const char *raw_data = VARDATA(raw);
	Size		raw_len = VARSIZE(raw) - VARHDRSZ;

	HeapTuple	type_tuple;
	Form_pg_type type_form;
	int16		typlen;
	bool		typbyval;
	Oid			typoutput;
	Datum		value;
	char	   *out;

	type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	if (!HeapTupleIsValid(type_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("unknown type oid %u", typoid)));

	type_form = (Form_pg_type) GETSTRUCT(type_tuple);
	typlen = type_form->typlen;
	typbyval = type_form->typbyval;
	typoutput = type_form->typoutput;

	if (typlen == -1)
	{
		/* varlena: bytea arg is already a 4-byte-header varlena; target
		 * typoutput consumes via VARDATA_ANY/VARSIZE_ANY_EXHDR which
		 * don't care about the wrapper's C struct tag. */
		value = PointerGetDatum(raw);
	}
	else if (typlen == -2)
	{
		/* cstring: NUL-terminated; the on-disk body is already C-string. */
		char	   *s = (char *) palloc(raw_len + 1);

		memcpy(s, raw_data, raw_len);
		s[raw_len] = '\0';
		value = PointerGetDatum(s);
	}
	else if (typbyval)
	{
		/* fixed pass-by-value: pack low bytes into a Datum */
		Datum		d = 0;
		Size		n = (Size) typlen < sizeof(Datum) ? (Size) typlen : sizeof(Datum);

		if (raw_len < n)
		{
			ReleaseSysCache(type_tuple);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("raw bytes %zu shorter than typlen %d for oid %u",
							raw_len, typlen, typoid)));
		}
		memcpy(&d, raw_data, n);
		value = d;
	}
	else
	{
		/* fixed pass-by-reference: heap-allocate typlen bytes */
		char	   *p;

		if (typlen <= 0)
		{
			ReleaseSysCache(type_tuple);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("non-positive typlen %d for oid %u",
							typlen, typoid)));
		}
		if (raw_len < (Size) typlen)
		{
			ReleaseSysCache(type_tuple);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("raw bytes %zu shorter than typlen %d for oid %u",
							raw_len, typlen, typoid)));
		}
		p = (char *) palloc(typlen);
		memcpy(p, raw_data, typlen);
		value = PointerGetDatum(p);
	}

	if (!OidIsValid(typoutput))
	{
		ReleaseSysCache(type_tuple);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("type oid %u has no typoutput function", typoid)));
	}

	out = OidOutputFunctionCall(typoutput, value);

	ReleaseSysCache(type_tuple);

	return out;
}
