//! Direct tuple-view encoding must write the same blocks, and fail with the
//! same errors, as decoding owned cells first

use clickhouse_c::Allocator;
use walrus::pg::walparser::RelFileNode;

use crate::column_rules::{ColumnRule, ColumnRules, ColumnRulesBuilder, Substitute};
use crate::decode::heap_decoder::{
    ColumnValue, CommittedTuple, DecodedHeap, HeapOp, decode_block_data, view_block_data,
};
use crate::emit::ch_emitter::{
    ColumnBuf, Direct, OP_DELETE, OP_INSERT, RowStamp, TableEncoder, TablePlan,
};
use crate::mapping::{ColumnMapping, SystemColumns, TableMapping, TableTarget};
use crate::schema::*;
use crate::table_rules::MatchKind;

/// One source column, its target, and stored datums to try, `None` for NULL
struct Case {
    oid: u32,
    len: i16,
    align: char,
    type_name: &'static str,
    target: &'static str,
    datums: Vec<Option<Vec<u8>>>,
}

fn case(oid: u32, len: i16, align: char, target: &'static str, datums: Vec<Vec<u8>>) -> Case {
    let mut datums: Vec<_> = datums.into_iter().map(Some).collect();
    datums.push(None);
    Case {
        oid,
        len,
        align,
        type_name: "",
        target,
        datums,
    }
}

fn attr(attnum: i16, oid: u32, len: i16, align: char, type_name: &str) -> RelAttr {
    RelAttr {
        attnum,
        name: format!("c{attnum}"),
        type_oid: oid,
        typmod: -1,
        not_null: false,
        dropped: false,
        type_name: type_name.into(),
        type_byval: len > 0 && len <= 8,
        type_len: len,
        type_align: align,
        type_storage: if len == -1 { 'x' } else { 'p' },
        missing_default: None,
    }
}

fn rel(attributes: Vec<RelAttr>) -> RelDescriptor {
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16385,
        },
        oid: 16385,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", "t"),
        kind: 'r',
        persistence: 'p',
        ancestors: Vec::new(),
        replident: ReplIdent::Full { pk_attnums: None },
        attributes,
    }
}

fn short(content: &[u8]) -> Vec<u8> {
    let mut v = vec![(((1 + content.len()) as u8) << 1) | 1];
    v.extend_from_slice(content);
    v
}

fn long(content: &[u8]) -> Vec<u8> {
    let mut v = (((4 + content.len()) as u32) << 2).to_le_bytes().to_vec();
    v.extend_from_slice(content);
    v
}

fn lz4(content: &[u8]) -> Vec<u8> {
    let body = lz4::block::compress(content, None, false).unwrap();
    let mut v = ((((8 + body.len()) as u32) << 2) | 0b10)
        .to_le_bytes()
        .to_vec();
    v.extend_from_slice(&(content.len() as u32 | (1 << 30)).to_le_bytes());
    v.extend_from_slice(&body);
    v
}

fn external() -> Vec<u8> {
    let mut v = vec![0x01, 18];
    for word in [100i32 as u32, 96, 7, 16400] {
        v.extend_from_slice(&word.to_le_bytes());
    }
    v
}

/// Datums as `heap_fill_tuple` lays them out after a WAL `xl_heap_header`,
/// first `natts` attributes stored and later ones left to fast defaults
fn tuple(attrs: &[RelAttr], natts: usize, values: &[Option<&[u8]>]) -> Vec<u8> {
    let has_null = values[..natts].iter().any(Option::is_none);
    let bitmap_len = if has_null { natts.div_ceil(8) } else { 0 };
    let hoff = (23 + bitmap_len).next_multiple_of(8);
    let mut bitmap = vec![0u8; bitmap_len];
    let mut data = Vec::new();
    for (i, (att, v)) in attrs.iter().zip(values).take(natts).enumerate() {
        let Some(v) = v else { continue };
        if has_null {
            bitmap[i / 8] |= 1 << (i % 8);
        }
        // Short and external varlenas skip alignment
        if !(att.type_len == -1 && v[0] & 1 == 1) {
            let by = match att.type_align {
                's' => 2,
                'i' => 4,
                'd' => 8,
                _ => 1,
            };
            data.resize(data.len().next_multiple_of(by), 0);
        }
        data.extend_from_slice(v);
    }
    let mut out = (natts as u16).to_le_bytes().to_vec();
    out.extend_from_slice(&u16::from(has_null).to_le_bytes());
    out.push(hoff as u8);
    out.extend_from_slice(&bitmap);
    out.resize(5 + hoff - 23, 0);
    out.extend_from_slice(&data);
    out
}

fn mapping_of(rel: &RelDescriptor, targets: &[&str]) -> TableMapping {
    TableMapping {
        target: TableTarget::new("default", "t"),
        columns: rel
            .attributes
            .iter()
            .zip(targets)
            .filter(|(a, _)| !a.dropped)
            .map(|(a, t)| ColumnMapping {
                src_attnum: a.attnum,
                target_name: a.name.clone(),
                target_type: (*t).into(),
                type_pinned: false,
            })
            .collect(),
        tees: Vec::new(),
    }
}

fn plan(rel: &RelDescriptor, targets: &[&str], rules: &ColumnRules) -> TablePlan {
    TablePlan::build(
        Allocator::global(&mimalloc::MiMalloc),
        rel,
        &mapping_of(rel, targets),
        rules,
        &SystemColumns::default(),
    )
    .unwrap()
}

/// Encode every row both ways; blocks or first errors must match exactly
fn assert_parity(
    rel: &RelDescriptor,
    targets: &[&str],
    rules: &ColumnRules,
    rows: &[(usize, Vec<Option<&[u8]>>)],
) {
    let mapping = mapping_of(rel, targets);
    let mut cells = TableEncoder::new(plan(rel, targets, rules)).unwrap();
    let mut direct = TableEncoder::new(plan(rel, targets, rules)).unwrap();
    for (i, (natts, values)) in rows.iter().enumerate() {
        let data = tuple(&rel.attributes, *natts, values);
        let op = if i % 3 == 2 { OP_DELETE } else { OP_INSERT };
        let stamp = RowStamp {
            source_lsn: 0x1000 + i as u64,
            xid: 700 + i as u32,
            commit_ts: -86_400_000_000 * i as i64,
            op_code: op,
            leaf: None,
        };
        let mut decoded = decode_block_data(&data, rel).unwrap();
        // Placement renders PostGIS cells before batching
        crate::ops::oracle::render_ext_columns(&rel.attributes, &mut decoded.columns);
        let committed = CommittedTuple {
            decoded: DecodedHeap {
                rfn: rel.rfn,
                xid: stamp.xid,
                source_lsn: stamp.source_lsn,
                op: if op == OP_DELETE {
                    HeapOp::Delete
                } else {
                    HeapOp::Insert
                },
                new: (op != OP_DELETE).then(|| decoded.clone()),
                old: (op == OP_DELETE).then_some(decoded),
            },
            commit_ts: stamp.commit_ts,
            commit_lsn: 0,
        };
        let view = view_block_data(&data, rel).unwrap();
        assert_eq!(view.to_tuple(rel).columns.len(), rel.attributes.len());
        let a = cells
            .append_routed(&committed, &mapping, op, None)
            .map_err(|e| e.to_string());
        let b = direct
            .append_view(rel, &view.datums, &stamp)
            .map_err(|e| e.to_string());
        assert_eq!(a, b, "row {i}: {values:?}");
        if a.is_err() {
            return;
        }
    }
    let (a, na) = cells.take_block().unwrap();
    let (b, nb) = direct.take_block().unwrap();
    assert_eq!(na, nb);
    let wire = |bufs: &[ColumnBuf]| {
        bufs.iter()
            .map(|b| format!("{:?}", b.wire()))
            .collect::<Vec<_>>()
    };
    assert_eq!(wire(&a), wire(&b));
}

/// Each case in its own column between an int4 and a text column, so
/// alignment of neighbours runs through both walkers
fn run(c: &Case, rules: &ColumnRules) {
    let rel = rel(vec![
        attr(1, INT4OID, 4, 'i', "int4"),
        attr(2, c.oid, c.len, c.align, c.type_name),
        attr(3, TEXTOID, -1, 'i', "text"),
    ]);
    let targets = ["Int32", c.target, "Nullable(String)"];
    let id = 7i32.to_le_bytes();
    let tail = short(b"tail");
    let rows: Vec<_> = c
        .datums
        .iter()
        .map(|d| (3, vec![Some(&id[..]), d.as_deref(), Some(&tail[..])]))
        .collect();
    assert_parity(&rel, &targets, rules, &rows);
}

fn ints<const N: usize>(vals: impl IntoIterator<Item = [u8; N]>) -> Vec<Vec<u8>> {
    vals.into_iter().map(|b| b.to_vec()).collect()
}

fn numeric_short(neg: bool, dscale: u16, digits: &[i16]) -> Vec<u8> {
    let header = 0x8000 | if neg { 0x2000 } else { 0 } | ((dscale << 7) & 0x1F80);
    let mut v = header.to_le_bytes().to_vec();
    for d in digits {
        v.extend_from_slice(&d.to_le_bytes());
    }
    v
}

fn name(s: &[u8]) -> Vec<u8> {
    let mut v = s.to_vec();
    v.resize(64, 0);
    v
}

fn jsonb_scalar(s: &[u8]) -> Vec<u8> {
    let mut v = 0x5000_0001u32.to_le_bytes().to_vec();
    v.extend_from_slice(&(s.len() as u32).to_le_bytes());
    v.extend_from_slice(s);
    v
}

fn rules(entries: &[(&str, ColumnRule)]) -> ColumnRules {
    let mut b = ColumnRulesBuilder::new();
    for (attname, rule) in entries {
        b.add(
            &RelName::new("public", "t"),
            MatchKind::Exact,
            attname,
            MatchKind::Exact,
            rule.clone(),
        );
    }
    b.finish().0
}

fn target_type(ty: &str) -> ColumnRules {
    rules(&[(
        "c2",
        ColumnRule {
            target_type: Some(ty.into()),
            ..ColumnRule::default()
        },
    )])
}

#[test]
fn fixed_width_types_match() {
    let f4 = [
        f32::from_bits(0x7fc0_0001),
        -0.0,
        f32::INFINITY,
        f32::MIN_POSITIVE,
    ];
    let f8 = [
        f64::from_bits(0x7ff8_0000_0000_0001),
        -0.0,
        f64::NEG_INFINITY,
        1.5,
    ];
    let cases = [
        case(BOOLOID, 1, 'c', "Bool", vec![vec![0], vec![1]]),
        case(CHAROID, 1, 'c', "Int8", vec![vec![0x80], vec![65]]),
        case(
            INT2OID,
            2,
            's',
            "Int16",
            ints([i16::MIN, -1, 0, i16::MAX].map(i16::to_le_bytes)),
        ),
        case(
            INT4OID,
            4,
            'i',
            "Nullable(Int32)",
            ints([i32::MIN, -1, 0, i32::MAX].map(i32::to_le_bytes)),
        ),
        case(
            INT8OID,
            8,
            'd',
            "Int64",
            ints([i64::MIN, -1, 0, i64::MAX].map(i64::to_le_bytes)),
        ),
        case(
            MONEYOID,
            8,
            'd',
            "Int64",
            ints([-12_345i64, 0, 99].map(i64::to_le_bytes)),
        ),
        case(FLOAT4OID, 4, 'i', "Float32", ints(f4.map(f32::to_le_bytes))),
        case(
            FLOAT8OID,
            8,
            'd',
            "Nullable(Float64)",
            ints(f8.map(f64::to_le_bytes)),
        ),
        case(
            OIDOID,
            4,
            'i',
            "UInt32",
            ints([0u32, u32::MAX].map(u32::to_le_bytes)),
        ),
        case(XIDOID, 4, 'i', "UInt32", ints([3u32].map(u32::to_le_bytes))),
        case(CIDOID, 4, 'i', "UInt32", ints([9u32].map(u32::to_le_bytes))),
        case(
            OID8OID,
            8,
            'd',
            "UInt64",
            ints([u64::MAX].map(u64::to_le_bytes)),
        ),
        case(
            XID8OID,
            8,
            'd',
            "UInt64",
            ints([1u64 << 40].map(u64::to_le_bytes)),
        ),
        case(
            PG_LSNOID,
            8,
            'd',
            "UInt64",
            ints([0x16_B374_D848u64].map(u64::to_le_bytes)),
        ),
        case(
            DATEOID,
            4,
            'i',
            "Date32",
            ints([-1i32, 0, 8_000].map(i32::to_le_bytes)),
        ),
        case(
            TIMEOID,
            8,
            'd',
            "Time64(6)",
            ints([45_296_000_000i64, 0].map(i64::to_le_bytes)),
        ),
        case(
            TIMESTAMPOID,
            8,
            'd',
            "DateTime64(6, 'UTC')",
            ints([-1i64, 0, 1_700_000_000_000_000].map(i64::to_le_bytes)),
        ),
        case(
            TIMESTAMPTZOID,
            8,
            'd',
            "Nullable(DateTime64(3, 'UTC'))",
            ints([-1_999i64, 1_999].map(i64::to_le_bytes)),
        ),
        case(
            TIMETZOID,
            12,
            'd',
            "String",
            vec![
                [
                    45_296_000_000i64.to_le_bytes().as_slice(),
                    &(-7200i32).to_le_bytes(),
                ]
                .concat(),
            ],
        ),
        case(
            UUIDOID,
            16,
            'c',
            "UUID",
            vec![(0u8..16).collect(), vec![0xff; 16]],
        ),
        case(
            INTERVALOID,
            16,
            'd',
            "String",
            vec![
                [
                    1_000_000i64.to_le_bytes().as_slice(),
                    &3i32.to_le_bytes(),
                    &14i32.to_le_bytes(),
                ]
                .concat(),
            ],
        ),
        case(
            NAMEOID,
            64,
            'c',
            "String",
            vec![name(b"abc"), name(b""), name(b"\xc3\xa9t\xc3\xa9")],
        ),
        case(
            MACADDROID,
            6,
            'i',
            "String",
            vec![vec![0x08, 0, 0x2b, 1, 2, 0xab]],
        ),
        case(MACADDR8OID, 8, 'i', "String", vec![vec![0xfe; 8]]),
        case(TIDOID, 6, 's', "String", vec![vec![1, 0, 2, 0, 3, 0]]),
    ];
    for c in &cases {
        run(c, &ColumnRules::default());
    }
}

#[test]
fn copy_requires_matching_storage() {
    let cases = [
        (INT4OID, 4, 'i', "Int32", Direct::Copy),
        (INT4OID, 4, 'i', "UInt32", Direct::Value),
        (INT4OID, 4, 'i', "Float32", Direct::Value),
        (OIDOID, 4, 'i', "Int32", Direct::Value),
        (FLOAT8OID, 8, 'd', "Int64", Direct::Value),
        (INT8OID, 8, 'd', "Nullable(Int64)", Direct::Copy),
        (MONEYOID, 8, 'd', "Int64", Direct::Copy),
        (DATEOID, 4, 'i', "Date32", Direct::Value),
        (BOOLOID, 1, 'c', "Bool", Direct::Value),
    ];
    for (oid, len, align, target, want) in cases {
        let rel = rel(vec![attr(1, oid, len, align, "")]);
        let plan = plan(&rel, &[target], &ColumnRules::default());
        assert_eq!(
            plan.columns[0].source.unwrap().direct,
            want,
            "{oid} {target}"
        );
    }
    let rel = rel(vec![attr(1, 999_999, -1, 'd', "geometry")]);
    let plan = plan(&rel, &["String"], &ColumnRules::default());
    assert_eq!(plan.columns[0].source.unwrap().direct, Direct::Wkt);
}

#[test]
fn varlena_types_match() {
    let long_text = vec![b'x'; 300];
    let inet4 = [2u8, 24, 0, 4, 10, 1, 2, 0];
    let cases = [
        case(
            TEXTOID,
            -1,
            'i',
            "String",
            vec![
                short(b""),
                short("héllo".as_bytes()),
                long(&long_text),
                lz4(&long_text),
                short(b"\xff\xfe"),
            ],
        ),
        case(
            VARCHAROID,
            -1,
            'i',
            "Nullable(String)",
            vec![long(b"abc"), lz4(&long_text)],
        ),
        case(BPCHAROID, -1, 'i', "String", vec![short(b"ab  ")]),
        case(
            BYTEAOID,
            -1,
            'i',
            "String",
            vec![short(&[0, 1, 0xff]), long(&[]), lz4(&long_text)],
        ),
        case(JSONOID, -1, 'i', "String", vec![short(br#"{"a": 1}"#)]),
        case(
            JSONBOID,
            -1,
            'i',
            "String",
            vec![short(&jsonb_scalar(b"abc")), long(&jsonb_scalar(b""))],
        ),
        case(
            NUMERICOID,
            -1,
            'i',
            "String",
            vec![
                short(&numeric_short(false, 2, &[1, 5000])),
                short(&numeric_short(true, 0, &[42])),
                short(&0xC000u16.to_le_bytes()),
                short(&0xD000u16.to_le_bytes()),
            ],
        ),
        case(
            NUMERICOID,
            -1,
            'i',
            "Decimal(10, 2)",
            vec![
                short(&numeric_short(false, 2, &[1, 5000])),
                short(&numeric_short(true, 0, &[42])),
            ],
        ),
        case(
            INETOID,
            -1,
            'i',
            "String",
            vec![short(&inet4), short(&[9, 9])],
        ),
        case(
            CIDROID,
            -1,
            'i',
            "String",
            vec![short(&[2, 8, 1, 4, 10, 0, 0, 0])],
        ),
    ];
    for c in &cases {
        run(c, &ColumnRules::default());
    }
}

#[test]
fn errors_match() {
    let cases = [
        // Unresolved pointer reaches encoder in both paths
        case(TEXTOID, -1, 'i', "String", vec![external()]),
        // Non-finite numeric into Decimal rejects
        case(
            NUMERICOID,
            -1,
            'i',
            "Decimal(10, 2)",
            vec![short(&0xC000u16.to_le_bytes())],
        ),
        // Temporal infinity rejects without substitute
        case(
            DATEOID,
            4,
            'i',
            "Date32",
            vec![i32::MAX.to_le_bytes().to_vec()],
        ),
        // Invalid name bytes stay unsupported
        case(NAMEOID, 64, 'c', "String", vec![name(b"\xff")]),
    ];
    for c in &cases {
        run(c, &ColumnRules::default());
    }
}

#[test]
fn overrides_and_substitutes_match() {
    let ints4 = ints([i32::MIN, -1, 0, i32::MAX].map(i32::to_le_bytes));
    run(
        &case(INT4OID, 4, 'i', "Int32", ints4.clone()),
        &target_type("UInt32"),
    );
    run(
        &case(INT4OID, 4, 'i', "Int32", ints4),
        &target_type("Nullable(Int32)"),
    );
    run(
        &case(
            NUMERICOID,
            -1,
            'i',
            "Decimal(10, 2)",
            vec![short(&numeric_short(false, 0, &[7]))],
        ),
        &target_type("Int64"),
    );
    let inf = ColumnRule {
        nan: Some(Substitute::Literal("0".into())),
        pos_inf: Some(Substitute::Max),
        neg_inf: Some(Substitute::Null),
        ..ColumnRule::default()
    };
    run(
        &case(
            NUMERICOID,
            -1,
            'i',
            "Nullable(Decimal(10, 2))",
            vec![
                short(&0xC000u16.to_le_bytes()),
                short(&0xD000u16.to_le_bytes()),
                short(&0xF000u16.to_le_bytes()),
                short(&numeric_short(false, 2, &[1, 5000])),
            ],
        ),
        &rules(&[("c2", inf)]),
    );
    let temporal = ColumnRule {
        pos_inf: Some(Substitute::Null),
        neg_inf: Some(Substitute::Null),
        ..ColumnRule::default()
    };
    run(
        &case(
            TIMESTAMPTZOID,
            8,
            'd',
            "Nullable(DateTime64(6, 'UTC'))",
            ints([i64::MAX, i64::MIN, 5].map(i64::to_le_bytes)),
        ),
        &rules(&[("c2", temporal)]),
    );
}

#[test]
fn oracle_columns_match() {
    // Outside local matrix: raw body reaches oracle request unchanged
    run(
        &case(
            3614,
            -1,
            'i',
            "String",
            vec![short(b"\x01\x02"), lz4(&[7; 300])],
        ),
        &ColumnRules::default(),
    );
    // Composite target routes even local types to oracle
    run(
        &case(TEXTOID, -1, 'i', "Array(String)", vec![short(b"{a,b}")]),
        &ColumnRules::default(),
    );
    run(
        &case(
            JSONBOID,
            -1,
            'i',
            "Map(String, String)",
            vec![short(&jsonb_scalar(b"x"))],
        ),
        &ColumnRules::default(),
    );
    let mut point = vec![0, 0, 0, 0];
    point.extend_from_slice(&1u32.to_le_bytes());
    point.extend_from_slice(&[0; 8]);
    point.extend_from_slice(&1.5f64.to_le_bytes());
    point.extend_from_slice(&(-2.0f64).to_le_bytes());
    let geometry = Case {
        type_name: "geometry",
        ..case(
            999_999,
            -1,
            'd',
            "String",
            vec![long(&point), short(&[0, 0, 0, 0, 3, 0, 0, 0])],
        )
    };
    run(&geometry, &ColumnRules::default());
}

#[test]
fn defaults_dropped_and_absent_match() {
    let mut attrs = vec![
        attr(1, INT4OID, 4, 'i', "int4"),
        attr(2, TEXTOID, -1, 'i', "text"),
        attr(3, INT8OID, 8, 'd', "int8"),
        attr(4, TEXTOID, -1, 'i', "text"),
        attr(5, NUMERICOID, -1, 'i', "numeric"),
        attr(6, 3614, -1, 'i', "tsvector"),
    ];
    attrs[1].dropped = true;
    attrs[2].missing_default = Some(MissingDefault::Text("-5".into()));
    attrs[3].missing_default = Some(MissingDefault::Raw(
        crate::decode::heap_decoder::raw_missing_array(TEXTOID, &short(b"dflt")),
    ));
    attrs[4].missing_default = Some(MissingDefault::Text("NaN".into()));
    attrs[5].missing_default = Some(MissingDefault::Text("'a'".into()));
    let rel = rel(attrs);
    let targets = [
        "Int32",
        "String",
        "Int64",
        "String",
        "Nullable(String)",
        "String",
    ];
    let one = 1i32.to_le_bytes();
    let gone = short(b"dropped");
    let big = 9i64.to_le_bytes();
    let rows = vec![
        (
            2,
            vec![Some(&one[..]), Some(&gone[..]), None, None, None, None],
        ),
        (1, vec![Some(&one[..]), None, None, None, None, None]),
        (
            3,
            vec![Some(&one[..]), None, Some(&big[..]), None, None, None],
        ),
        (6, vec![None; 6]),
    ];
    assert_parity(&rel, &targets, &ColumnRules::default(), &rows);
}

#[test]
fn predeclared_columns_match() {
    // Mapping names attnum 2, descriptor predates it
    let rel = rel(vec![attr(1, INT4OID, 4, 'i', "int4")]);
    let mapping_rel = self::rel(vec![
        attr(1, INT4OID, 4, 'i', "int4"),
        attr(2, TEXTOID, -1, 'i', "text"),
    ]);
    let targets = ["Int32", "Nullable(String)"];
    let mapping = mapping_of(&mapping_rel, &targets);
    let build = || {
        TableEncoder::new(
            TablePlan::build(
                Allocator::global(&mimalloc::MiMalloc),
                &rel,
                &mapping,
                &ColumnRules::default(),
                &SystemColumns::default(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let (mut cells, mut direct) = (build(), build());
    let one = 1i32.to_le_bytes();
    let data = tuple(&rel.attributes, 1, &[Some(&one[..])]);
    let decoded = decode_block_data(&data, &rel).unwrap();
    let stamp = RowStamp {
        source_lsn: 1,
        xid: 2,
        commit_ts: 3,
        op_code: OP_INSERT,
        leaf: None,
    };
    let committed = CommittedTuple {
        decoded: DecodedHeap {
            rfn: rel.rfn,
            xid: 2,
            source_lsn: 1,
            op: HeapOp::Insert,
            new: Some(decoded),
            old: None,
        },
        commit_ts: 3,
        commit_lsn: 0,
    };
    cells
        .append_routed(&committed, &mapping, OP_INSERT, None)
        .unwrap();
    let view = view_block_data(&data, &rel).unwrap();
    direct.append_view(&rel, &view.datums, &stamp).unwrap();
    let wire = |e: &mut TableEncoder| {
        let (bufs, _) = e.take_block().unwrap();
        bufs.iter()
            .map(|b| format!("{:?}", b.wire()))
            .collect::<Vec<_>>()
    };
    assert_eq!(wire(&mut cells), wire(&mut direct));
}

#[test]
fn view_matches_owned_decode_for_records() {
    let rel = rel(vec![
        attr(1, INT4OID, 4, 'i', "int4"),
        attr(2, TEXTOID, -1, 'i', "text"),
    ]);
    let one = 1i32.to_le_bytes();
    let text = long(b"value");
    let data = tuple(&rel.attributes, 2, &[Some(&one[..]), Some(&text[..])]);
    let view = view_block_data(&data, &rel).unwrap();
    assert_eq!(view.to_tuple(&rel), decode_block_data(&data, &rel).unwrap());
    assert_eq!(
        view.to_tuple(&rel).columns[1],
        Some(ColumnValue::Text("value".into()))
    );
}

/// Operation with new and old tuple bytes
type Change = (HeapOp, Option<Vec<u8>>, Option<Vec<u8>>);

/// Plan row changes both ways; segments must carry identical rows
fn plan_both(rel: &RelDescriptor, targets: &[&str], changes: &[Change]) {
    use crate::emit::pipeline::plan_spool::{PlanItem, PlanWriter, ViewChange};
    use crate::emit::route::RouteSnapshot;
    let rel = std::sync::Arc::new(rel.clone());
    let route = RouteSnapshot::freeze(
        std::sync::Arc::new(mapping_of(&rel, targets)),
        std::sync::Arc::default(),
        Default::default(),
    );
    let tmp = tempfile::tempdir().unwrap();
    let mut out = Vec::new();
    for views in [false, true] {
        let mut w = PlanWriter::create(tmp.path().join(format!("{views}.plan")), 1 << 20, 1 << 20)
            .for_commit(9);
        let table = w.table(&rel, 1, || Some(route.clone()));
        for (i, (op, new, old)) in changes.iter().enumerate() {
            let lsn = 0x1000 + 0x10 * i as u64;
            if views {
                let new = new.as_ref().map(|d| view_block_data(d, &rel).unwrap());
                let old = old.as_ref().map(|d| view_block_data(d, &rel).unwrap());
                let change = ViewChange {
                    op: *op,
                    xid: 5,
                    source_lsn: lsn,
                    new: new.as_ref(),
                    old: old.as_ref(),
                };
                w.push_views(change, table).unwrap();
            } else {
                let decoded = DecodedHeap {
                    rfn: rel.rfn,
                    xid: 5,
                    source_lsn: lsn,
                    op: *op,
                    new: new.as_ref().map(|d| decode_block_data(d, &rel).unwrap()),
                    old: old.as_ref().map(|d| decode_block_data(d, &rel).unwrap()),
                };
                w.push_cells(decoded, table).unwrap();
            }
        }
        let mut rd = w.seal(0x2000, 9).unwrap().replay().unwrap();
        let mut bytes = Vec::new();
        while let Some(item) = rd.next_item().unwrap() {
            if let PlanItem::Segment(s) = item {
                bytes.extend(s.chunks.iter().map(|c| (c.rows(), c.to_bytes())));
            }
        }
        out.push(bytes);
    }
    assert_eq!(out[0], out[1]);
    assert!(!out[0].is_empty());
}

#[test]
fn plan_views_match_cells_for_row_changes() {
    let int4 = |v: i32| v.to_le_bytes();
    let f8 = |v: f64| v.to_le_bytes();
    let txt = |s: &str| short(s.as_bytes());
    for replident in [
        ReplIdent::Full { pk_attnums: None },
        ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
    ] {
        let mut r = rel(vec![
            attr(1, INT4OID, 4, 'i', "int4"),
            attr(2, FLOAT8OID, 8, 'd', "float8"),
            attr(3, TEXTOID, -1, 'i', "text"),
        ]);
        r.replident = replident;
        let attrs = r.attributes.clone();
        let row = |id: i32, f: f64, t: &str| {
            tuple(
                &attrs,
                3,
                &[Some(&int4(id)[..]), Some(&f8(f)[..]), Some(&txt(t)[..])],
            )
        };
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        plan_both(
            &r,
            &["Int32", "Float64", "String"],
            &[
                (HeapOp::Insert, Some(row(1, 1.0, "a")), None),
                // Key change splits into delete marker plus new row
                (
                    HeapOp::Update,
                    Some(row(2, 1.0, "a")),
                    Some(row(1, 1.0, "a")),
                ),
                // Same key, other columns change
                (
                    HeapOp::Update,
                    Some(row(2, 2.0, "b")),
                    Some(row(2, 1.0, "a")),
                ),
                // NaN never equals itself
                (
                    HeapOp::Update,
                    Some(row(2, nan, "b")),
                    Some(row(2, nan, "b")),
                ),
                (
                    HeapOp::HotUpdate,
                    Some(row(2, 3.0, "c")),
                    Some(row(2, nan, "b")),
                ),
                (HeapOp::Update, Some(row(3, 3.0, "c")), None),
                (HeapOp::Delete, None, Some(row(3, 3.0, "c"))),
                (HeapOp::Delete, None, None),
            ],
        );
    }
}
