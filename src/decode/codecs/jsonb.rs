//! PG `jsonb`: the `utils/jsonb.h` container tree, rendered as `jsonb_out`
//! text.
//!
//! A container is `uint32 header` (child count + array / object / scalar
//! flags), one `JEntry` per child, then the children's payloads. A `JEntry`
//! holds a payload length, except every `JB_OFFSET_STRIDE`th child, which
//! holds an end offset instead, so payload bounds only resolve by walking
//! children in order. Object children are every key ahead of every value.
//!
//! Only the root may carry `JB_FSCALAR`, PG's shape for a bare scalar: a
//! one-element array printed without its brackets.

use super::{CodecError, numeric::render_numeric_text};

const JB_CMASK: u32 = 0x0FFF_FFFF;
const JB_FSCALAR: u32 = 0x1000_0000;
const JB_FOBJECT: u32 = 0x2000_0000;

const JENTRY_OFFLENMASK: u32 = 0x0FFF_FFFF;
const JENTRY_TYPEMASK: u32 = 0x7000_0000;
const JENTRY_HAS_OFF: u32 = 0x8000_0000;
const JENTRY_ISSTRING: u32 = 0x0000_0000;
const JENTRY_ISNUMERIC: u32 = 0x1000_0000;
const JENTRY_ISBOOL_FALSE: u32 = 0x2000_0000;
const JENTRY_ISBOOL_TRUE: u32 = 0x3000_0000;
const JENTRY_ISNULL: u32 = 0x4000_0000;
const JENTRY_ISCONTAINER: u32 = 0x5000_0000;

const JSONB_MAX_DEPTH: usize = 1000;

/// Decode `jsonb` varlena body to PG `jsonb_out` text (`JsonbToCString`)
pub fn decode_jsonb(body: &[u8]) -> Result<String, CodecError> {
    let mut out = String::with_capacity(body.len());
    render_jsonb_container(body, 0, &mut out)?;
    Ok(out)
}

#[derive(Clone, Copy)]
struct Container<'a> {
    header: u32,
    entries: &'a [[u8; 4]],
    data: &'a [u8],
}

impl<'a> Container<'a> {
    fn parse(container: &'a [u8]) -> Result<Self, CodecError> {
        let header = container
            .get(..4)
            .ok_or(CodecError::BadJsonbContainer("header past end"))?;
        let header = u32::from_le_bytes(header.try_into().unwrap());
        let count = (header & JB_CMASK) as usize;
        let n_children = if header & JB_FOBJECT != 0 {
            2 * count
        } else {
            count
        };
        let entries = container
            .get(4..4 + 4 * n_children)
            .ok_or(CodecError::BadJsonbContainer("child entries past end"))?;
        Ok(Self {
            header,
            entries: entries.as_chunks::<4>().0,
            data: &container[4 + 4 * n_children..],
        })
    }

    /// PG `getJsonbOffset`: a start offset sums the lengths back to the
    /// nearest offset child
    fn start_of(&self, index: usize) -> usize {
        let mut start = 0;
        for entry in self.entries[..index].iter().rev() {
            let entry = u32::from_le_bytes(*entry);
            start += (entry & JENTRY_OFFLENMASK) as usize;
            if entry & JENTRY_HAS_OFF != 0 {
                break;
            }
        }
        start
    }

    /// Children resolve in order, so an object walks its keys and its values
    /// from two seats rather than materializing every bound
    fn walk(self, index: usize) -> Walk<'a> {
        Walk {
            container: self,
            index,
            start: self.start_of(index),
        }
    }
}

struct Walk<'a> {
    container: Container<'a>,
    index: usize,
    start: usize,
}

impl Walk<'_> {
    fn next_child(&mut self) -> Result<(u32, std::ops::Range<usize>), CodecError> {
        let entry = u32::from_le_bytes(self.container.entries[self.index]);
        let start = self.start;
        let field = (entry & JENTRY_OFFLENMASK) as usize;
        // PG `JBE_ADVANCE_OFFSET`: an offset child restarts the walk
        let end = if entry & JENTRY_HAS_OFF != 0 {
            field
        } else {
            start + field
        };
        if end < start || end > self.container.data.len() {
            return Err(CodecError::BadJsonbContainer("child data past end"));
        }
        self.index += 1;
        self.start = end;
        Ok((entry, start..end))
    }
}

fn render_jsonb_container(
    container: &[u8],
    depth: usize,
    out: &mut String,
) -> Result<(), CodecError> {
    if depth > JSONB_MAX_DEPTH {
        return Err(CodecError::BadJsonbContainer("nesting past depth bound"));
    }
    let jb = Container::parse(container)?;
    let n_children = jb.entries.len();
    if jb.header & JB_FSCALAR != 0 {
        if n_children == 0 {
            return Err(CodecError::BadJsonbContainer("scalar without a child"));
        }
        let (entry, range) = jb.walk(0).next_child()?;
        return render_jsonb_value(entry, jb.data, range, depth, out);
    }
    if jb.header & JB_FOBJECT != 0 {
        let pairs = n_children / 2;
        let mut keys = jb.walk(0);
        let mut values = jb.walk(pairs);
        out.push('{');
        for i in 0..pairs {
            if i > 0 {
                out.push_str(", ");
            }
            let (key_entry, key) = keys.next_child()?;
            if key_entry & JENTRY_TYPEMASK != JENTRY_ISSTRING {
                return Err(CodecError::BadJsonbContainer("object key is not a string"));
            }
            render_jsonb_string(&jb.data[key], out)?;
            out.push_str(": ");
            let (entry, range) = values.next_child()?;
            render_jsonb_value(entry, jb.data, range, depth, out)?;
        }
        out.push('}');
    } else {
        let mut items = jb.walk(0);
        out.push('[');
        for i in 0..n_children {
            if i > 0 {
                out.push_str(", ");
            }
            let (entry, range) = items.next_child()?;
            render_jsonb_value(entry, jb.data, range, depth, out)?;
        }
        out.push(']');
    }
    Ok(())
}

fn render_jsonb_value(
    entry: u32,
    data: &[u8],
    range: std::ops::Range<usize>,
    depth: usize,
    out: &mut String,
) -> Result<(), CodecError> {
    let (start, end) = (range.start, range.end);
    // PG `fillJsonbValue`: numeric and container children start `INTALIGN`ed,
    // their padding counted in the child's own length
    let aligned = || -> Result<&[u8], CodecError> {
        data.get(start.next_multiple_of(4)..end)
            .ok_or(CodecError::BadJsonbContainer("aligned child past end"))
    };
    match entry & JENTRY_TYPEMASK {
        JENTRY_ISSTRING => render_jsonb_string(&data[start..end], out)?,
        JENTRY_ISNUMERIC => {
            // Whole `Numeric` datum, 4-byte varlena header included: PG
            // detoasts before storing, so the header is never the short form
            let datum = aligned()?;
            let body = datum
                .get(4..)
                .ok_or(CodecError::BadJsonbContainer("numeric child past end"))?;
            render_numeric_text(body, out)?;
        }
        JENTRY_ISBOOL_TRUE => out.push_str("true"),
        JENTRY_ISBOOL_FALSE => out.push_str("false"),
        JENTRY_ISNULL => out.push_str("null"),
        JENTRY_ISCONTAINER => render_jsonb_container(aligned()?, depth + 1, out)?,
        _ => return Err(CodecError::BadJsonbContainer("unknown child type")),
    }
    Ok(())
}

/// PG `escape_json_char`: two-character escapes for backspace, form feed,
/// newline, carriage return, tab, quote and backslash, `\uXXXX` for the rest
/// below `0x20`, every other byte verbatim
fn render_jsonb_string(raw: &[u8], out: &mut String) -> Result<(), CodecError> {
    let s =
        std::str::from_utf8(raw).map_err(|_| CodecError::BadJsonbContainer("string not utf8"))?;
    out.push('"');
    // escapes are ASCII, so each run between them is a char-boundary slice
    let mut run = 0;
    for (i, b) in s.bytes().enumerate() {
        if b >= b' ' && b != b'"' && b != b'\\' {
            continue;
        }
        out.push_str(&s[run..i]);
        run = i + 1;
        match b {
            0x8 => out.push_str("\\b"),
            0xc => out.push_str("\\f"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            // control byte below 0x20, so one nibble of it is already known
            _ => {
                out.push_str(if b < 0x10 { "\\u000" } else { "\\u001" });
                out.push(b"0123456789abcdef"[(b & 0xf) as usize] as char);
            }
        }
    }
    out.push_str(&s[run..]);
    out.push('"');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PG 17 `pageinspect`: (`encode(t_data, 'hex')`, `j::text`), one `jsonb` column
    const JSONB_VECTORS: &[(&str, &str)] = &[
        (
            "3301000020010000800b000010610000002000000000800100",
            "{\"a\": 1}",
        ),
        ("0b00000020", "{}"),
        ("0b00000040", "[]"),
        ("1301000050000000c0", "null"),
        ("1301000050000000b0", "true"),
        ("1301000050000000a0", "false"),
        ("2301000050080000902000000000802a00", "42"),
        ("23010000500800009020000000ffa20c00", "-0.00120"),
        ("2301000050080000902000000001806400", "1000000"),
        ("1d0100005005000080706c61696e", "\"plain\""),
        (
            "3b0100005014000080712220625c207409206e0a20630120660c20720d",
            "\"q\\\" b\\\\ t\\t n\\n c\\u0001 f\\f r\\r\"",
        ),
        (
            "2d010000500d000080756e6920c3a9e4b8adf09f9880",
            "\"uni é中😀\"",
        ),
        (
            "8f050000400800009003000000000000400000003023000050200000000080010074776f000100002001000080150000506b000000010000400a00009028000000808003008813",
            "[1, \"two\", null, true, {\"k\": [3.5]}]",
        ),
        (
            "b3020000200100008001000000010000004100005061627800010000200100008033000050630000000100002001000080230000506400000001000040180000d001000040100000d001000040080000902000000000800100",
            "{\"a\": \"x\", \"b\": {\"c\": {\"d\": [[[1]]]}}}",
        ),
        (
            "33010000200300008009000010647570002000000000800200",
            "{\"dup\": 2}",
        ),
        (
            "9b04000020010000800100000001000000020000000b00001008000010080000100800001061797a61610000002000000000800400200000000080020020000000008001002000000000800300",
            "{\"a\": 4, \"y\": 2, \"z\": 1, \"aa\": 3}",
        ),
        (
            "5b01000020010000801f0000106e0000007000000087840c00800dd21ed2042e163423800dd21e00000000e803",
            "{\"n\": 123456789012345678901234567890.000000001}",
        ),
        (
            "b303000040180000d0180000501800005001000020010000800b00001061000000200000000080010001000020010000800b00001062000000200000000080020001000020010000800b000010630000002000000000800300",
            "[{\"a\": 1}, {\"b\": 2}, {\"c\": 3}]",
        ),
        ("1b010000200000008000000000", "{\"\": \"\"}"),
        ("1b01000040040000d000000040", "[[]]"),
        ("1b01000040040000d000000020", "[{}]"),
        (
            "a007000028000040080000900800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008000010080000100800001008010090080000100800001008000010080000100800001008000010080000102000000000800100200000000080020020000000008003002000000000800400200000000080050020000000008006002000000000800700200000000080080020000000008009002000000000800a002000000000800b002000000000800c002000000000800d002000000000800e002000000000800f0020000000008010002000000000801100200000000080120020000000008013002000000000801400200000000080150020000000008016002000000000801700200000000080180020000000008019002000000000801a002000000000801b002000000000801c002000000000801d002000000000801e002000000000801f00200000000080200020000000008021002000000000802200200000000080230020000000008024002000000000802500200000000080260020000000008027002000000000802800",
            "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]",
        ),
        // 40 pairs: the stride puts an offset child mid-key-run and
        // mid-value-run, so both seats of the walk resolve from one
        (
            "e00800002800002003000080030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000006300008003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000c30000800300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000000300000003000000030000006b30306b30316b30326b30336b30346b30356b30366b30376b30386b30396b31306b31316b31326b31336b31346b31356b31366b31376b31386b31396b32306b32316b32326b32336b32346b32356b32366b32376b32386b32396b33306b33316b33326b33336b33346b33356b33366b33376b33386b3339763030763031763032763033763034763035763036763037763038763039763130763131763132763133763134763135763136763137763138763139763230763231763232763233763234763235763236763237763238763239763330763331763332763333763334763335763336763337763338763339",
            "{\"k00\": \"v00\", \"k01\": \"v01\", \"k02\": \"v02\", \"k03\": \"v03\", \"k04\": \"v04\", \"k05\": \"v05\", \"k06\": \"v06\", \"k07\": \"v07\", \"k08\": \"v08\", \"k09\": \"v09\", \"k10\": \"v10\", \"k11\": \"v11\", \"k12\": \"v12\", \"k13\": \"v13\", \"k14\": \"v14\", \"k15\": \"v15\", \"k16\": \"v16\", \"k17\": \"v17\", \"k18\": \"v18\", \"k19\": \"v19\", \"k20\": \"v20\", \"k21\": \"v21\", \"k22\": \"v22\", \"k23\": \"v23\", \"k24\": \"v24\", \"k25\": \"v25\", \"k26\": \"v26\", \"k27\": \"v27\", \"k28\": \"v28\", \"k29\": \"v29\", \"k30\": \"v30\", \"k31\": \"v31\", \"k32\": \"v32\", \"k33\": \"v33\", \"k34\": \"v34\", \"k35\": \"v35\", \"k36\": \"v36\", \"k37\": \"v37\", \"k38\": \"v38\", \"k39\": \"v39\"}",
        ),
    ];

    /// PG heap datum bytes (short or 4-byte varlena header) → varlena body
    fn varlena_body(hex: &str) -> Vec<u8> {
        let bytes: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let header = if bytes[0] & 1 != 0 { 1 } else { 4 };
        bytes[header..].to_vec()
    }

    #[test]
    fn jsonb_renders_what_pg_jsonb_out_renders() {
        for (hex, want) in JSONB_VECTORS {
            assert_eq!(&decode_jsonb(&varlena_body(hex)).expect(hex), want, "{hex}");
        }
    }

    #[test]
    fn jsonb_scalar_root_carries_no_brackets() {
        // JB_FARRAY | JB_FSCALAR, one child: PG's shape for a bare scalar
        let mut body = 0x5000_0001u32.to_le_bytes().to_vec();
        body.extend_from_slice(&0x0000_0003u32.to_le_bytes());
        body.extend_from_slice(b"abc");
        assert_eq!(decode_jsonb(&body).unwrap(), r#""abc""#);
    }

    #[test]
    fn jsonb_malformed_bytes_error_rather_than_render() {
        // Header alone, no room for the child entry it counts
        let short_header = 0x2000_0001u32.to_le_bytes().to_vec();
        assert_eq!(
            decode_jsonb(&short_header),
            Err(CodecError::BadJsonbContainer("child entries past end"))
        );
        // Child length running past the data area
        let mut long_child = 0x4000_0001u32.to_le_bytes().to_vec();
        long_child.extend_from_slice(&0x0000_0009u32.to_le_bytes());
        long_child.extend_from_slice(b"ab");
        assert_eq!(
            decode_jsonb(&long_child),
            Err(CodecError::BadJsonbContainer("child data past end"))
        );
        assert_eq!(
            decode_jsonb(&[0u8; 3]),
            Err(CodecError::BadJsonbContainer("header past end"))
        );
    }
}
