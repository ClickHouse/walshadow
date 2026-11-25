//! PG `text[]` element list, for runtime config's own key columns.

use super::array::{ArrayBody, array_elem};

/// Decode on-disk `text[]` body (outer varlena header stripped) for runtime
/// config's key lists. `None` for anything else: nested dims, NULL elements,
/// other element types, malformed bytes
pub fn decode_text_array(body: &[u8]) -> Option<Vec<String>> {
    let arr = ArrayBody::parse(body, array_elem(crate::schema::TEXTARRAYOID)?).ok()?;
    if arr.ndim() > 1 || arr.has_nulls() {
        return None;
    }
    arr.elements()
        .map(|e| Some(std::str::from_utf8(e.ok()??).ok()?.to_owned()))
        .collect()
}

#[cfg(test)]
pub(crate) fn text_array_body(values: &[&str]) -> Vec<u8> {
    let mut body = vec![0; 20];
    body[0..4].copy_from_slice(&1i32.to_le_bytes());
    body[8..12].copy_from_slice(&crate::schema::TEXTOID.to_le_bytes());
    body[12..16].copy_from_slice(&(values.len() as i32).to_le_bytes());
    body[16..20].copy_from_slice(&1i32.to_le_bytes());
    for value in values {
        body.extend_from_slice(&(((value.len() + 4) as u32) << 2).to_le_bytes());
        body.extend_from_slice(value.as_bytes());
        body.resize(body.len().next_multiple_of(4), 0);
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_array_decodes_identifiers_without_delimiter_parsing() {
        let body = text_array_body(&["tenant,id", "tick`"]);
        assert_eq!(
            decode_text_array(&body).unwrap(),
            ["tenant,id".to_owned(), "tick`".to_owned()]
        );

        let mut empty = vec![0; 12];
        empty[8..12].copy_from_slice(&crate::schema::TEXTOID.to_le_bytes());
        assert!(decode_text_array(&empty).unwrap().is_empty());

        let stored = super::super::stored_array;
        for refused in [
            stored(1009, &[(2, 1)], &[Some(b"tenant"), None]),
            stored(1009, &[(1, 1), (1, 1)], &[Some(b"a")]),
            stored(1009, &[(1, 1)], &[Some(b"\xff")]),
            stored(1015, &[(1, 1)], &[Some(b"a")]),
            text_array_body(&["tenant"])[..22].to_vec(),
        ] {
            assert_eq!(decode_text_array(&refused), None, "{refused:?}");
        }

        // 1-byte varlena header: length byte counts itself, `(len << 1) | 1`
        let mut short = text_array_body(&[]);
        short[12..16].copy_from_slice(&1i32.to_le_bytes());
        short.extend_from_slice(&[7, b'a', b'b']);
        assert_eq!(decode_text_array(&short).unwrap(), ["ab".to_owned()]);
    }
}
