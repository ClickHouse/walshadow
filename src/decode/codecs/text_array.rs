//! PG `text[]` element list, for runtime config's own key columns.

/// Decode on-disk `text[]` body (outer varlena header stripped) for runtime
/// config's key lists. `None` for anything else: nested dims, NULL elements
/// (`dataoffset != 0`), other element types, malformed bytes
pub fn decode_text_array(body: &[u8]) -> Option<Vec<String>> {
    let word = |at: usize| Some(i32::from_le_bytes(body.get(at..at + 4)?.try_into().ok()?));
    let (ndim, dataoffset, elemtype) = (word(0)?, word(4)?, word(8)? as u32);
    if dataoffset != 0 || elemtype != crate::schema::TEXTOID || !(0..=1).contains(&ndim) {
        return None;
    }
    if ndim == 0 {
        return Some(Vec::new());
    }
    let nitems = usize::try_from(word(12)?).ok()?;
    // ARR_DATA_PTR: MAXALIGN(sizeof(ArrayType) + 2 * 4 * ndim) less stripped header
    let mut cursor = 20usize;
    let mut values = Vec::with_capacity(nitems);
    for _ in 0..nitems {
        cursor = cursor.next_multiple_of(4);
        // Elements keep their own varlena header, nominal-aligned (`array_out`)
        let first = *body.get(cursor)?;
        let (header, total) = if first & 1 != 0 {
            (1, (first >> 1) as usize)
        } else {
            (4, (word(cursor)? as u32 >> 2) as usize)
        };
        let text = body.get(cursor + header..cursor.checked_add(total)?)?;
        values.push(std::str::from_utf8(text).ok()?.to_owned());
        cursor += total;
    }
    Some(values)
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

        // dataoffset != 0 marks a NULL bitmap
        let mut with_null = text_array_body(&["tenant"]);
        with_null[4..8].copy_from_slice(&32i32.to_le_bytes());
        assert_eq!(decode_text_array(&with_null), None);
    }
}
