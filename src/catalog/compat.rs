//! Physical compatibility predicate: can the final committed descriptor
//! decode tuples written earlier in the same dirty interval?
//!
//! Bias rejects: capture publishes an ambiguity interval on any transition
//! not on the proven-safe list, never guesses. Compared fields are what
//! tuple walking + value interpretation read: attnum sequence, dropped
//! slots, attlen/attalign/attbyval, type oid + typmod + storage,
//! missing-value semantics. Rename, replica identity, and not-null are
//! metadata for decode purposes
//!
//! Dropped slots keep physical walk fields: PG `RemoveAttributeById`
//! (`src/backend/catalog/heap.c`) preserves attlen/attalign/attbyval and
//! zeroes atttypid, clears attmissingval

use crate::schema::{RelAttr, RelDescriptor};

/// `Ok(())` when `new` provably decodes tuples formatted under `old`;
/// `Err` names the first failing check
pub fn compatible_reader(old: &RelDescriptor, new: &RelDescriptor) -> Result<(), &'static str> {
    if old.oid != new.oid {
        return Err("oid mismatch");
    }
    if old.rfn != new.rfn {
        return Err("filenode rotated");
    }
    if old.kind != new.kind {
        return Err("relkind change");
    }
    if old.persistence != new.persistence {
        return Err("persistence change");
    }
    // Old rows' external pointers resolve against the toast relation they
    // were written under; 0 -> oid is toast creation, old rows predate it
    if old.toast_oid != 0 && old.toast_oid != new.toast_oid {
        return Err("toast relation change");
    }
    if new.attributes.len() < old.attributes.len() {
        return Err("attribute truncation");
    }
    for (o, n) in old.attributes.iter().zip(&new.attributes) {
        slot_compatible(o, n)?;
    }
    // Appended columns: old tuples read the stored missing value, or NULL.
    // NOT NULL without a missing value implies the rewrite path, which this
    // predicate must not bless for in-place history
    for n in &new.attributes[old.attributes.len()..] {
        if !n.dropped && n.not_null && n.missing_text.is_none() {
            return Err("appended not-null column without missing value");
        }
    }
    Ok(())
}

fn slot_compatible(o: &RelAttr, n: &RelAttr) -> Result<(), &'static str> {
    if o.attnum != n.attnum {
        return Err("attnum sequence change");
    }
    // Walk fields are read regardless of dropped state
    if o.type_len != n.type_len || o.type_align != n.type_align || o.type_byval != n.type_byval {
        return Err("physical walk fields change");
    }
    if o.dropped && !n.dropped {
        // PG re-adds at a fresh attnum, never resurrects a dropped slot
        return Err("dropped slot resurrected");
    }
    if n.dropped {
        // Present -> dropped inside the interval: value discarded either
        // way; atttypid/attmissingval zeroed on drop, walk fields checked
        return Ok(());
    }
    if o.type_oid != n.type_oid || o.typmod != n.typmod {
        return Err("type or typmod change");
    }
    if o.type_storage != n.type_storage {
        return Err("storage change");
    }
    // Tuples shorter than attnum read the missing value; a different one
    // reinterprets history
    if o.missing_text != n.missing_text {
        return Err("missing value change");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{RelName, ReplIdent};
    use walrus::pg::walparser::RelFileNode;

    fn attr(attnum: i16, type_oid: u32, type_len: i16) -> RelAttr {
        RelAttr {
            attnum,
            name: format!("c{attnum}"),
            type_oid,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: "t".into(),
            type_byval: type_len > 0,
            type_len,
            type_align: 'i',
            type_storage: if type_len > 0 { 'p' } else { 'x' },
            missing_text: None,
        }
    }

    fn dropped_slot(attnum: i16, type_len: i16) -> RelAttr {
        RelAttr {
            type_oid: 0,
            dropped: true,
            name: format!("........pg.dropped.{attnum}........"),
            type_name: String::new(),
            missing_text: None,
            not_null: false,
            ..attr(attnum, 0, type_len)
        }
    }

    fn rel(attributes: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 7000,
            },
            oid: 42,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes,
        }
    }

    #[test]
    fn metadata_changes_allowed() {
        let old = rel(vec![attr(1, 23, 4)]);
        let mut new = rel(vec![attr(1, 23, 4)]);
        new.rel_name = RelName::new("renamed_ns", "renamed");
        new.namespace_oid = 9999;
        new.replident = ReplIdent::Full { pk_attnums: None };
        new.attributes[0].name = "renamed_col".into();
        new.attributes[0].not_null = true;
        assert_eq!(compatible_reader(&old, &new), Ok(()));
    }

    #[test]
    fn append_only_columns() {
        let old = rel(vec![attr(1, 23, 4)]);
        // Nullable append, no missing value: old rows read NULL
        let new = rel(vec![attr(1, 23, 4), attr(2, 20, 8)]);
        assert_eq!(compatible_reader(&old, &new), Ok(()));
        // NOT NULL append with stored missing value
        let mut with_missing = attr(2, 20, 8);
        with_missing.not_null = true;
        with_missing.missing_text = Some("7".into());
        let new = rel(vec![attr(1, 23, 4), with_missing]);
        assert_eq!(compatible_reader(&old, &new), Ok(()));
        // NOT NULL append without missing value = rewrite territory
        let mut bad = attr(2, 20, 8);
        bad.not_null = true;
        let new = rel(vec![attr(1, 23, 4), bad]);
        assert!(compatible_reader(&old, &new).is_err());
        // Added-then-dropped inside the interval appends a dropped slot
        let new = rel(vec![attr(1, 23, 4), dropped_slot(2, 8)]);
        assert_eq!(compatible_reader(&old, &new), Ok(()));
    }

    #[test]
    fn physical_changes_rejected() {
        let old = rel(vec![attr(1, 23, 4)]);
        let type_change = rel(vec![attr(1, 20, 8)]);
        assert!(compatible_reader(&old, &type_change).is_err());
        let mut typmod = rel(vec![attr(1, 23, 4)]);
        typmod.attributes[0].typmod = 12;
        assert!(compatible_reader(&old, &typmod).is_err());
        let mut storage = rel(vec![attr(1, 23, 4)]);
        storage.attributes[0].type_storage = 'e';
        assert!(compatible_reader(&old, &storage).is_err());
        let mut missing = rel(vec![attr(1, 23, 4)]);
        missing.attributes[0].missing_text = Some("1".into());
        assert!(compatible_reader(&old, &missing).is_err());
        let truncated = rel(vec![]);
        assert!(compatible_reader(&old, &truncated).is_err());
        let reorder = rel(vec![attr(2, 23, 4)]);
        assert!(compatible_reader(&old, &reorder).is_err());
    }

    #[test]
    fn dropped_slot_transitions() {
        let old = rel(vec![attr(1, 23, 4), attr(2, 20, 8)]);
        // Drop preserves walk fields: compatible
        let new = rel(vec![attr(1, 23, 4), dropped_slot(2, 8)]);
        assert_eq!(compatible_reader(&old, &new), Ok(()));
        // Dropped slot with altered walk fields cannot parse old tuples
        let new = rel(vec![attr(1, 23, 4), dropped_slot(2, 4)]);
        assert!(compatible_reader(&old, &new).is_err());
        // Resurrection: PG never reuses a dropped attnum
        let was_dropped = rel(vec![attr(1, 23, 4), dropped_slot(2, 8)]);
        let resurrected = rel(vec![attr(1, 23, 4), attr(2, 20, 8)]);
        assert!(compatible_reader(&was_dropped, &resurrected).is_err());
        // Dropped in both stays compatible
        assert_eq!(
            compatible_reader(&was_dropped, &was_dropped.clone()),
            Ok(())
        );
    }

    #[test]
    fn relation_level_changes() {
        let old = rel(vec![attr(1, 23, 4)]);
        let mut kind = rel(vec![attr(1, 23, 4)]);
        kind.kind = 'm';
        assert!(compatible_reader(&old, &kind).is_err());
        let mut persistence = rel(vec![attr(1, 23, 4)]);
        persistence.persistence = 'u';
        assert!(compatible_reader(&old, &persistence).is_err());
        // Toast creation: old rows predate any external pointer
        let mut toast_added = rel(vec![attr(1, 23, 4)]);
        toast_added.toast_oid = 8800;
        assert_eq!(compatible_reader(&old, &toast_added), Ok(()));
        // Toast replacement invalidates old external pointers
        let mut old_toast = rel(vec![attr(1, 23, 4)]);
        old_toast.toast_oid = 8800;
        let mut new_toast = rel(vec![attr(1, 23, 4)]);
        new_toast.toast_oid = 8801;
        assert!(compatible_reader(&old_toast, &new_toast).is_err());
        let mut rotated = rel(vec![attr(1, 23, 4)]);
        rotated.rfn.rel_node = 7001;
        assert!(compatible_reader(&old, &rotated).is_err());
        let mut other_oid = rel(vec![attr(1, 23, 4)]);
        other_oid.oid = 43;
        assert!(compatible_reader(&old, &other_oid).is_err());
    }
}
