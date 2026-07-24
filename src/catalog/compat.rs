//! Physical compatibility predicate: can the final committed descriptor
//! decode tuples written earlier in the same dirty interval?
//!
//! Compared fields are what tuple walking + value interpretation read:
//! attnum sequence, dropped slots, attlen/attalign/attbyval, type oid,
//! missing-value semantics. Rename, replica identity, and not-null are
//! metadata for decode purposes
//!
//! Rejects split by physical consequence ([`Incompat`]). A `Physical`
//! reject means no descriptor reads both formats, so capture fences the
//! interval; a `Benign` one means the declared shape drifted while every
//! byte reads the same, so bias-early history stays sound. The split is
//! what keeps the fence off the shapes PG actually produces in place —
//! layout-moving ALTERs rewrite the relation into a fresh filenode, and a
//! rotation never reaches this predicate
//!
//! Dropped slots keep physical walk fields: PG `RemoveAttributeById`
//! (`src/backend/catalog/heap.c`) preserves attlen/attalign/attbyval and
//! zeroes atttypid, clears attmissingval

use crate::schema::{RelAttr, RelDescriptor};

/// Why `new` isn't a proven reader of tuples formatted under `old`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Incompat {
    /// Physical read is identical; only the declared shape drifts
    Benign(&'static str),
    /// Walk fields or datum identity differ; no descriptor reads both
    Physical(&'static str),
}

impl Incompat {
    /// Failing check's name, for logs and ambiguity diagnostics
    pub fn why(&self) -> &'static str {
        match self {
            Self::Benign(why) | Self::Physical(why) => why,
        }
    }

    pub fn is_physical(&self) -> bool {
        matches!(self, Self::Physical(_))
    }
}

/// `Ok(())` when `new` provably decodes tuples formatted under `old`;
/// `Err` names the first failing check, with `Physical` winning over
/// `Benign` wherever both apply
pub fn compatible_reader(old: &RelDescriptor, new: &RelDescriptor) -> Result<(), Incompat> {
    if old.oid != new.oid {
        return Err(Incompat::Physical("oid mismatch"));
    }
    if old.rfn != new.rfn {
        return Err(Incompat::Physical("filenode rotated"));
    }
    if old.kind != new.kind {
        return Err(Incompat::Physical("relkind change"));
    }
    if old.persistence != new.persistence {
        return Err(Incompat::Physical("persistence change"));
    }
    // Old rows' external pointers resolve against the toast relation they
    // were written under; 0 -> oid is toast creation, old rows predate it
    if old.toast_oid != 0 && old.toast_oid != new.toast_oid {
        return Err(Incompat::Physical("toast relation change"));
    }
    if new.attributes.len() < old.attributes.len() {
        return Err(Incompat::Physical("attribute truncation"));
    }
    let mut benign: Option<&'static str> = None;
    for (o, n) in old.attributes.iter().zip(&new.attributes) {
        if let Err(e) = slot_compatible(o, n) {
            let Incompat::Benign(why) = e else {
                return Err(e);
            };
            benign = benign.or(Some(why));
        }
    }
    // Appended columns: old tuples read the stored missing value, or NULL.
    // NOT NULL without a missing value implies the rewrite path, which this
    // predicate must not bless for in-place history
    for n in &new.attributes[old.attributes.len()..] {
        if !n.dropped && n.not_null && n.missing_text.is_none() {
            return Err(Incompat::Physical(
                "appended not-null column without missing value",
            ));
        }
    }
    benign.map_or(Ok(()), |why| Err(Incompat::Benign(why)))
}

fn slot_compatible(o: &RelAttr, n: &RelAttr) -> Result<(), Incompat> {
    if o.attnum != n.attnum {
        return Err(Incompat::Physical("attnum sequence change"));
    }
    // Walk fields are read regardless of dropped state
    if o.type_len != n.type_len || o.type_align != n.type_align || o.type_byval != n.type_byval {
        return Err(Incompat::Physical("physical walk fields change"));
    }
    if o.dropped && !n.dropped {
        // PG re-adds at a fresh attnum, never resurrects a dropped slot
        return Err(Incompat::Physical("dropped slot resurrected"));
    }
    if n.dropped {
        // Present -> dropped inside the interval: value discarded either
        // way; atttypid/attmissingval zeroed on drop, walk fields checked
        return Ok(());
    }
    // Same width can still reinterpret: int4 vs date both walk 4 bytes
    if o.type_oid != n.type_oid {
        return Err(Incompat::Physical("type change"));
    }
    // Tuples shorter than attnum read the missing value; a different one
    // reinterprets history
    if o.missing_text != n.missing_text {
        return Err(Incompat::Physical("missing value change"));
    }
    // Below: no reader consults these. The walk reads attlen/attalign/
    // attbyval only (PG `src/backend/access/common/heaptuple.c`) and
    // numeric/varlena datums carry their own scale and length, so a widened
    // typmod reinterprets nothing
    if o.typmod != n.typmod {
        return Err(Incompat::Benign("typmod change"));
    }
    // attstorage drives writer-side toasting choices (PG
    // `src/backend/access/table/toast_helper.c`); the reader detects
    // external/compressed per datum from the varlena header (PG
    // `src/include/varatt.h`)
    if o.type_storage != n.type_storage {
        return Err(Incompat::Benign("storage change"));
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
        assert!(
            compatible_reader(&old, &type_change)
                .unwrap_err()
                .is_physical()
        );
        // Same width, different type: walk matches, values reinterpret
        let mut same_width = rel(vec![attr(1, 23, 4)]);
        same_width.attributes[0].type_oid = 1082; // date
        assert_eq!(
            compatible_reader(&old, &same_width),
            Err(Incompat::Physical("type change"))
        );
        let mut missing = rel(vec![attr(1, 23, 4)]);
        missing.attributes[0].missing_text = Some("1".into());
        assert_eq!(
            compatible_reader(&old, &missing),
            Err(Incompat::Physical("missing value change"))
        );
        let truncated = rel(vec![]);
        assert!(
            compatible_reader(&old, &truncated)
                .unwrap_err()
                .is_physical()
        );
        let reorder = rel(vec![attr(2, 23, 4)]);
        assert!(compatible_reader(&old, &reorder).unwrap_err().is_physical());
    }

    #[test]
    fn declared_shape_drift_is_benign() {
        let old = rel(vec![attr(1, 1043, -1)]);
        // varchar(10) -> varchar(20): varlena carries its own length
        let mut typmod = rel(vec![attr(1, 1043, -1)]);
        typmod.attributes[0].typmod = 24;
        assert_eq!(
            compatible_reader(&old, &typmod),
            Err(Incompat::Benign("typmod change"))
        );
        let mut storage = rel(vec![attr(1, 1043, -1)]);
        storage.attributes[0].type_storage = 'e';
        assert_eq!(
            compatible_reader(&old, &storage),
            Err(Incompat::Benign("storage change"))
        );
        // Physical wins wherever both apply, whatever the slot order
        let mut both = rel(vec![attr(1, 1043, -1), attr(2, 23, 4)]);
        both.attributes[0].typmod = 24;
        both.attributes[1].missing_text = Some("1".into());
        let old_two = rel(vec![attr(1, 1043, -1), attr(2, 23, 4)]);
        assert_eq!(
            compatible_reader(&old_two, &both),
            Err(Incompat::Physical("missing value change"))
        );
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
