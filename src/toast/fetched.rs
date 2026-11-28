/// Store-side value fetch outcome. Ordering violations are transport
/// errors, not outcomes: ascending dense feed is part of the fetch
/// contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchedValue {
    /// No live chunk visible at bound
    Missing,
    /// Chunk run deviates from pointer size (partial merge collapse or
    /// generation mixing): gapped, short, or over-long. Fills per miss
    /// policy, counted distinctly
    Mismatch { got: usize },
    /// Exactly `expected_size` bytes, seq-dense from 0
    Assembled(Vec<u8>),
    /// Chunks postdate referring record, indicating value-ID reuse
    /// Applies to complete and partial replacements, shadow store only
    Generation,
}
