//! Accepted-but-ignored request fields log WARN once per key so silent
//! divergence from PeerDB semantics stays greppable without flooding

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

pub fn warn_ignored(key: &str, detail: &str) {
    let seen = SEEN.get_or_init(|| Mutex::new(HashSet::new()));
    if seen.lock().is_ok_and(|mut s| s.insert(key.to_string())) {
        tracing::warn!(field = key, detail, "ignoring unsupported PeerDB field");
    }
}
