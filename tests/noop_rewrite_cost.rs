//! Cost of the per-record NOOP rewrite the filter applies to every
//! `Route::ToDecoder` record, measured against the rate the pump sees.
//!
//! Run with `cargo test --release --test noop_rewrite_cost -- --ignored --nocapture`.

/// Records/s reaching the rewrite in the cloud repro (266k records/s, ~70% ToDecoder)
const REWRITES_PER_SEC: f64 = 186_000.0;

#[test]
#[ignore = "diagnostic microbenchmark"]
fn noop_replace_cost_by_record_size() {
    println!();
    for len in [64usize, 128, 197, 400, 697, 1500, 8192] {
        let mut rec = vec![0u8; len];
        rec[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        for (i, b) in rec.iter_mut().enumerate().skip(24) {
            *b = (i % 251) as u8;
        }
        let iters = 500_000usize;
        let mut sink = 0u64;

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let mut r = rec.clone();
            walshadow::rewrite::noop_replace(&mut r).ok();
            sink = sink.wrapping_add(r[20] as u64);
        }
        let with_rewrite = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iters {
            let r = rec.clone();
            sink = sink.wrapping_add(r[20] as u64);
        }
        let clone_only = start.elapsed();

        let net_ns = with_rewrite.saturating_sub(clone_only).as_nanos() as f64 / iters as f64;
        println!(
            "  len={len:<5} {net_ns:>7.1} ns/record   {:>5.2}% of one core at {REWRITES_PER_SEC:.0} rewrites/s   [{}]",
            net_ns * REWRITES_PER_SEC / 1e9 * 100.0,
            sink & 1,
        );
    }
}
