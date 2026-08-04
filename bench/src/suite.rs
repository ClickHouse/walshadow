//! The standard four-benchmark pass, run into `<results_dir>/<name>/`.
//!
//! Each shape runs as a child of this same executable, so a bench that fails or
//! hangs cannot take the rest of the pass with it, and its output is teed to
//! both the terminal (long runs are watched live) and its result file.
//!
//! Shapes:
//!   * `single`           — single-row commit→visible latency distribution
//!   * `sustained`        — fixed insert rate, latency under load
//!   * `interleaved`      — concurrent long transactions, one round each
//!   * `interleaved-long` — one long transaction thread, ten rounds

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, Result, bail};

/// Quick-pass load duration for `sustained`, in seconds.
const SUSTAINED_SECS: u64 = 20;
/// Quick-pass load duration for `interleaved`, in seconds.
const INTERLEAVED_SECS: u64 = 30;

/// A single-word flag list, as it would be typed on the command line.
fn flags(s: &str) -> Vec<String> {
    s.split_whitespace().map(str::to_string).collect()
}

/// The four shapes and their flags. `run_secs` overrides the load duration of
/// `sustained` + `interleaved`; `interleaved-long` already spans 10 × 30s.
fn shapes(run_secs: Option<u64>) -> Vec<(&'static str, Vec<String>)> {
    let sustained = run_secs.unwrap_or(SUSTAINED_SECS);
    let interleaved = run_secs.unwrap_or(INTERLEAVED_SECS);
    vec![
        (
            "single",
            flags("--bench single-row --iterations 100 --warmup 10"),
        ),
        (
            "sustained",
            flags(&format!(
                "--bench sustained --rate 30000 --duration-secs {sustained} --concurrency 90"
            )),
        ),
        (
            "interleaved",
            flags(&format!(
                "--bench interleaved --xact-threads 90 --rounds 1 --xact-secs {interleaved}"
            )),
        ),
        (
            "interleaved-long",
            flags("--bench interleaved --xact-threads 1 --rounds 10 --xact-secs 30"),
        ),
    ]
}

/// Run every shape in sequence. `common` is the flag list each child inherits
/// (destination, network, state dir). Returns the names that failed.
pub fn run(
    name: &str,
    results_dir: &Path,
    common: &[String],
    run_secs: Option<u64>,
) -> Result<Vec<String>> {
    let out = results_dir.join(name);
    if out.exists() {
        bail!("{} already exists — choose a different name", out.display());
    }
    fs::create_dir_all(&out).with_context(|| format!("create {}", out.display()))?;
    let exe = std::env::current_exe().context("locate own executable")?;
    println!("results → {}", out.display());

    let mut failed = Vec::new();
    for (shape, shape_flags) in shapes(run_secs) {
        println!("\n===== {shape} =====");
        let file = out.join(format!("{shape}.txt"));
        match run_shape(&exe, &file, &shape_flags, common) {
            Ok(()) => {}
            Err(e) => {
                println!("  ⚠ {shape} FAILED ({e}) — see {}", file.display());
                failed.push(shape.to_string());
            }
        }
    }

    println!("\nall done → {}", out.display());
    Ok(failed)
}

/// Run one shape as a child process, teeing its merged output to `file`.
fn run_shape(exe: &Path, file: &Path, shape_flags: &[String], common: &[String]) -> Result<()> {
    let mut sink = File::create(file).with_context(|| format!("create {}", file.display()))?;
    let argv: Vec<&String> = shape_flags.iter().chain(common).collect();
    let cmdline = argv
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    writeln!(
        sink,
        "# {} {cmdline}\n",
        exe.file_name().unwrap_or_default().to_string_lossy(),
    )?;

    // One pipe carrying stdout and stderr keeps their interleaving in the file,
    // the way `2>&1 | tee` did.
    let (reader, writer) = std::io::pipe().context("create output pipe")?;
    let started = Instant::now();
    let mut cmd = Command::new(exe);
    cmd.args(&argv)
        .stdout(writer.try_clone().context("clone pipe")?)
        .stderr(writer);
    let mut child = cmd.spawn().with_context(|| format!("spawn {exe:?}"))?;
    // Command holds this process's write ends until dropped; without this the
    // read below never sees EOF.
    drop(cmd);

    for line in BufReader::new(reader).lines() {
        let line = line.context("read bench output")?;
        println!("{line}");
        writeln!(sink, "{line}")?;
    }

    let status = child.wait().context("wait for bench")?;
    let secs = started.elapsed().as_secs_f64();
    if !status.success() {
        writeln!(sink, "# FAILED ({status}) after {secs:.1}s")?;
        bail!("{status}");
    }
    writeln!(sink, "# ok: {secs:.1}s")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shapes_cover_the_four_named_runs() {
        let names: Vec<&str> = shapes(None).into_iter().map(|(n, _)| n).collect();
        assert_eq!(
            names,
            ["single", "sustained", "interleaved", "interleaved-long"]
        );
    }

    #[test]
    fn run_secs_overrides_sustained_and_interleaved_only() {
        let quick = shapes(None);
        let long = shapes(Some(300));

        let flags_of = |s: &[(&str, Vec<String>)], want: &str| {
            s.iter()
                .find(|(n, _)| *n == want)
                .map(|(_, f)| f.join(" "))
                .unwrap()
        };

        assert!(flags_of(&quick, "sustained").contains("--duration-secs 20"));
        assert!(flags_of(&long, "sustained").contains("--duration-secs 300"));
        assert!(flags_of(&quick, "interleaved").contains("--xact-secs 30"));
        assert!(flags_of(&long, "interleaved").contains("--xact-secs 300"));
        // interleaved-long owns its own duration (10 rounds × 30s).
        assert_eq!(
            flags_of(&quick, "interleaved-long"),
            flags_of(&long, "interleaved-long")
        );
    }

    #[test]
    fn run_refuses_an_existing_results_folder() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("taken")).unwrap();

        let err = run("taken", dir.path(), &[], None).unwrap_err().to_string();
        assert!(err.contains("already exists"), "unexpected error: {err}");
    }

    /// The tee path, exercised against a stand-in "bench" that prints on both
    /// streams and exits non-zero.
    #[test]
    fn run_shape_tees_both_streams_and_reports_failure() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("out.txt");
        let script = vec![
            "-c".to_string(),
            "echo from-stdout; echo from-stderr >&2; exit 3".to_string(),
        ];

        let err = run_shape(Path::new("/bin/sh"), &file, &script, &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains('3'), "unexpected error: {err}");

        let logged = fs::read_to_string(&file).unwrap();
        assert!(logged.contains("from-stdout"), "missing stdout: {logged}");
        assert!(logged.contains("from-stderr"), "missing stderr: {logged}");
        assert!(logged.contains("# FAILED"), "missing footer: {logged}");
    }

    #[test]
    fn run_shape_marks_a_clean_run_ok() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("out.txt");
        let script = vec!["-c".to_string(), "true".to_string()];

        run_shape(Path::new("/bin/sh"), &file, &script, &[]).unwrap();

        let logged = fs::read_to_string(&file).unwrap();
        assert!(logged.contains("# ok"), "missing footer: {logged}");
    }
}
