//! Embed the git commit in `--version` output via `WALSHADOW_GIT_SHA`.
//!
//! Set `WALSHADOW_GIT_SHA` in the environment where no `.git` is available,
//! eg the docker build, which copies only the sources. Falls back to
//! `unknown` so a source tarball still builds.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn main() {
    println!("cargo:rerun-if-env-changed=WALSHADOW_GIT_SHA");
    // `--git-path` resolves HEAD inside worktrees too, where `.git` is a file
    if let Some(head) = git(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head}");
    }
    let sha = std::env::var("WALSHADOW_GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| git(&["rev-parse", "--short", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=WALSHADOW_GIT_SHA={sha}");
}
