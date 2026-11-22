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
    build_mimalloc();

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

fn build_mimalloc() {
    println!("cargo:rerun-if-changed=vendor/mimalloc");
    let musl = std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("musl");
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let mut build = cc::Build::new();
    build
        .file("vendor/mimalloc/src/static.c")
        .include("vendor/mimalloc/include")
        .define("MI_DEBUG", "0")
        .define("NDEBUG", "1")
        .flag_if_supported("-fvisibility=hidden");
    if musl {
        build
            .define("MI_LIBC_MUSL", "1")
            .flag_if_supported("-ftls-model=local-dynamic");
    } else if os != "haiku" {
        build.flag_if_supported("-ftls-model=initial-exec");
    }
    build.compile("mimalloc");
    if os == "linux" && (arch == "arm" || arch.starts_with("riscv32")) {
        println!("cargo:rustc-link-lib=atomic");
    }
}
