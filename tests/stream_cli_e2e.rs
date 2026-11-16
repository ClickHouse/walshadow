//! `walshadow-stream` startup refusals that need no source or shadow

use std::process::{Command, Output};

fn stream(args: &[&str]) -> Output {
    Command::new(std::env::var("CARGO_BIN_EXE_walshadow-stream").unwrap())
        .args(args)
        .env_remove("WALSHADOW_CONTROL_SOCKET")
        .output()
        .expect("spawn walshadow-stream")
}

/// Daemon args that pass validation, beside `extra`
fn daemon(tmp: &tempfile::TempDir, extra: &[&str]) -> Output {
    let dir = |name: &str| tmp.path().join(name).to_str().unwrap().to_owned();
    let (out, spill, sock, data) = (dir("out"), dir("spill"), dir("sock"), dir("data"));
    let mut args = vec![
        "--out-dir",
        &out,
        "--spill-dir",
        &spill,
        "--shadow-socket-dir",
        &sock,
        "--bootstrap-shadow-data-dir",
        &data,
        "--walsender-bind",
        "127.0.0.1:1",
    ];
    args.extend(extra);
    stream(&args)
}

fn stderr(out: &Output) -> String {
    assert!(!out.status.success(), "{out:?}");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn ctl_names_the_socket_it_cannot_reach() {
    let tmp = tempfile::tempdir().unwrap();
    let socket = tmp.path().join("missing.sock");
    let err = stderr(&stream(&[
        "ctl",
        "--socket",
        socket.to_str().unwrap(),
        "status",
    ]));
    assert!(err.contains("connect control socket"), "{err}");
    assert!(err.contains("missing.sock"), "{err}");
}

#[test]
fn control_socket_without_config_refuses_start() {
    let tmp = tempfile::tempdir().unwrap();
    let socket = tmp.path().join("control.sock");
    let err = stderr(&daemon(
        &tmp,
        &["--control-socket", socket.to_str().unwrap()],
    ));
    assert!(
        err.contains("--control-socket requires --ch-config"),
        "{err}"
    );
}

#[test]
fn unreadable_config_refuses_start() {
    let tmp = tempfile::tempdir().unwrap();
    let config = tmp.path().join("broken.toml");
    std::fs::write(&config, "[[[").unwrap();
    let err = stderr(&daemon(&tmp, &["--ch-config", config.to_str().unwrap()]));
    assert!(
        err.contains("load config") && err.contains("broken.toml"),
        "{err}"
    );
}
