use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::backfill::backup_page_walk::CatalogMap;
use crate::catalog::shadow::{BridgeConf, Shadow, ShadowConfig};
use crate::ops::oracle::Oracle;
use crate::schema::{
    BOOLOID, BPCHAROID, BYTEAOID, CHAROID, CIDROID, DATEOID, FLOAT4OID, FLOAT8OID, INETOID,
    INT2OID, INT4OID, INT8OID, INTERVALOID, JSONOID, NAMEOID, NUMERICOID, OIDOID, RelAttr, TEXTOID,
    TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID, VARCHAROID,
};

const ORACLE_PORT: u16 = 55440;

pub struct BootstrapOracle {
    shadow: Shadow,
    oracle: Arc<Oracle>,
    base_dir: PathBuf,
}

impl BootstrapOracle {
    pub async fn provision(
        base_dir: PathBuf,
        source_conninfo: String,
        source_password: Option<String>,
        bridge_lib_dir: Option<PathBuf>,
        connect_budget: Duration,
    ) -> Result<Self> {
        let data_dir = base_dir.join("pg");
        let socket_dir = base_dir.join("sock");
        let bridge_socket = socket_dir.join("walshadow-bridge.sock");

        let (b_data, b_sock, b_bridge, b_base) = (
            data_dir.clone(),
            socket_dir.clone(),
            bridge_socket.clone(),
            base_dir.clone(),
        );
        let shadow = tokio::task::spawn_blocking(move || -> Result<Shadow> {
            std::fs::remove_dir_all(&b_base).ok();
            std::fs::create_dir_all(&b_sock)?;

            let cfg_a = oracle_cfg(&b_data, &b_base, &b_sock, None);
            let a = Shadow::new(cfg_a);
            a.initdb().context("initdb")?;
            a.write_base_conf().context("base conf")?;
            a.start_binary_upgrade().context("start -b")?;
            let dump = run_pg_dump(&source_conninfo, source_password.as_deref())
                .context("pg_dump --binary-upgrade")?;
            a.apply_schema_dump(&dump).context("apply schema")?;
            a.stop().context("stop -b")?;

            let mut bridge = BridgeConf::in_dir(&b_sock);
            bridge.socket_path = b_bridge;
            bridge.library_dir = bridge_lib_dir;
            let cfg_b = oracle_cfg(&b_data, &b_base, &b_sock, Some(bridge));
            let b = Shadow::new(cfg_b);
            b.write_base_conf().context("serve conf")?;
            b.start().context("start serve")?;
            Ok(b)
        })
        .await
        .context("bootstrap oracle provision task")?
        .context("bootstrap oracle provision")?;

        let bridge = crate::ops::bridge::connect_with_budget(&bridge_socket, connect_budget)
            .await
            .context("bootstrap oracle bridge connect")?;
        Ok(Self {
            shadow,
            oracle: Arc::new(Oracle::new(Arc::new(bridge))),
            base_dir,
        })
    }

    pub fn oracle(&self) -> Arc<Oracle> {
        self.oracle.clone()
    }
}

impl Drop for BootstrapOracle {
    fn drop(&mut self) {
        let _ = self.shadow.stop();
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }
}

fn oracle_cfg(
    data_dir: &std::path::Path,
    filter_out_dir: &std::path::Path,
    socket_dir: &std::path::Path,
    bridge: Option<BridgeConf>,
) -> ShadowConfig {
    let mut cfg = ShadowConfig::new(data_dir.to_path_buf(), filter_out_dir.to_path_buf());
    cfg.socket_dir = socket_dir.to_path_buf();
    cfg.port = ORACLE_PORT;
    cfg.user = "postgres".into();
    cfg.dbname = "postgres".into();
    cfg.bridge = bridge;
    cfg
}

fn run_pg_dump(conninfo: &str, password: Option<&str>) -> Result<String> {
    let mut cmd = Command::new("pg_dump");
    cmd.args([
        "--binary-upgrade",
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "-d",
        conninfo,
    ]);
    if let Some(pw) = password {
        cmd.env("PGPASSWORD", pw);
    }
    let out = cmd.output().context("spawn pg_dump")?;
    if !out.status.success() {
        anyhow::bail!("pg_dump failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    String::from_utf8(out.stdout).context("pg_dump output not utf8")
}

pub fn needs_bridge(catalog: &CatalogMap) -> bool {
    catalog.descriptors().any(|d| {
        d.attributes
            .iter()
            .any(|a| !a.dropped && attr_needs_bridge(a))
    })
}

fn attr_needs_bridge(a: &RelAttr) -> bool {
    !is_native_scalar(a.type_oid)
        && !matches!(
            a.type_name.as_str(),
            "geography" | "geometry" | "vector" | "halfvec"
        )
}

fn is_native_scalar(oid: u32) -> bool {
    matches!(
        oid,
        BOOLOID
            | BYTEAOID
            | CHAROID
            | NAMEOID
            | INT8OID
            | INT2OID
            | INT4OID
            | TEXTOID
            | OIDOID
            | JSONOID
            | CIDROID
            | FLOAT4OID
            | FLOAT8OID
            | INETOID
            | BPCHAROID
            | VARCHAROID
            | DATEOID
            | TIMEOID
            | TIMESTAMPOID
            | TIMESTAMPTZOID
            | INTERVALOID
            | TIMETZOID
            | NUMERICOID
            | UUIDOID
    )
}
