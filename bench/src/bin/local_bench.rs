//! Replication-latency benchmarks against the local docker-compose stack
//! (`docker/docker-compose.yml`, over host-exposed ports). The benchmark
//! engine and the full CLI surface live in `../bench.rs`, shared with
//! `ec2_bench`; the only thing this binary does differently is default the
//! endpoints to localhost.
//!
//! Benches (`--bench`): `single-row` | `sustained` | `interleaved`.
//! Examples:
//!   cargo run --release --bin walshadow-local-bench -- --bench single-row
//!   cargo run --release --bin walshadow-local-bench -- --bench sustained --rate 500 --concurrency 4
//!   cargo run --release --bin walshadow-local-bench -- --bench interleaved --xact-secs 150

use anyhow::Result;
use clap::Parser;

use walshadow_bench::CommonArgs;

#[derive(Parser, Debug)]
#[command(
    name = "walshadow-local-bench",
    about = "Measure source-Postgres → ClickHouse replication latency (local compose stack)"
)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    // Local stack: source + destination default to localhost unless overridden
    // (--ch-host doubles as the destination-host override here).
    let pg_host = args
        .common
        .pg_host
        .clone()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let dest_host = args
        .common
        .ch_host
        .clone()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    walshadow_bench::dispatch(&args.common, pg_host, dest_host).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use clap::Parser;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    use walshadow_bench::{Bench, ChHttp, Destination, Summary, wait_visible};

    const OK: &str = "HTTP/1.0 200 OK";

    #[test]
    fn args_parse_single_row_defaults() {
        let args = Args::try_parse_from(["bench", "--bench", "single-row"]).unwrap();

        assert_eq!(args.common.bench, Bench::SingleRow);
        assert_eq!(args.common.pg_host, None);
        assert_eq!(args.common.pg_port, 5432);
        assert_eq!(args.common.pg_user, "postgres");
        assert_eq!(args.common.pg_dbname, "postgres");
        assert_eq!(args.common.table, "demo.users");
        assert_eq!(args.common.ch_host, None);
        assert_eq!(args.common.ch_http_port, 8123);
        assert_eq!(args.common.ch_table, "demo.users");
        assert_eq!(args.common.iterations, 100);
        assert_eq!(args.common.warmup, 10);
        assert_eq!(args.common.id_base, 1_000_000);
    }

    #[test]
    fn args_parse_sustained_overrides() {
        let args = Args::try_parse_from([
            "bench",
            "--bench",
            "sustained",
            "--rate",
            "500",
            "--duration-secs",
            "7",
            "--probe-every",
            "3",
            "--concurrency",
            "4",
            "--pg-password",
            "secret",
        ])
        .unwrap();

        assert_eq!(args.common.bench, Bench::Sustained);
        assert_eq!(args.common.rate, 500);
        assert_eq!(args.common.duration_secs, 7);
        assert_eq!(args.common.probe_every, 3);
        assert_eq!(args.common.concurrency, 4);
        assert_eq!(args.common.pg_password.as_deref(), Some("secret"));
    }

    #[test]
    fn args_require_bench() {
        assert!(Args::try_parse_from(["bench"]).is_err());
    }

    #[test]
    fn summary_empty_samples() {
        let mut samples = [];
        let summary = Summary::from(&mut samples);

        assert_eq!(summary.n, 0);
        assert_eq!(summary.min, 0.0);
        assert_eq!(summary.p50, 0.0);
        assert_eq!(summary.p90, 0.0);
        assert_eq!(summary.p99, 0.0);
        assert_eq!(summary.max, 0.0);
        assert_eq!(summary.mean, 0.0);
    }

    #[test]
    fn summary_sorts_and_uses_nearest_rank_percentiles() {
        let mut samples = vec![100.0, 1.0, 50.0, 10.0, 5.0];
        let summary = Summary::from(&mut samples);

        assert_eq!(samples, vec![1.0, 5.0, 10.0, 50.0, 100.0]);
        assert_eq!(summary.n, 5);
        assert_eq!(summary.min, 1.0);
        assert_eq!(summary.p50, 10.0);
        assert_eq!(summary.p90, 100.0);
        assert_eq!(summary.p99, 100.0);
        assert_eq!(summary.max, 100.0);
        assert_eq!(summary.mean, 33.2);
    }

    #[test]
    fn summary_print_handles_empty_and_populated_samples() {
        let mut empty = [];
        Summary::from(&mut empty).print("empty");

        let mut samples = vec![2.0, 1.0];
        Summary::from(&mut samples).print("samples");
    }

    #[tokio::test]
    async fn ch_query_sends_post_and_trims_success_body() {
        let (ch, mut requests) = ch_with_responses(vec![(OK, " 1\n")]).await;

        let body = ch.query("SELECT 1").await.unwrap();

        assert_eq!(body, "1");
        let request = recv_request(&mut requests).await;
        assert!(request.starts_with("POST / HTTP/1.0\r\n"));
        assert!(request.contains("Host: 127.0.0.1\r\n"));
        assert!(request.contains("Content-Length: 8\r\n"));
        assert!(request.ends_with("SELECT 1"));
    }

    #[tokio::test]
    async fn ch_query_reports_http_errors() {
        let (ch, _requests) =
            ch_with_responses(vec![("HTTP/1.0 500 Internal Error", "boom\n")]).await;

        let err = ch.query("SELECT 1").await.unwrap_err().to_string();

        assert!(err.contains("ClickHouse HTTP error: boom"));
    }

    #[tokio::test]
    async fn ch_query_rejects_malformed_response() {
        let (ch, _requests) = ch_with_raw_responses(vec!["no headers".to_string()]).await;

        let err = ch.query("SELECT 1").await.unwrap_err().to_string();

        assert!(err.contains("malformed HTTP response from ClickHouse"));
    }

    #[tokio::test]
    async fn count_id_formats_query_and_parses_count() {
        let (ch, mut requests) = ch_with_responses(vec![(OK, "2\n")]).await;

        let count = ch.count_id(42).await.unwrap();

        assert_eq!(count, 2);
        let request = recv_request(&mut requests).await;
        assert!(request.ends_with("SELECT count() FROM demo.users WHERE id = 42"));
    }

    #[tokio::test]
    async fn count_id_rejects_bad_count() {
        let (ch, _requests) = ch_with_responses(vec![(OK, "abc\n")]).await;

        let err = ch.count_id(42).await.unwrap_err().to_string();

        assert!(err.contains("parse count() response"));
    }

    #[tokio::test]
    async fn wait_visible_returns_seen_instant_after_positive_count() {
        let (ch, _requests) = ch_with_responses(vec![(OK, "0\n"), (OK, "1\n")]).await;

        let seen = wait_visible(&ch, 42, Duration::from_millis(1), Duration::from_millis(50))
            .await
            .unwrap();

        assert!(seen.is_some());
    }

    #[tokio::test]
    async fn wait_visible_times_out_after_query_errors() {
        let (ch, _requests) =
            ch_with_responses(vec![("HTTP/1.0 500 Internal Error", "boom\n")]).await;

        let seen = wait_visible(&ch, 42, Duration::from_millis(1), Duration::from_millis(5))
            .await
            .unwrap();

        assert!(seen.is_none());
    }

    async fn ch_with_responses(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (ChHttp, mpsc::UnboundedReceiver<String>) {
        let responses = responses
            .into_iter()
            .map(|(status, body)| {
                format!("{status}\r\nContent-Length: {}\r\n\r\n{body}", body.len())
            })
            .collect();

        ch_with_raw_responses(responses).await
    }

    async fn ch_with_raw_responses(
        responses: Vec<String>,
    ) -> (ChHttp, mpsc::UnboundedReceiver<String>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            for response in responses {
                let Ok((mut socket, _peer)) = listener.accept().await else {
                    return;
                };
                let request = read_request(&mut socket).await;
                let _ = tx.send(request);
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        (
            ChHttp::new("127.0.0.1".to_string(), port, "demo.users".to_string()),
            rx,
        )
    }

    async fn read_request(socket: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];

        loop {
            let n = socket.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if request_complete(&buf) {
                break;
            }
        }

        String::from_utf8(buf).unwrap()
    }

    fn request_complete(buf: &[u8]) -> bool {
        let Some(header_end) = buf.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let head = String::from_utf8_lossy(&buf[..header_end]);
        let content_len = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(0);

        buf.len() >= header_end + 4 + content_len
    }

    async fn recv_request(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
