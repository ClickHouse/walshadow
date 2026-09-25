use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::ch::CompressionChoice;
use crate::emit::ch_emitter::{EmitterConfig, RetryConfig};

pub(crate) async fn retry_server(
    max_attempts: u32,
    sql: &str,
    insert: bool,
    idle: bool,
) -> (EmitterConfig, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = EmitterConfig {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        compression: CompressionChoice::None,
        retry: RetryConfig {
            max_attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        },
        idle_reconnect: if idle { Duration::ZERO } else { Duration::MAX },
        ..EmitterConfig::default()
    };
    assert!(sql.len() < 128);
    // Empty query ID, clickhouse-c ClientInfo, no settings or interserver secret,
    // complete stage, no compression
    let mut expected = vec![1, 0, 1, 0, 0, 20];
    expected.extend_from_slice(b"[::ffff:127.0.0.1]:0");
    expected.extend_from_slice(&[0; 8]);
    expected.extend_from_slice(&[1, 0, 0, 19]);
    expected.extend_from_slice(b"clickhouse-c client");
    expected.extend_from_slice(&[0, 0, 0xc1, 0xa9, 0x03, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[0, 0, 2, 0, sql.len() as u8]);
    expected.extend_from_slice(sql.as_bytes());
    // Empty params, then empty Data block with BlockInfo
    const EMPTY_DATA: &[u8] = &[2, 0, 1, 0, 2, 0xff, 0xff, 0xff, 0xff, 0, 0, 0];
    expected.push(0);
    expected.extend_from_slice(EMPTY_DATA);
    if insert {
        // One UInt8 column x without custom serialization, one row 42
        expected.extend_from_slice(
            b"\x02\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01\x01\x01x\x05UInt8\x00\x2a",
        );
        expected.extend_from_slice(EMPTY_DATA);
    }
    let server = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), async {
            for connection in 0..=max_attempts + u32::from(idle) {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut hello = [0; 1024];
                assert!(stream.read(&mut hello).await.unwrap() > 0);
                if connection == 1 {
                    continue;
                }
                // Hello: empty name, version 1.0, revision 54462, empty timezone &
                // display name, patch 0, no password rules, zero nonce
                stream
                    .write_all(&[
                        0, 0, 1, 0, 0xbe, 0xa9, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    ])
                    .await
                    .unwrap();
                // Empty quota key addendum, then Ping
                assert_eq!(stream.read_u16().await.unwrap(), 0x0004);
                stream.write_all(&[4]).await.unwrap();
                if idle && connection == 0 {
                    continue;
                }
                let mut request = vec![0; expected.len()];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, expected);
                if connection > 1 {
                    stream.write_all(&[5]).await.unwrap();
                }
            }
        })
        .await
        .expect("retry server completed");
    });
    (config, server)
}
