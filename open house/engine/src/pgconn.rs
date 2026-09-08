use anyhow::Result;
use std::sync::Arc;
use tokio_postgres::Client;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};

/// Single place that opens a Postgres connection.
///
/// Managed Postgres requires TLS, so a plain `NoTls` connect fails outright.
/// rustls is already in the tree via reqwest, so this reuses it rather than
/// pulling in OpenSSL.
///
/// Verification follows libpq's own `sslmode` semantics: `verify-ca` and
/// `verify-full` check the chain against the platform roots, anything lower
/// encrypts without verifying. That matters here because ClickHouse Cloud's
/// managed Postgres presents a certificate from a private Ubicloud CA that is
/// in no public root store — `sslmode=require` is exactly the mode psql uses
/// to talk to it. Ask for `verify-full` and you will need that CA on the box.
pub async fn connect(dsn: &str) -> Result<Client> {
    // reqwest and tokio-postgres-rustls both pull rustls in, so no provider is
    // installed by default; pick one explicitly, once.
    static PROVIDER: std::sync::Once = std::sync::Once::new();
    PROVIDER.call_once(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    });

    let verifying = dsn.contains("sslmode=verify-ca") || dsn.contains("sslmode=verify-full");

    let tls_cfg = if verifying {
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(EncryptOnly))
            .with_no_client_auth()
    };

    let cfg: tokio_postgres::Config = dsn.parse()?;
    let (client, conn) = cfg
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls_cfg))
        .await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// Encrypts the connection without authenticating the peer — libpq's
/// `sslmode=require`. Signature checks still run, so the handshake is real;
/// what is skipped is chain and hostname validation.
#[derive(Debug)]
struct EncryptOnly;

impl ServerCertVerifier for EncryptOnly {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(message, cert, dss, &provider().signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(message, cert, dss, &provider().signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider().signature_verification_algorithms.supported_schemes()
    }
}

fn provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(tokio_rustls::rustls::crypto::ring::default_provider()))
}
