//! Process-wide `tracing` wiring, with an optional OTLP export layer.

/// OTLP/gRPC batch tracer provider for `endpoint`. Must run inside the tokio
/// runtime (tonic exporter + batch worker need it).
pub(crate) fn build_otlp_provider(
    endpoint: &str,
) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    // Head sampling happens at span creation (per txn, see TxnSpanRegistry),
    // so the SDK exports everything it's handed.
    Ok(SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_batch_exporter(exporter)
        .with_resource(Resource::builder().with_service_name("walshadow").build())
        .build())
}

/// Wire `tracing` once per process (`RUST_LOG` filter, default
/// `warn,walshadow=info`). With `otlp_endpoint` set, stacks an OTel layer on
/// the stderr `fmt` layer; the returned provider must be `.shutdown()` at exit.
pub(crate) fn init_tracing(
    otlp_endpoint: Option<&str>,
) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    use std::io::IsTerminal;

    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::prelude::*;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        // Redirected stderr is read by tests and log collectors, not a pager
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr);

    // Best-effort: a bad endpoint logs and degrades to no-traces rather
    // than refusing to boot — observability never blocks the pipeline.
    let provider = if let Some(endpoint) = otlp_endpoint {
        match build_otlp_provider(endpoint) {
            Ok(p) => {
                opentelemetry::global::set_tracer_provider(p.clone());
                Some(p)
            }
            Err(e) => {
                eprintln!("walshadow: OTLP exporter init failed for {endpoint}: {e:#}");
                None
            }
        }
    } else {
        None
    };

    // `walshadow::trace` spans only feed the OTLP exporter; with none attached
    // they are pure per-record overhead, so disable that target — unless the
    // user explicitly set it in RUST_LOG.
    let user_set_trace = std::env::var("RUST_LOG")
        .map(|v| v.contains("walshadow::trace"))
        .unwrap_or(false);
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,walshadow=info"));
    let filter = if provider.is_some() || user_set_trace {
        filter
    } else {
        filter.add_directive(
            "walshadow::trace=off"
                .parse()
                .expect("static trace-off directive parses"),
        )
    };
    let otel_layer = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("walshadow")));

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init();
    provider
}
