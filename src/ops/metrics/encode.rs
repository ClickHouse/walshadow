//! OpenMetrics family encoders shared by every collector

use std::fmt;

use prometheus_client::encoding::{
    DescriptorEncoder, EncodeCounterValue, EncodeGaugeValue, MetricEncoder, NoLabelSet,
};
use prometheus_client::metrics::MetricType;

use crate::atomic_stats::HistogramSnapshot;

/// Family declaration off a snapshot field name: a `_by_<label>` tail names
/// the label rather than the family, and OpenMetrics appends `_total` to
/// counter samples itself, so a counter family declares the name without it
pub(crate) fn declare<'s>(
    enc: &'s mut DescriptorEncoder<'_>,
    name: &'s str,
    help: &str,
    kind: MetricType,
) -> Result<MetricEncoder<'s>, fmt::Error> {
    let name = name.rsplit_once("_by_").map_or(name, |(head, _)| head);
    let name = if matches!(kind, MetricType::Counter) {
        name.strip_suffix("_total").unwrap_or(name)
    } else {
        name
    };
    enc.encode_descriptor(name, help, None, kind)
}

pub(crate) fn counter<V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: V,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Counter)?
        .encode_counter::<NoLabelSet, _, u64>(&value, None)
}

pub(crate) fn gauge<V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: V,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Gauge)?.encode_gauge(&value)
}

/// One `key=` labelled series per `(label, value)`, under one declaration
pub(crate) fn counter_series<'a, V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    series: impl IntoIterator<Item = (&'a str, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (label, value) in series {
        family
            .encode_family(&[(key, label)])?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

pub(crate) fn counter_pairs<'a, V: EncodeCounterValue, const N: usize>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    keys: [&str; N],
    series: impl IntoIterator<Item = ([&'a str; N], V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (values, value) in series {
        let labels: [(&str, &str); N] = std::array::from_fn(|i| (keys[i], values[i]));
        family
            .encode_family(&labels)?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

pub(crate) fn gauge_series<'a, V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    key: &str,
    series: impl IntoIterator<Item = (&'a str, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Gauge)?;
    for (label, value) in series {
        family
            .encode_family(&[(key, label)])?
            .encode_gauge(&value)?;
    }
    Ok(())
}

pub(crate) fn histogram(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    value: &HistogramSnapshot,
) -> fmt::Result {
    declare(enc, name, help, MetricType::Histogram)?.encode_histogram::<NoLabelSet>(
        value.sum,
        value.count,
        value.encoded_buckets(),
        None,
    )
}
