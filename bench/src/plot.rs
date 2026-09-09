use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use ab_glyph::{Font, FontRef, PxScale, ScaleFont, point};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use tiny_skia::{Color, Paint, PathBuilder, Pixmap, Rect, Stroke, Transform};

type Rgb = (u8, u8, u8);
const BG: Rgb = (0x29, 0x25, 0x22);
const FG: Rgb = (0xEC, 0xE1, 0xD7);
const MUTED: Rgb = (0xC1, 0xA7, 0x8E);
const GRID: Rgb = (0x40, 0x3A, 0x36);
const COLORS: [Rgb; 12] = [
    (0xFA, 0xFF, 0x69),
    (0xFC, 0x7F, 0x5D),
    (0x89, 0xB3, 0xE6),
    (0x85, 0xB6, 0x95),
    (0xCF, 0x9B, 0xC2),
    (0xE4, 0x9B, 0x5D),
    (0xA3, 0xA9, 0xCE),
    (0x89, 0xB3, 0xB6),
    (0xB3, 0x80, 0xB0),
    (0xD3, 0xCF, 0xA0),
    (0x6D, 0xD6, 0xB6),
    (0xDE, 0xA6, 0xA0),
];
const WIDTH: u32 = 1180;
const LEFT: f32 = 108.0;
const RIGHT: f32 = 1130.0;

struct Canvas {
    pixmap: Pixmap,
    font: FontRef<'static>,
}

impl Canvas {
    fn new(height: u32) -> Result<Self> {
        let mut pixmap = Pixmap::new(WIDTH, height).context("allocate chart")?;
        pixmap.fill(Color::from_rgba8(BG.0, BG.1, BG.2, 255));
        Ok(Self {
            pixmap,
            font: FontRef::try_from_slice(dejavu::sans::regular()).context("embedded font")?,
        })
    }

    fn text(&mut self, x: f32, y: f32, size: f32, text: &str, color: Rgb) {
        let font = self.font.as_scaled(PxScale::from(size));
        let mut caret = x;
        let mut previous = None;
        let width = self.pixmap.width();
        let height = self.pixmap.height();
        for ch in text.chars() {
            let id = self.font.glyph_id(ch);
            if let Some(previous) = previous {
                caret += font.kern(previous, id);
            }
            let glyph = id.with_scale_and_position(size, point(caret, y + font.ascent()));
            caret += font.h_advance(id);
            previous = Some(id);
            if let Some(glyph) = self.font.outline_glyph(glyph) {
                let bounds = glyph.px_bounds();
                let data = self.pixmap.data_mut();
                glyph.draw(|gx, gy, coverage| {
                    let px = bounds.min.x as i32 + gx as i32;
                    let py = bounds.min.y as i32 + gy as i32;
                    if px < 0 || py < 0 || px as u32 >= width || py as u32 >= height {
                        return;
                    }
                    let index = ((py as u32 * width + px as u32) * 4) as usize;
                    for (channel, value) in [color.0, color.1, color.2].into_iter().enumerate() {
                        data[index + channel] = (value as f32 * coverage
                            + data[index + channel] as f32 * (1.0 - coverage))
                            .round() as u8;
                    }
                });
            }
        }
    }

    fn line(&mut self, points: &[(f32, f32)], color: Rgb, width: f32) {
        let Some(&(x, y)) = points.first() else {
            return;
        };
        let mut path = PathBuilder::new();
        path.move_to(x, y);
        for &(x, y) in &points[1..] {
            path.line_to(x, y);
        }
        if let Some(path) = path.finish() {
            self.pixmap.stroke_path(
                &path,
                &paint(color),
                &Stroke {
                    width,
                    ..Stroke::default()
                },
                Transform::identity(),
                None,
            );
        }
    }

    fn rect(&mut self, x: f32, y: f32, width: f32, height: f32, color: Rgb) {
        if let Some(rect) = Rect::from_xywh(x, y, width, height) {
            self.pixmap
                .fill_rect(rect, &paint(color), Transform::identity(), None);
        }
    }

    fn save(&self, path: &Path) -> Result<()> {
        self.pixmap
            .save_png(path)
            .with_context(|| format!("save {}", path.display()))
    }
}

fn paint(color: Rgb) -> Paint<'static> {
    let mut paint = Paint::default();
    paint.set_color_rgba8(color.0, color.1, color.2, 255);
    paint
}

fn number(value: f64) -> String {
    if value.abs() >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value.abs() >= 1_000.0 {
        format!("{:.1}k", value / 1_000.0)
    } else if value.abs() >= 10.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.2}")
    }
}

struct Series {
    label: String,
    points: Vec<(f64, f64)>,
    color: Rgb,
    step: bool,
}

fn chart(path: &Path, title: &str, xlabel: &str, ylabel: &str, series: &[Series]) -> Result<()> {
    if series.is_empty() {
        return Ok(());
    }
    let mut canvas = Canvas::new(470 + series.len() as u32 * 23)?;
    canvas.text(24.0, 14.0, 23.0, title, FG);
    canvas.text(LEFT, 50.0, 16.0, ylabel, MUTED);
    let xmax = series
        .iter()
        .flat_map(|s| &s.points)
        .map(|p| p.0)
        .fold(0.0, f64::max)
        .max(0.001);
    let ymax = series
        .iter()
        .flat_map(|s| &s.points)
        .map(|p| p.1)
        .fold(0.0, f64::max)
        .max(0.001)
        * 1.05;
    let ymin = series
        .iter()
        .flat_map(|s| &s.points)
        .map(|p| p.1)
        .fold(0.0, f64::min);
    let xy = |(x, y): (f64, f64)| {
        (
            LEFT + (x / xmax) as f32 * (RIGHT - LEFT),
            380.0 - ((y - ymin) / (ymax - ymin)) as f32 * 300.0,
        )
    };
    for i in 0..=5 {
        let x = xmax * i as f64 / 5.0;
        let y = ymin + (ymax - ymin) * i as f64 / 5.0;
        let (px, py) = xy((x, y));
        canvas.line(&[(LEFT, py), (RIGHT, py)], GRID, 1.0);
        canvas.line(&[(px, 80.0), (px, 380.0)], GRID, 1.0);
        canvas.text(12.0, py - 8.0, 15.0, &number(y), MUTED);
        canvas.text(px - 12.0, 390.0, 15.0, &number(x), MUTED);
    }
    canvas.text(LEFT, 418.0, 16.0, xlabel, MUTED);
    for (i, series) in series.iter().enumerate() {
        let mut points = Vec::new();
        for (index, &p) in series.points.iter().enumerate() {
            if series.step && index > 0 {
                points.push(xy((p.0, series.points[index - 1].1)));
            }
            points.push(xy(p));
        }
        canvas.line(&points, series.color, 2.0);
        for &(x, y) in &points {
            canvas.rect(x - 1.5, y - 1.5, 3.0, 3.0, series.color);
        }
        let y = 456.0 + i as f32 * 23.0;
        canvas.line(&[(24.0, y + 8.0), (48.0, y + 8.0)], series.color, 3.0);
        canvas.text(58.0, y, 16.0, &series.label, FG);
    }
    canvas.save(path)
}

fn read_file(path: &Path, label: &str) -> Result<Vec<Value>> {
    let text = fs::read_to_string(path)?;
    let mut records = Vec::new();
    for line in text.lines() {
        let Some(json) = line.strip_prefix("BENCH_JSON ") else {
            continue;
        };
        let mut record: Value =
            serde_json::from_str(json).with_context(|| format!("parse {}", path.display()))?;
        if !record["bench"].is_string() {
            continue;
        }
        record["label"] = Value::from(label);
        if let Ok(provenance) = fs::read_to_string(path.with_file_name("provenance.txt")) {
            for line in provenance.lines() {
                if let Some((key, value)) = line.split_once(':')
                    && matches!(
                        key,
                        "daemon_revision"
                            | "daemon_image"
                            | "driver_image"
                            | "instance_type"
                            | "az"
                    )
                {
                    record[key] = Value::from(value.trim());
                }
                if line.ends_with("/opt/walshadow/ch-config.toml") {
                    record["config_sha256"] =
                        Value::from(line.split_whitespace().next().unwrap_or_default());
                }
                if line.starts_with("postgres (PostgreSQL)") {
                    record["postgres_version"] = Value::from(line.trim());
                }
            }
        }
        record["file"] = Value::from(path.display().to_string());
        if text.contains("# FAILED") {
            record["complete"] = Value::Bool(false);
        }
        if record["complete"] != true {
            for field in ["elapsed_secs", "rows_per_sec", "mib_per_sec"] {
                record[field] = Value::Null;
            }
        }
        records.push(record);
    }
    Ok(records)
}

fn read_run(path: &Path, label: &str) -> Result<Vec<Value>> {
    if path.is_file() {
        return read_file(path, label);
    }
    let mut files = fs::read_dir(path)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.sort();
    let mut records = Vec::new();
    for file in files {
        if file.extension().is_some_and(|ext| ext == "txt") {
            let name = file.file_stem().unwrap_or_default().to_string_lossy();
            let label = if name == "bench" {
                label.to_owned()
            } else {
                format!("{label}/{name}")
            };
            records.extend(read_file(&file, &label)?);
        }
    }
    Ok(records)
}

fn label(record: &Value) -> String {
    let name = record["label"].as_str().unwrap_or("unknown");
    if record["complete"].as_bool() == Some(true) {
        name.into()
    } else {
        format!("{name} [incomplete]")
    }
}

fn pairs(value: &Value) -> Vec<(f64, f64)> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| {
            let (x, y) = (v[0].as_f64()?, v[1].as_f64()?);
            (x.is_finite() && y.is_finite()).then_some((x, y))
        })
        .collect()
}

fn validate_comparison(records: &[&Value]) -> Result<()> {
    let first = records[0];
    for record in records.iter().skip(1) {
        for key in [
            "count_interval_ms",
            "seed_batch_rows",
            "seed_wal_drained",
            "driver_image",
            "config_sha256",
            "instance_type",
            "az",
            "postgres_version",
        ] {
            if record[key] != first[key] {
                bail!(
                    "incomparable {key}: {} versus {}",
                    first["label"],
                    record["label"]
                );
            }
        }
        if record["measurement_version"].as_u64().unwrap_or(1)
            != first["measurement_version"].as_u64().unwrap_or(1)
        {
            bail!("cannot compare different measurement versions");
        }
        for key in ["table_bytes", "database_bytes"] {
            if let (Some(a), Some(b)) = (first[key].as_f64(), record[key].as_f64())
                && (a - b).abs() > a.max(b) * 0.02
            {
                bail!("{key} differs by more than 2%, restore matching fixture");
            }
        }
        for other in records {
            if record["daemon_revision"].is_string()
                && record["daemon_revision"] == other["daemon_revision"]
                && record["label"] != other["label"]
            {
                bail!("same revision has different labels, use one label for repeated runs");
            }
            if record["daemon_image"].is_string()
                && record["daemon_image"] == other["daemon_image"]
                && record["daemon_revision"] != other["daemon_revision"]
            {
                bail!("same daemon image attributed to different revisions");
            }
        }
    }
    Ok(())
}

fn median(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn comparison(path: &Path, _title: &str, records: &[&Value]) -> Result<()> {
    validate_comparison(records)?;
    let first = records[0];
    let mut groups: Vec<Vec<&Value>> = Vec::new();
    for record in records {
        if let Some(group) = groups.iter_mut().find(|group| {
            group[0]["label"] == record["label"]
                && group[0]["daemon_revision"] == record["daemon_revision"]
        }) {
            group.push(record);
        } else {
            groups.push(vec![record]);
        }
    }
    let panel_height = 74 + groups.len() as u32 * 58;
    let mut canvas = Canvas::new(200 + panel_height * 2)?;
    let shape = if first["bench"] == "bootstrap" {
        "Greenfield bootstrap"
    } else {
        "Static per-table initial load"
    };
    let mode = first["mode"]
        .as_str()
        .unwrap_or("unknown")
        .replace('_', " ");
    canvas.text(
        24.0,
        12.0,
        22.0,
        &format!(
            "{shape} | {mode} | {} rows x {} B payload",
            number(first["expected_rows"].as_f64().unwrap_or_default()),
            first["row_width"]
        ),
        FG,
    );
    canvas.text(
        24.0,
        44.0,
        16.0,
        "Bars: median | whiskers: observed min-max | marks: individual successful runs",
        MUTED,
    );
    for (panel, (key, heading)) in [
        ("elapsed_secs", "Rows visible (seconds), lower is better"),
        (
            "mib_per_sec",
            "Table size / time to visible (MiB/s), higher is better",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let precision = if key == "elapsed_secs" { 1 } else { 2 };
        let top = 82.0 + panel as f32 * panel_height as f32;
        canvas.text(24.0, top, 18.0, heading, MUTED);
        let maximum = records
            .iter()
            .filter(|r| r["complete"] == true)
            .filter_map(|r| r[key].as_f64())
            .fold(0.0, f64::max)
            .max(0.001)
            * 1.08;
        let x = |value: f64| 435.0 + (value / maximum) as f32 * 525.0;
        for (index, group) in groups.iter().enumerate() {
            let y = top + 34.0 + index as f32 * 58.0;
            let mut values: Vec<_> = group
                .iter()
                .filter(|r| r["complete"] == true)
                .filter_map(|r| r[key].as_f64())
                .collect();
            values.sort_by(f64::total_cmp);
            canvas.text(
                24.0,
                y,
                16.0,
                group[0]["label"].as_str().unwrap_or("unknown"),
                FG,
            );
            canvas.text(
                24.0,
                y + 22.0,
                13.0,
                &format!("n={} / {} complete", values.len(), group.len()),
                MUTED,
            );
            if values.is_empty() {
                canvas.text(435.0, y, 16.0, "INCOMPLETE", COLORS[1]);
                continue;
            }
            let mid = median(&values);
            canvas.rect(
                435.0,
                y + 2.0,
                x(mid) - 435.0,
                20.0,
                COLORS[index % COLORS.len()],
            );
            canvas.line(
                &[
                    (x(values[0]), y + 12.0),
                    (x(*values.last().unwrap()), y + 12.0),
                ],
                FG,
                2.0,
            );
            for value in &values {
                canvas.line(&[(x(*value), y + 5.0), (x(*value), y + 19.0)], FG, 2.0);
            }
            canvas.text(970.0, y, 16.0, &format!("{mid:.precision$}"), FG);
            canvas.text(
                970.0,
                y + 22.0,
                12.0,
                &format!(
                    "{:.precision$}-{:.precision$}",
                    values[0],
                    values.last().unwrap()
                ),
                MUTED,
            );
        }
        let y = top + panel_height as f32 - 12.0;
        for value in [0.0, maximum / 2.0, maximum] {
            canvas.text(x(value), y, 12.0, &number(value), MUTED);
        }
    }
    let footer = 94.0 + panel_height as f32 * 2.0;
    let version = first["measurement_version"].as_u64().unwrap_or(1);
    canvas.text(24.0, footer, 14.0, if version == 1 {
        "Opt-in to visibility, includes outstanding seed WAL"
    } else { "Seed WAL drained before opt-in (or fresh bootstrap); setup and settlement recorded separately" }, MUTED);
    canvas.text(24.0, footer + 23.0, 14.0, "Range shows repeat variation, not a confidence interval; throughput is not backup transfer bandwidth", MUTED);
    let verified = records.iter().all(|r| {
        [
            "daemon_revision",
            "daemon_image",
            "driver_image",
            "config_sha256",
        ]
        .iter()
        .all(|key| r[*key].is_string())
    });
    let environment = if verified {
        format!(
            "{} | {} | polling {} ms | table {:.1} MiB",
            first["instance_type"]
                .as_str()
                .unwrap_or("hardware unspecified"),
            first["postgres_version"]
                .as_str()
                .unwrap_or("PG unspecified"),
            first["count_interval_ms"],
            first["table_bytes"].as_f64().unwrap_or_default() / 1048576.0
        )
    } else {
        "UNVERIFIED PROVENANCE: do not use for revision attribution".to_string()
    };
    canvas.text(
        24.0,
        footer + 46.0,
        14.0,
        &environment,
        if verified { MUTED } else { COLORS[1] },
    );
    canvas.save(path)
}

pub fn generate(runs: &[PathBuf], labels: &[String], out: &Path) -> Result<()> {
    if !labels.is_empty() && labels.len() != runs.len() {
        bail!("supply one --label per --run, or omit labels");
    }
    let mut records = Vec::new();
    for (index, run) in runs.iter().enumerate() {
        let default = run.file_name().unwrap_or_default().to_string_lossy();
        records.extend(read_run(
            run,
            labels.get(index).map(String::as_str).unwrap_or(&default),
        )?);
    }
    if records.is_empty() {
        bail!("no BENCH_JSON summaries found");
    }
    fs::create_dir_all(out)?;
    fs::write(
        out.join("summary.json"),
        serde_json::to_string_pretty(&records)? + "\n",
    )?;
    let fields = [
        "label",
        "bench",
        "mode",
        "complete",
        "expected_rows",
        "row_width",
        "seed_secs",
        "table_bytes",
        "total_relation_bytes",
        "database_bytes",
        "elapsed_secs",
        "settled_secs",
        "measurement_version",
        "daemon_revision",
        "rows_per_sec",
        "mib_per_sec",
        "count_errors",
        "metrics_errors",
    ];
    let mut csv = csv::Writer::from_path(out.join("summary.csv"))?;
    csv.write_record(fields)?;
    for record in &records {
        csv.write_record(fields.map(|field| match &record[field] {
            Value::Null => String::new(),
            Value::String(value) => value.clone(),
            value => value.to_string(),
        }))?;
    }
    csv.flush()?;
    let mut stages = csv::Writer::from_path(out.join("stages.csv"))?;
    stages.write_record(["label", "source", "stage", "value"])?;
    for record in &records {
        for key in ["harness_stage_secs", "daemon_stage_deltas"] {
            if let Some(values) = record[key].as_object() {
                for (stage, value) in values {
                    stages.write_record([
                        label(record),
                        key.to_string(),
                        stage.clone(),
                        value.to_string(),
                    ])?;
                }
            }
        }
    }
    stages.flush()?;
    let shapes: BTreeSet<_> = records.iter().filter_map(|r| r["bench"].as_str()).collect();
    for shape in shapes {
        if matches!(shape, "initial-load" | "bootstrap") {
            for kind in ["count", "rate"] {
                let stale = out.join(format!("{shape}-{kind}.png"));
                if stale.exists() {
                    fs::remove_file(stale)?;
                }
            }
            continue;
        }
        if !shape
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
        {
            bail!("invalid benchmark shape {shape:?}");
        }
        for (kind, ylabel) in [
            ("count", "Visible row versions"),
            ("rate", "Sampled visible rows/s"),
            ("latency", "Fraction of successful probes"),
        ] {
            let mut series = Vec::new();
            for record in records.iter().filter(|r| r["bench"] == shape) {
                let mut points = pairs(&record["curve"]);
                if kind == "rate" {
                    points = points
                        .windows(2)
                        .filter_map(|w| {
                            let dt = w[1].0 - w[0].0;
                            (dt > 0.0).then(|| ((w[0].0 + w[1].0) / 2.0, (w[1].1 - w[0].1) / dt))
                        })
                        .collect();
                } else if kind == "latency" {
                    let mut samples: Vec<f64> = record["latency_ms"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_f64)
                        .filter(|v| v.is_finite())
                        .collect();
                    samples.sort_by(f64::total_cmp);
                    points = samples
                        .iter()
                        .enumerate()
                        .map(|(i, &v)| (v, (i + 1) as f64 / samples.len() as f64))
                        .collect();
                }
                if points.is_empty() {
                    continue;
                }
                let label = if kind == "latency" {
                    format!("{} ({} timeouts)", label(record), record["timeouts"])
                } else {
                    label(record)
                };
                series.push(Series {
                    label,
                    points,
                    color: COLORS[series.len() % COLORS.len()],
                    step: kind != "rate",
                });
            }
            chart(
                &out.join(format!("{shape}-{kind}.png")),
                &format!("{shape}: {ylabel}"),
                if kind == "latency" {
                    "Commit to visible (ms)"
                } else {
                    "Seconds from measurement start"
                },
                ylabel,
                &series,
            )?;
        }
    }
    let mut groups: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for record in &records {
        if !matches!(record["bench"].as_str(), Some("initial-load" | "bootstrap")) {
            continue;
        }
        let key = format!(
            "{}-{}-{}x{}",
            record["bench"].as_str().unwrap(),
            record["mode"].as_str().unwrap_or("unknown"),
            record["expected_rows"],
            record["row_width"]
        );
        if !key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
        {
            bail!("invalid initial-load group {key:?}");
        }
        groups.entry(key).or_default().push(record);
    }
    for (key, group) in groups {
        comparison(&out.join(format!("{key}-comparison.png")), &key, &group)?;
        let series: Vec<_> = group
            .iter()
            .enumerate()
            .map(|(index, record)| Series {
                label: label(record),
                points: pairs(&record["curve"]),
                color: COLORS[index % COLORS.len()],
                step: true,
            })
            .collect();
        chart(
            &out.join(format!("{key}-count.png")),
            &format!("{key}: destination visibility"),
            "Seconds from measurement start",
            "Visible row versions",
            &series,
        )?;
    }
    for (index, record) in records.iter().enumerate() {
        let Some(samples) = record["metrics"].as_array() else {
            continue;
        };
        let keys: BTreeSet<&str> = samples
            .iter()
            .filter_map(|s| s[1].as_object())
            .flat_map(|values| values.keys().map(String::as_str))
            .filter(|key| {
                key.split('{')
                    .next()
                    .unwrap_or(key)
                    .ends_with("_seconds_total")
            })
            .collect();
        let mut series = Vec::new();
        for key in keys {
            let mut points: Vec<_> = samples
                .iter()
                .filter_map(|s| Some((s[0].as_f64()?, s[1][key].as_f64()?)))
                .collect();
            if points.len() < 2 || points.windows(2).any(|w| w[1].1 < w[0].1) {
                continue;
            }
            let baseline = if record["bench"] == "bootstrap" && record["measurement_version"] == 2 {
                0.0
            } else {
                points[0].1
            };
            if points.last().unwrap().1 == baseline {
                continue;
            }
            for point in &mut points {
                point.1 -= baseline;
            }
            series.push(Series {
                label: key.to_owned(),
                points,
                color: COLORS[series.len() % COLORS.len()],
                step: false,
            });
        }
        chart(
            &out.join(format!("stages-{index}.png")),
            &format!("{}: overlapping stage counters", label(record)),
            "Seconds from measurement start",
            "Cumulative counter delta (s), not wall-time shares",
            &series,
        )?;
    }
    println!("graphs and summaries → {}", out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_mixed_protocols_fixtures_and_revision_labels() {
        let base = serde_json::json!({"label": "base", "measurement_version": 2,
            "daemon_revision": "a", "daemon_image": "one", "database_bytes": 1000});
        let mut other = base.clone();
        other["measurement_version"] = 1.into();
        assert!(validate_comparison(&[&base, &other]).is_err());
        other = base.clone();
        other["database_bytes"] = 1100.into();
        assert!(validate_comparison(&[&base, &other]).is_err());
        other = base.clone();
        other["label"] = "optimize".into();
        assert!(validate_comparison(&[&base, &other]).is_err());
        other["daemon_revision"] = "b".into();
        assert!(validate_comparison(&[&base, &other]).is_err());
        other["daemon_image"] = "two".into();
        validate_comparison(&[&base, &other]).unwrap();
        assert_eq!(median(&[1.0, 2.0, 90.0]), 2.0);
    }

    #[test]
    fn failed_footer_censors_rates_and_renders_png() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("run.txt");
        fs::write(&input, "BENCH_JSON {\"bench\":\"initial-load\",\"mode\":\"copy\",\"expected_rows\":10,\"row_width\":8,\"complete\":true,\"elapsed_secs\":2,\"mib_per_sec\":1,\"curve\":[[0,0],[2,10]]}\n# FAILED\n").unwrap();
        let out = dir.path().join("graphs");
        generate(&[input], &[], &out).unwrap();
        let records: Value =
            serde_json::from_str(&fs::read_to_string(out.join("summary.json")).unwrap()).unwrap();
        assert_eq!(records[0]["complete"], false);
        assert!(records[0]["elapsed_secs"].is_null());
        let bytes = fs::read(out.join("initial-load-copy-10x8-comparison.png")).unwrap();
        assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
        assert!(!out.join("initial-load-rate.png").exists());
    }
}
