//! JSONB sizing, rendering, and combined passes without servers.
//! `cargo bench --bench jsonb -- --samples 5 --millis 100`

use std::hint::black_box;
use std::time::{Duration, Instant};

use clap::Parser;
use walshadow::codecs::{decode_jsonb_with_capacity, jsonb_text_len};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..))]
    samples: u32,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    millis: u64,
    #[arg(long, default_value = "")]
    filter: String,
}

fn measure(args: &Args, mut f: impl FnMut()) -> f64 {
    let mut samples = Vec::new();
    for _ in 0..args.samples {
        let start = Instant::now();
        let mut iterations = 0u64;
        loop {
            for _ in 0..256 {
                f();
            }
            iterations += 256;
            if start.elapsed() >= Duration::from_millis(args.millis) {
                break;
            }
        }
        samples.push(start.elapsed().as_nanos() as f64 / iterations as f64);
    }
    samples.sort_unstable_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn fixture(values: &[String]) -> (Vec<u8>, String) {
    let scalar = values.len() == 1;
    let header = if scalar { 0x5000_0000 } else { 0x4000_0000 };
    let mut body = (header | values.len() as u32).to_le_bytes().to_vec();
    for value in values {
        body.extend_from_slice(&(value.len() as u32).to_le_bytes());
    }
    for value in values {
        body.extend_from_slice(value.as_bytes());
    }
    let strings: Vec<_> = values
        .iter()
        .map(|value| serde_json::to_string(value).unwrap())
        .collect();
    let expected = if scalar {
        strings[0].clone()
    } else {
        format!("[{}]", strings.join(", "))
    };
    (body, expected)
}

fn run(args: &Args, name: &str, values: &[String]) {
    if !name.contains(&args.filter) {
        return;
    }
    let (body, expected) = fixture(values);
    let len = jsonb_text_len(&body).unwrap();
    assert_eq!(len, expected.len());
    assert_eq!(decode_jsonb_with_capacity(&body, len).unwrap(), expected);
    for mode in ["size", "render", "both"] {
        let ns = measure(args, || {
            let body = black_box(body.as_slice());
            match mode {
                "size" => {
                    black_box(jsonb_text_len(body).unwrap());
                }
                "render" => {
                    black_box(decode_jsonb_with_capacity(body, black_box(len)).unwrap());
                }
                _ => {
                    let len = jsonb_text_len(body).unwrap();
                    black_box(decode_jsonb_with_capacity(body, len).unwrap());
                }
            }
        });
        println!("{name},{mode},{},{ns:.2}", body.len());
    }
}

fn main() {
    if std::env::args().any(|arg| arg == "--list") {
        return;
    }
    let args = Args::parse_from(std::env::args().filter(|arg| arg != "--bench"));
    println!("case,mode,body_bytes,median_ns");
    for len in [8, 32, 128, 1024, 16384] {
        for (name, pattern) in [
            ("plain", "abcdefgh"),
            (
                "sparse",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\"\\",
            ),
            ("dense", "ab\"\\"),
            ("control", "\u{1}\u{8}\u{c}\n\r\t\u{1f}x"),
            ("unicode", "é中😀"),
        ] {
            let value: String = pattern.chars().cycle().take(len).collect();
            run(&args, &format!("{name}/{len}"), &[value]);
        }
    }
    run(&args, "array/64x8", &vec!["abcdefgh".to_owned(); 64]);
}
