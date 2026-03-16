use std::error::Error;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Clone, Copy)]
struct CliOptions {
    target_mode: TargetMode,
    pipeline_depth: usize,
}

#[derive(Debug, Clone, Copy)]
enum TargetMode {
    Both,
    FoxkvOnly,
}

impl TargetMode {
    fn from_value(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "both" => Ok(Self::Both),
            "foxkv" | "foxkv-only" | "foxkv_only" => Ok(Self::FoxkvOnly),
            other => Err(format!(
                "invalid target mode '{other}', expected one of: both, foxkv"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::FoxkvOnly => "foxkv",
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let options = parse_cli_options()?;
    let csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("orders_1m.csv");
    let redis_addr = std::env::var("COMPARE_REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    let mycache_addr =
        std::env::var("COMPARE_FOXKV_ADDR").unwrap_or_else(|_| "127.0.0.1:6380".to_string());
    let thread_count = std::env::var("COMPARE_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or_else(default_thread_count);

    println!("csv path: {}", csv_path.display());
    println!("redis addr: {}", redis_addr);
    println!("foxkv addr: {}", mycache_addr);
    println!("thread count: {}", thread_count);
    println!("target mode: {}", options.target_mode.as_str());
    println!("pipeline depth: {}", options.pipeline_depth);

    let load_start = Instant::now();
    let rows = Arc::new(load_csv_rows(&csv_path)?);
    let load_elapsed = load_start.elapsed();
    println!("loaded rows: {}", rows.len());
    println!("csv load elapsed: {:?}", load_elapsed);

    if matches!(options.target_mode, TargetMode::Both) {
        let redis_elapsed = run_target(
            "redis",
            &redis_addr,
            Arc::clone(&rows),
            thread_count,
            options.pipeline_depth,
        )?;
        print_summary("redis", rows.len(), thread_count, redis_elapsed);
    } else {
        println!("redis write skipped");
    }
    let foxkv_elapsed = run_target(
        "foxkv",
        &mycache_addr,
        Arc::clone(&rows),
        thread_count,
        options.pipeline_depth,
    )?;
    print_summary("foxkv", rows.len(), thread_count, foxkv_elapsed);

    Ok(())
}

fn parse_cli_options() -> Result<CliOptions, Box<dyn Error>> {
    let mut mode_value = std::env::var("COMPARE_TARGET").unwrap_or_else(|_| "both".to_string());
    let mut pipeline_depth = std::env::var("COMPARE_PIPELINE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--target" => {
                mode_value = args
                    .next()
                    .ok_or("--target requires a value, expected: both or foxkv")?;
            }
            "--pipeline" => {
                let value = args
                    .next()
                    .ok_or("--pipeline requires a value, expected a positive integer")?;
                pipeline_depth = value
                    .parse::<usize>()
                    .ok()
                    .filter(|v| *v > 0)
                    .ok_or("--pipeline must be a positive integer")?;
            }
            "-h" | "--help" => {
                println!(
                    "Usage: cargo run --release --bin compare_write -- [--target <both|foxkv>] [--pipeline <N>]"
                );
                std::process::exit(0);
            }
            _ => {
                return Err(format!(
                    "unknown argument '{arg}', supported: --target <both|foxkv>, --pipeline <N>"
                )
                .into());
            }
        }
    }

    let target_mode = TargetMode::from_value(&mode_value).map_err(|e| -> Box<dyn Error> { e.into() })?;
    Ok(CliOptions {
        target_mode,
        pipeline_depth,
    })
}

fn default_thread_count() -> usize {
    std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1)
        .max(1)
        .saturating_mul(2)
}

fn load_csv_rows(path: &PathBuf) -> Result<Rows, Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new().has_headers(true).from_path(path)?;
    let mut rows = Vec::new();

    for result in reader.records() {
        let record = result?;
        if record.is_empty() {
            continue;
        }

        let key = record
            .get(0)
            .ok_or("missing key column")?
            .as_bytes()
            .to_vec();

        let mut value = Vec::new();
        for (idx, field) in record.iter().enumerate().skip(1) {
            if idx > 1 {
                value.push(b',');
            }
            value.extend_from_slice(field.as_bytes());
        }
        rows.push((key, value));
    }

    Ok(rows)
}

fn run_target(
    name: &str,
    addr: &str,
    rows: Arc<Rows>,
    thread_count: usize,
    pipeline_depth: usize,
) -> Result<Duration, Box<dyn Error>> {
    let chunk_size = rows.len().div_ceil(thread_count.max(1));
    let start = Instant::now();

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for worker_id in 0..thread_count {
            let begin = worker_id * chunk_size;
            if begin >= rows.len() {
                continue;
            }
            let end = (begin + chunk_size).min(rows.len());
            let addr = addr.to_string();
            let rows_ref = Arc::clone(&rows);
            let target_name = name.to_string();

            handles.push(scope.spawn(move || -> Result<(), String> {
                let mut stream = TcpStream::connect(&addr)
                    .map_err(|e| format!("{target_name} connect {addr} failed: {e}"))?;
                stream
                    .set_nodelay(true)
                    .map_err(|e| format!("{target_name} set_nodelay failed: {e}"))?;
                let reader_stream = stream
                    .try_clone()
                    .map_err(|e| format!("{target_name} clone stream failed: {e}"))?;
                let mut reader = BufReader::new(reader_stream);
                let mut line = Vec::with_capacity(64);
                let mut write_buf = Vec::with_capacity(8 * 1024);

                let mut cursor = begin;
                while cursor < end {
                    let batch_end = (cursor + pipeline_depth).min(end);
                    write_buf.clear();
                    for idx in cursor..batch_end {
                        let (key, value) = &rows_ref[idx];
                        let cmd = encode_set_command(key, value);
                        write_buf.extend_from_slice(&cmd);
                    }
                    stream
                        .write_all(&write_buf)
                        .map_err(|e| format!("{target_name} write failed: {e}"))?;

                    let response_count = batch_end - cursor;
                    for _ in 0..response_count {
                        line.clear();
                        reader
                            .read_until(b'\n', &mut line)
                            .map_err(|e| format!("{target_name} read failed: {e}"))?;
                        if line.as_slice() != b"+OK\r\n" {
                            let got = String::from_utf8_lossy(&line);
                            return Err(format!(
                                "{target_name} unexpected response for SET: {}",
                                got.trim_end()
                            ));
                        }
                    }
                    cursor = batch_end;
                }
                Ok(())
            }));
        }

        for h in handles {
            h.join().map_err(|_| format!("{name} worker panicked"))??;
        }

        Ok::<(), String>(())
    })
    .map_err(|e| -> Box<dyn Error> { e.into() })?;

    Ok(start.elapsed())
}

fn encode_set_command(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + key.len() + value.len());
    out.extend_from_slice(b"*3\r\n");
    out.extend_from_slice(b"$3\r\nSET\r\n");
    out.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
    out
}

fn print_summary(name: &str, total_rows: usize, thread_count: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let throughput = if secs > 0.0 {
        total_rows as f64 / secs
    } else {
        0.0
    };
    println!(
        "{} write elapsed: {:?}, rows: {}, threads: {}, throughput: {:.0} ops/s",
        name, elapsed, total_rows, thread_count, throughput
    );
}
