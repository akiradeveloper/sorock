use anyhow::Result;

const DURATION: Duration = Duration::from_secs(5);
const IOSIZES: [usize; 3] = [10, 100, 1000]; // X-axis
const THREADS: [u32; 4] = [1, 10, 100, 1000]; // Y-axis

#[derive(Default, Clone, Copy)]
struct BenchResult {
    throughput: f64,
    latency: Duration,
}

fn draw_result(tbl: [[BenchResult; 4]; 3]) {
    let mut builder = tabled::builder::Builder::default();

    for i in 0..3 {
        let mut arr = vec![];
        for j in 0..4 {
            let cell = format!(
                "{:.2} MB/s, {:.2} us/op",
                tbl[i][j].throughput,
                tbl[i][j].latency.as_micros()
            );
            arr.push(cell);
        }
        builder.push_record(arr);
    }

    builder.insert_record(0, vec!["1", "10", "100", "1000"]);
    builder.insert_column(0, vec!["", "10 B", "100 B", "1000 B"]);

    let table = builder.build();
    eprintln!("{}", table);
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut bench_result_tbl = [[BenchResult::default(); 4]; 3];

    for (i, &iosize) in IOSIZES.iter().enumerate() {
        for (j, &n_thread) in THREADS.iter().enumerate() {
            eprintln!(
                "Running ({}sec): iosize={} bytes, n_thread={}",
                DURATION.as_secs(),
                iosize,
                n_thread
            );
            bench_result_tbl[i][j] = run_bench(iosize, n_thread).await;
        }
    }

    draw_result(bench_result_tbl);

    Ok(())
}

use sorock::log_storage::LogStorage;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Total {
    count: u32,
    iotime: Duration,
}

async fn run_bench(iosize: usize, n_threads: u32) -> BenchResult {
    // let mem = redb::backends::InMemoryBackend::new();
    // let db = redb::Database::builder().create_with_backend(mem).unwrap();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let db = redb::Database::create(tmp.path()).unwrap();
    let log_storage = LogStorage::new(Arc::new(db));

    let total = Arc::new(spin::Mutex::new(Total {
        count: 0,
        iotime: Duration::ZERO,
    }));

    let mut handles = vec![];
    for i in 0..n_threads {
        let shard = log_storage.get_shard(i).unwrap();
        let total = total.clone();
        let hdl = tokio::spawn(async move {
            let mut index = 0;
            let time = Instant::now();
            while time.elapsed() < DURATION {
                let data = randbytes(iosize);

                let io_start = Instant::now();
                shard.insert_entry(index, data).await.unwrap();
                let io_time = io_start.elapsed();

                let mut total = total.lock();
                total.count += 1;
                total.iotime += io_time;

                index += 1;
            }
        });
        handles.push(hdl);
    }
    futures::future::join_all(handles).await;

    let total = total.lock();
    BenchResult {
        throughput: (total.count as f64 * iosize as f64) / (1024.0 * 1024.0) / 5.0,
        latency: total.iotime / total.count,
    }
}

fn randbytes(n: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::rng();
    let mut out = vec![0; n];
    rng.fill(&mut out[..]);
    out
}
