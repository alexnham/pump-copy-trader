//! Controlled logger comparison. No keys, database, RPC endpoint, or live transactions.
use clap::Parser;
use std::{
    io::{self, Write},
    time::{Duration, Instant},
};
use tracing::instrument::WithSubscriber;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u32).range(1..))]
    samples: u32,
    #[arg(long, default_value_t = 1000)]
    sink_delay_us: u64,
    #[arg(long, default_value_t = 2)]
    rpc_delay_ms: u64,
    #[arg(long, default_value_t = 8192, value_parser = clap::value_parser!(u32).range(1..))]
    capacity: u32,
}

#[derive(Clone)]
struct DelayedSink(Duration);
impl Write for DelayedSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.0.is_zero() {
            std::thread::sleep(self.0);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn replay(args: &Args) -> Vec<u128> {
    let mut samples = Vec::with_capacity(args.samples as usize);
    for attempt in 0..args.samples {
        let received = Instant::now();
        // Identical preparation and route RPC delays in both runs.
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(args.rpc_delay_ms)).await;
        }
        tracing::info!(target: "pump_copy_trader", attempt, "route selected");
        samples.push(received.elapsed().as_micros()); // About to submit; no send occurs.
        tracing::info!(target: "pump_copy_trader", attempt, "copy timings");
    }
    samples
}

fn dispatch<W>(writer: W) -> tracing::Dispatch
where
    W: for<'a> fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(EnvFilter::new("warn,pump_copy_trader=info"))
            .with(
                fmt::layer()
                    .compact()
                    .with_ansi(false)
                    .with_target(false)
                    .with_file(false)
                    .with_line_number(false)
                    .with_thread_ids(false)
                    .with_thread_names(false)
                    .with_writer(writer),
            ),
    )
}

fn report(name: &str, mut samples: Vec<u128>, dropped: usize) {
    samples.sort_unstable();
    let middle = samples.len() / 2;
    let median = if samples.len().is_multiple_of(2) {
        samples[middle - 1] + (samples[middle] - samples[middle - 1]) / 2
    } else {
        samples[middle]
    };
    let p95 = samples[(samples.len() * 95).div_ceil(100) - 1];
    println!("{name:12} median={median:8} us  p95={p95:8} us  dropped_lines={dropped}");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
    let sink = DelayedSink(Duration::from_micros(args.sink_delay_us));
    // Shared mutex matches stderr's serialized writes while retaining the controlled sink delay.
    let synchronous = dispatch(std::sync::Mutex::new(sink.clone()));
    let sync_samples = replay(&args).with_subscriber(synchronous).await;
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(args.capacity as usize)
        .lossy(true)
        .finish(sink);
    let drops = writer.error_counter();
    let background = dispatch(writer);
    let background_samples = replay(&args).with_subscriber(background).await;
    drop(guard);
    println!(
        "Synthetic replay: {} attempts, three {}ms mock RPC waits, {}us sink delay/write",
        args.samples, args.rpc_delay_ms, args.sink_delay_us
    );
    report("synchronous", sync_samples, 0);
    report("background", background_samples, drops.dropped_lines());
    println!("These are modeled receipt-to-send-start timings, not measured mainnet gains.");
    println!("A faster result with dropped lines trades log completeness for latency.");
}
