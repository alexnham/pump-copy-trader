use std::io::{self, IsTerminal};

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_FILTER: &str = "warn,pump_copy_trader=info";

pub fn init() -> tracing_appender::non_blocking::WorkerGuard {
    // Dependency-level INFO logs drown out the copy pipeline, so the default only
    // enables INFO for this crate. RUST_LOG remains the escape hatch for deeper work.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let ansi = io::stderr().is_terminal();
    let (writer, guard) = background_writer(io::stderr());

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .compact()
                .with_ansi(ansi)
                .with_target(false)
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_writer(writer),
        )
        .init();
    guard
}

fn background_writer(
    writer: impl io::Write + Send + 'static,
) -> (
    tracing_appender::non_blocking::NonBlocking,
    tracing_appender::non_blocking::WorkerGuard,
) {
    tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .thread_name("copy-trader-logs")
        .finish(writer)
}

/// Keeps signatures and public keys recognizable without letting them dominate a log line.
pub fn compact_id(value: impl AsRef<str>) -> String {
    let value = value.as_ref();
    if value.chars().count() <= 20 {
        return value.to_owned();
    }

    let start: String = value.chars().take(8).collect();
    let end: String = value
        .chars()
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{start}…{end}")
}

#[cfg(test)]
mod tests {
    use super::compact_id;

    #[test]
    fn blocked_output_and_full_queue_do_not_block_log_producers() {
        use std::{io::Write, sync::mpsc, time::Duration};
        struct BlockedOutput {
            entered: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            blocked: bool,
        }
        impl Write for BlockedOutput {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if !self.blocked {
                    self.blocked = true;
                    self.entered.send(()).expect("entered");
                    self.release.recv().expect("release");
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (entered, ready) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let (mut writer, guard) = super::background_writer(BlockedOutput {
            entered,
            release: blocked,
            blocked: false,
        });
        writer.write_all(b"first\n").expect("first");
        ready
            .recv_timeout(Duration::from_secs(2))
            .expect("worker blocked");
        let errors = writer.error_counter();
        let (finished, done) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            for _ in 0..5000 {
                writer.write_all(b"queued\n").expect("enqueue");
            }
            finished.send(()).expect("finished");
        });
        let result = done.recv_timeout(Duration::from_secs(2));
        release.send(()).expect("release output");
        producer.join().expect("producer");
        assert!(
            result.is_ok(),
            "log producer must finish while output is blocked"
        );
        assert!(errors.dropped_lines() > 0);
        drop(guard);
    }

    #[test]
    fn compact_id_preserves_short_values() {
        assert_eq!(compact_id("mainnet"), "mainnet");
    }

    #[test]
    fn compact_id_shortens_long_values_at_character_boundaries() {
        assert_eq!(compact_id("1234567890abcdefghijXYZ"), "12345678…fghijXYZ");
    }
}
