use std::io::{self, IsTerminal};

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_FILTER: &str = "warn,pump_copy_trader=info";

pub fn init() {
    // Dependency-level INFO logs drown out the copy pipeline, so the default only
    // enables INFO for this crate. RUST_LOG remains the escape hatch for deeper work.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let ansi = io::stderr().is_terminal();

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
                .with_writer(io::stderr),
        )
        .init();
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
    fn compact_id_preserves_short_values() {
        assert_eq!(compact_id("mainnet"), "mainnet");
    }

    #[test]
    fn compact_id_shortens_long_values_at_character_boundaries() {
        assert_eq!(compact_id("1234567890abcdefghijXYZ"), "12345678…fghijXYZ");
    }
}
