//! Daemon configuration lifecycle, independent of socket serving.

use meta_signal_herdr_nexus::{Configuration, ConfigurationError, ConfigureOutcome};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Serves one framed meta request. The caller supplies the startup capacity;
/// no runtime default is hidden here.
pub fn serve_meta_once<S>(
    stream: &mut S,
    store: &ConfigurationStore,
    capacity: signal::FrameCapacity,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: std::io::Read + std::io::Write,
{
    use signal::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
    let frame = stream.read_frame(capacity)?;
    let query = Signal::<meta_signal_herdr_nexus::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = match query {
        meta_signal_herdr_nexus::Query::Configure(configuration) => {
            meta_signal_herdr_nexus::Response::Configured(store.configure(configuration))
        }
    };
    stream.write_frame(&response.signalize()?, capacity)?;
    Ok(())
}

/// Serves one framed public request through a typed handler. Boot code exposes
/// the public socket only after configuration and captures its snapshot before
/// constructing the handler.
pub fn serve_public_once<S, H>(
    stream: &mut S,
    capacity: signal::FrameCapacity,
    handle: H,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: std::io::Read + std::io::Write,
    H: FnOnce(signal_herdr_nexus::Query) -> signal_herdr_nexus::Response,
{
    use signal::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
    let frame = stream.read_frame(capacity)?;
    let query = Signal::<signal_herdr_nexus::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = handle(query);
    stream.write_frame(&response.signalize()?, capacity)?;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfiguration {
    pub source: Configuration,
    pub polling: Duration,
    pub call_timeout: Duration,
    pub start_timeout: Duration,
    pub frame_capacity_bytes: usize,
}

#[derive(Default)]
pub struct ConfigurationStore {
    current: RwLock<Option<Arc<RuntimeConfiguration>>>,
}

impl ConfigurationStore {
    pub fn configure(&self, candidate: Configuration) -> ConfigureOutcome {
        match validate(candidate) {
            Ok(validated) => {
                let mut current = self
                    .current
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *current = Some(Arc::new(validated));
                ConfigureOutcome::Succeeded
            }
            Err(errors) => ConfigureOutcome::Invalid(errors),
        }
    }

    /// Captures the one immutable configuration an in-flight attempt uses.
    pub fn snapshot(&self) -> Option<Arc<RuntimeConfiguration>> {
        self.current
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

fn validate(candidate: Configuration) -> Result<RuntimeConfiguration, Vec<ConfigurationError>> {
    let mut errors = Vec::new();
    if candidate.herdr_executable.0.is_empty() {
        errors.push(ConfigurationError::EmptyExecutable);
    }
    let polling = checked_duration(candidate.polling_milliseconds.0).or_else(|| {
        errors.push(ConfigurationError::PollingOutOfRange(
            candidate.polling_milliseconds.clone(),
        ));
        None
    });
    let call_timeout = checked_duration(candidate.call_timeout_milliseconds.0).or_else(|| {
        errors.push(ConfigurationError::CallTimeoutOutOfRange(
            candidate.call_timeout_milliseconds.clone(),
        ));
        None
    });
    let start_timeout = checked_duration(candidate.start_timeout_milliseconds.0).or_else(|| {
        errors.push(ConfigurationError::StartTimeoutOutOfRange(
            candidate.start_timeout_milliseconds.clone(),
        ));
        None
    });
    let frame_capacity_bytes = usize::try_from(candidate.frame_capacity_bytes.0)
        .ok()
        .filter(|capacity| (1..=signal::MAXIMUM_SIGNAL_BYTES).contains(capacity))
        .or_else(|| {
            errors.push(ConfigurationError::FrameCapacityOutOfRange(
                candidate.frame_capacity_bytes.clone(),
            ));
            None
        });
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(RuntimeConfiguration {
        source: candidate,
        polling: polling.expect("validated polling duration"),
        call_timeout: call_timeout.expect("validated call timeout"),
        start_timeout: start_timeout.expect("validated start timeout"),
        frame_capacity_bytes: frame_capacity_bytes.expect("validated frame capacity"),
    })
}

fn checked_duration(milliseconds: i64) -> Option<Duration> {
    let duration = Duration::from_millis(u64::try_from(milliseconds).ok()?);
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        None
    } else {
        Some(duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meta_signal_herdr_nexus::{
        CallTimeoutMilliseconds, FrameCapacityBytes, HerdrExecutable, PollingMilliseconds,
        StartTimeoutMilliseconds,
    };
    use signal::{
        ByteViewable, FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable,
    };
    use std::io::{Cursor, Read, Write};

    struct Duplex {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for Duplex {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buffer)
        }
    }

    impl Write for Duplex {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.output.write(buffer)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn configuration(polling: i64) -> Configuration {
        Configuration {
            herdr_executable: HerdrExecutable("/run/current-system/sw/bin/herdr".into()),
            polling_milliseconds: PollingMilliseconds(polling),
            call_timeout_milliseconds: CallTimeoutMilliseconds(30_000),
            start_timeout_milliseconds: StartTimeoutMilliseconds(60_000),
            frame_capacity_bytes: FrameCapacityBytes(signal::MAXIMUM_SIGNAL_BYTES as i64),
        }
    }

    #[test]
    fn valid_candidate_becomes_one_snapshot() {
        let store = ConfigurationStore::default();
        assert_eq!(
            store.configure(configuration(750)),
            ConfigureOutcome::Succeeded
        );
        assert_eq!(
            store.snapshot().expect("configured").polling,
            Duration::from_millis(750)
        );
    }

    #[test]
    fn invalid_candidate_leaves_previous_configuration_whole() {
        let store = ConfigurationStore::default();
        assert_eq!(
            store.configure(configuration(750)),
            ConfigureOutcome::Succeeded
        );
        let before = store.snapshot().expect("configured");
        let invalid = store.configure(configuration(0));
        assert!(matches!(invalid, ConfigureOutcome::Invalid(_)));
        let after = store.snapshot().expect("still configured");
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn an_attempt_keeps_its_captured_snapshot_after_reconfiguration() {
        let store = ConfigurationStore::default();
        store.configure(configuration(750));
        let in_flight = store.snapshot().expect("first configuration");
        store.configure(configuration(1_500));
        assert_eq!(in_flight.polling, Duration::from_millis(750));
        assert_eq!(
            store.snapshot().expect("second configuration").polling,
            Duration::from_millis(1_500)
        );
    }

    #[test]
    fn framed_meta_request_validates_and_atomically_configures() {
        let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
        let query = meta_signal_herdr_nexus::Query::Configure(configuration(750));
        let query_signal = query.signalize().expect("archive query");
        let mut input = Vec::new();
        input
            .write_frame(&query_signal, capacity)
            .expect("frame query");
        let mut duplex = Duplex {
            input: Cursor::new(input),
            output: Vec::new(),
        };
        let store = ConfigurationStore::default();

        serve_meta_once(&mut duplex, &store, capacity).expect("serve meta request");

        let body = Cursor::new(duplex.output)
            .read_frame(capacity)
            .expect("read response");
        let response = Signal::<meta_signal_herdr_nexus::Response>::from(body.bytes().to_vec())
            .restore()
            .expect("restore response");
        assert_eq!(
            response,
            meta_signal_herdr_nexus::Response::Configured(ConfigureOutcome::Succeeded)
        );
        assert!(store.snapshot().is_some());
    }
}
