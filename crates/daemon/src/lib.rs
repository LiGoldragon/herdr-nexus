//! Daemon configuration lifecycle, independent of socket serving.

use meta_signal_herdr_nexus::{
    BootstrapRefusal, Configuration, ConfigurationError, ConfigureOutcome, Message, SocketPath,
};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Applies one absolute deadline across all reads and writes performed by a
/// framed attempt. Recomputing the socket timeout before every syscall keeps a
/// peer that drip-feeds a partial frame from extending the attempt forever.
pub struct DeadlineStream {
    stream: std::os::unix::net::UnixStream,
    deadline: Instant,
}

impl DeadlineStream {
    pub fn new(stream: std::os::unix::net::UnixStream, timeout: Duration) -> std::io::Result<Self> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "deadline overflow")
        })?;
        Ok(Self { stream, deadline })
    }

    fn remaining(&self) -> std::io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "attempt deadline elapsed")
            })
    }
}

impl std::io::Read for DeadlineStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.stream.set_read_timeout(Some(self.remaining()?))?;
        std::io::Read::read(&mut self.stream, buffer)
    }
}

impl std::io::Write for DeadlineStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        std::io::Write::write(&mut self.stream, buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        std::io::Write::flush(&mut self.stream)
    }
}

pub const PUBLIC_SOCKET_NAME: &str = "herdr.sock";
pub const META_SOCKET_NAME: &str = "herdr-meta.sock";
pub const TRANSLATOR_SOCKET_NAME: &str = "herdr-translator.sock";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketPaths {
    pub public: std::path::PathBuf,
    pub meta: std::path::PathBuf,
    pub translator: std::path::PathBuf,
}

impl From<&shared_translator::placement::Place> for SocketPaths {
    fn from(place: &shared_translator::placement::Place) -> Self {
        let directory = std::path::PathBuf::from(&place.directory.0);
        Self {
            public: directory.join(PUBLIC_SOCKET_NAME),
            meta: directory.join(META_SOCKET_NAME),
            translator: directory.join(TRANSLATOR_SOCKET_NAME),
        }
    }
}

struct OwnedListener {
    listener: std::os::unix::net::UnixListener,
    path: std::path::PathBuf,
    device: u64,
    inode: u64,
}

impl OwnedListener {
    fn bind(path: std::path::PathBuf) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let listener = std::os::unix::net::UnixListener::bind(&path)?;
        let metadata = std::fs::metadata(&path)?;
        Ok(Self {
            listener,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for OwnedListener {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        let still_ours = std::fs::metadata(&self.path)
            .map(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
            .unwrap_or(false);
        if still_ours {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub struct Bootstrap {
    paths: SocketPaths,
    store: ConfigurationStore,
    meta: OwnedListener,
    public: std::sync::Mutex<Option<OwnedListener>>,
    public_ready: std::sync::Condvar,
}

impl Bootstrap {
    pub fn bind(place: &shared_translator::placement::Place) -> std::io::Result<Self> {
        let paths = SocketPaths::from(place);
        let meta = OwnedListener::bind(paths.meta.clone())?;
        Ok(Self {
            paths,
            store: ConfigurationStore::default(),
            meta,
            public: std::sync::Mutex::new(None),
            public_ready: std::sync::Condvar::new(),
        })
    }

    pub fn configure(&self, candidate: Configuration) -> ConfigureOutcome {
        self.store.configure(candidate, || {
            let listener = OwnedListener::bind(self.paths.public.clone()).map_err(|error| {
                BootstrapRefusal {
                    socket_path: SocketPath(self.paths.public.display().to_string()),
                    message: Message(error.to_string()),
                }
            })?;
            let mut public = self
                .public
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *public = Some(listener);
            self.public_ready.notify_all();
            Ok(())
        })
    }

    pub fn snapshot(&self) -> Option<Arc<RuntimeConfiguration>> {
        self.store.snapshot()
    }

    pub fn serve_next_meta(
        &self,
        capacity: signal::FrameCapacity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut stream, _) = self.meta.listener.accept()?;
        serve_meta_once(&mut stream, capacity, |candidate| self.configure(candidate))
    }

    pub fn accept_public(&self) -> std::io::Result<std::os::unix::net::UnixStream> {
        let mut public = self
            .public
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while public.is_none() {
            public = self
                .public_ready
                .wait(public)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        public
            .as_ref()
            .expect("public listener notified only after binding")
            .listener
            .accept()
            .map(|(stream, _)| stream)
    }

    pub fn paths(&self) -> &SocketPaths {
        &self.paths
    }
}

/// Serves one framed meta request. The caller supplies the startup capacity;
/// no runtime default is hidden here.
pub fn serve_meta_once<S>(
    stream: &mut S,
    capacity: signal::FrameCapacity,
    configure: impl FnOnce(Configuration) -> ConfigureOutcome,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: std::io::Read + std::io::Write,
{
    use signal::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
    let frame = stream.read_frame(capacity)?;
    let query = Signal::<meta_signal_herdr_nexus::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = match query {
        meta_signal_herdr_nexus::Query::Configure(configuration) => {
            meta_signal_herdr_nexus::Response::Configured(configure(configuration))
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

pub struct Dispatcher {
    core: std::sync::Mutex<herdr_nexus_core::Core<herdr_nexus_core::InMemory>>,
    translator_socket: std::path::PathBuf,
}

impl Dispatcher {
    pub fn new(paths: &SocketPaths) -> Self {
        Self {
            core: std::sync::Mutex::new(herdr_nexus_core::Core::new(
                herdr_nexus_core::InMemory::default(),
            )),
            translator_socket: paths.translator.clone(),
        }
    }

    pub fn dispatch(
        &self,
        query: signal_herdr_nexus::Query,
        configuration: Arc<RuntimeConfiguration>,
    ) -> signal_herdr_nexus::Response {
        use emission_herdr_nexus::{
            AgentPrompt_Data, AgentStart_Data, Herdr, Keys_Data, Verb, WorkspaceCreate_Data,
        };
        use signal_herdr_nexus::{Query, Response};
        let mut core = self
            .core
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match query {
            Query::Create(workspace) => {
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: workspace.session.clone(),
                        verb: Verb::WorkspaceCreate(WorkspaceCreate_Data {
                            root: workspace.root.clone(),
                            variable_vector: workspace.variable_vector.clone(),
                        }),
                    },
                );
                Response::Created(core.create(workspace, created_result(external)))
            }
            Query::Start(start) => {
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: start.pane.session.clone(),
                        verb: Verb::AgentStart(AgentStart_Data {
                            pane_id: start.pane.pane_id.clone(),
                            command: start.command,
                        }),
                    },
                );
                Response::Started(core.start(start.pane, start.label, done_result(external)))
            }
            Query::Prompt(prompt) => {
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: prompt.pane.session.clone(),
                        verb: Verb::AgentPrompt(AgentPrompt_Data {
                            pane_id: prompt.pane.pane_id.clone(),
                            text: prompt.text,
                        }),
                    },
                );
                Response::Prompted(core.prompt(prompt.pane, done_result(external)))
            }
            Query::Press(press) => {
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: press.pane.session.clone(),
                        verb: Verb::Keys(Keys_Data {
                            pane_id: press.pane.pane_id.clone(),
                            keys: press.keys,
                        }),
                    },
                );
                Response::Pressed(core.press(press.pane, done_result(external)))
            }
            Query::Close(pane) => {
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: pane.session.clone(),
                        verb: Verb::PaneClose(pane.pane_id.clone()),
                    },
                );
                Response::Closed(core.close(pane, done_result(external)))
            }
            Query::List(session) => {
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: session.clone(),
                        verb: Verb::PaneList,
                    },
                );
                Response::Listed(core.list(session, listed_result(external)))
            }
            Query::Subscribe(session) => Response::Subscribed(
                core.subscribe(session, herdr_nexus_core::ExternalResult::Uncertain),
            ),
        }
    }

    /// A subscription is an acknowledged, nondurable stream. A complete List
    /// establishes and advances its state; uncertain/refused polls leave the
    /// previous state intact. Delivery is direct and serialized on this stream,
    /// so a failed or slow subscriber is removed simply by ending the call.
    pub fn subscribe(
        &self,
        stream: std::os::unix::net::UnixStream,
        session: signal_herdr_nexus::Session,
        configuration: Arc<RuntimeConfiguration>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use signal::{FrameWriting, Signalizable};
        use signal_herdr_nexus::{Changed_Data, Outcome, Query, Response};
        let capacity = signal::FrameCapacity::from(configuration.frame_capacity_bytes);
        let acknowledgement = {
            let mut core = self.core.lock().unwrap_or_else(|p| p.into_inner());
            Response::Subscribed(core.subscribe(
                session.clone(),
                herdr_nexus_core::ExternalResult::Succeeded(()),
            ))
        };
        let mut output = DeadlineStream::new(stream.try_clone()?, configuration.call_timeout)?;
        output.write_frame(&acknowledgement.signalize()?, capacity)?;

        let mut previous = std::collections::HashMap::new();
        loop {
            std::thread::sleep(configuration.polling);
            let response = self.dispatch(Query::List(session.clone()), Arc::clone(&configuration));
            let Response::Listed(Outcome::Listed(known)) = response else {
                continue;
            };
            let current: std::collections::HashMap<_, _> = known
                .into_iter()
                .map(|known| (known.pane, known.state))
                .collect();
            if !previous.is_empty() {
                for (pane, state) in &current {
                    if previous.get(pane) != Some(state) {
                        let changed = Response::Changed(Changed_Data {
                            pane: pane.clone(),
                            state: state.clone(),
                        });
                        output =
                            DeadlineStream::new(stream.try_clone()?, configuration.call_timeout)?;
                        output.write_frame(&changed.signalize()?, capacity)?;
                    }
                }
            }
            previous = current;
        }
    }

    fn execute(
        &self,
        configuration: &RuntimeConfiguration,
        herdr: emission_herdr_nexus::Herdr,
    ) -> emission_herdr_nexus::ExternalOutcome {
        use signal::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
        let result = (|| -> Result<_, Box<dyn std::error::Error>> {
            let stream = std::os::unix::net::UnixStream::connect(&self.translator_socket)?;
            let timeout = match &herdr.verb {
                emission_herdr_nexus::Verb::AgentStart(_) => configuration.start_timeout,
                _ => configuration.call_timeout,
            };
            let mut stream = DeadlineStream::new(stream, timeout)?;
            let request =
                emission_herdr_nexus::Query::Execute(emission_herdr_nexus::Execute_Data {
                    configuration: configuration.source.clone(),
                    herdr,
                });
            let capacity = signal::FrameCapacity::from(configuration.frame_capacity_bytes);
            stream.write_frame(&request.signalize()?, capacity)?;
            let body = stream.read_frame(capacity)?;
            let response =
                Signal::<emission_herdr_nexus::Response>::from(Vec::<u8>::from(body)).restore()?;
            match response {
                emission_herdr_nexus::Response::Executed(outcome) => Ok(outcome),
            }
        })();
        result.unwrap_or(emission_herdr_nexus::ExternalOutcome::Uncertain)
    }
}

fn done_result(
    outcome: emission_herdr_nexus::ExternalOutcome,
) -> herdr_nexus_core::ExternalResult<()> {
    match outcome {
        emission_herdr_nexus::ExternalOutcome::Done => {
            herdr_nexus_core::ExternalResult::Succeeded(())
        }
        emission_herdr_nexus::ExternalOutcome::Refused(refusal) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        _ => herdr_nexus_core::ExternalResult::Uncertain,
    }
}

fn created_result(
    outcome: emission_herdr_nexus::ExternalOutcome,
) -> herdr_nexus_core::ExternalResult<signal_herdr_nexus::Pane> {
    match outcome {
        emission_herdr_nexus::ExternalOutcome::Created(pane) => {
            herdr_nexus_core::ExternalResult::Succeeded(pane)
        }
        emission_herdr_nexus::ExternalOutcome::Refused(refusal) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        _ => herdr_nexus_core::ExternalResult::Uncertain,
    }
}

fn listed_result(
    outcome: emission_herdr_nexus::ExternalOutcome,
) -> herdr_nexus_core::ExternalResult<Vec<emission_herdr_nexus::PaneObservation>> {
    match outcome {
        emission_herdr_nexus::ExternalOutcome::Listed(observed) => {
            herdr_nexus_core::ExternalResult::Succeeded(observed)
        }
        emission_herdr_nexus::ExternalOutcome::Refused(refusal) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        _ => herdr_nexus_core::ExternalResult::Uncertain,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfiguration {
    pub source: Configuration,
    pub polling: Duration,
    pub call_timeout: Duration,
    pub start_timeout: Duration,
    pub frame_capacity_bytes: usize,
    pub external_output_limit_bytes: usize,
}

#[derive(Default)]
pub struct ConfigurationStore {
    current: RwLock<Option<Arc<RuntimeConfiguration>>>,
}

impl ConfigurationStore {
    pub fn configure(
        &self,
        candidate: Configuration,
        bind_public: impl FnOnce() -> Result<(), BootstrapRefusal>,
    ) -> ConfigureOutcome {
        match validate(candidate) {
            Ok(validated) => {
                let mut current = self
                    .current
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if current.is_none() {
                    if let Err(refusal) = bind_public() {
                        return ConfigureOutcome::Unavailable(refusal);
                    }
                }
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
    let external_output_limit_bytes = usize::try_from(candidate.external_output_limit_bytes.0)
        .ok()
        .filter(|limit| *limit > 0)
        .or_else(|| {
            errors.push(ConfigurationError::ExternalOutputLimitOutOfRange(
                candidate.external_output_limit_bytes.clone(),
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
        external_output_limit_bytes: external_output_limit_bytes
            .expect("validated external output limit"),
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
        CallTimeoutMilliseconds, ExternalOutputLimitBytes, FrameCapacityBytes, HerdrExecutable,
        PollingMilliseconds, StartTimeoutMilliseconds,
    };
    use signal::{
        ByteViewable, FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable,
    };
    use std::io::{Cursor, Read, Write};

    fn test_place(name: &str) -> (shared_translator::placement::Place, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "herdr-nexus-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir(&directory).expect("create owned test directory");
        (
            shared_translator::placement::Place {
                metaflow_name: shared_translator::placement::MetaflowName(name.into()),
                directory: shared_translator::placement::Directory(directory.display().to_string()),
            },
            directory,
        )
    }

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
            external_output_limit_bytes: ExternalOutputLimitBytes(1024 * 1024),
        }
    }

    #[test]
    fn absolute_deadline_stops_a_drip_fed_partial_read() {
        let (reader, mut writer) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let sender = std::thread::spawn(move || {
            for byte in [1_u8, 2, 3, 4] {
                if writer.write_all(&[byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(30));
            }
        });
        let mut reader =
            DeadlineStream::new(reader, Duration::from_millis(50)).expect("representable deadline");
        let mut bytes = [0_u8; 4];
        let error = reader
            .read_exact(&mut bytes)
            .expect_err("drip must not extend the absolute deadline");
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ));
        sender.join().expect("sender");
    }

    #[test]
    fn subscription_acknowledges_then_emits_only_complete_snapshot_changes() {
        use emission_herdr_nexus::{ExternalOutcome, PaneObservation};
        use signal_herdr_nexus::{
            Changed_Data, Label, Outcome, Pane, PaneId, Response, Root, Session, State, Workspace,
        };
        let (place, directory) = test_place("subscription");
        let paths = SocketPaths::from(&place);
        let dispatcher = Arc::new(Dispatcher::new(&paths));
        let pane = Pane {
            session: Session("subscription".into()),
            pane_id: PaneId("w1:p1".into()),
        };
        let other = Pane {
            session: Session("other".into()),
            pane_id: PaneId("w2:p1".into()),
        };
        {
            let mut core = dispatcher.core.lock().expect("core");
            for owned in [&pane, &other] {
                let workspace = Workspace {
                    session: owned.session.clone(),
                    root: Root(directory.display().to_string()),
                    variable_vector: vec![],
                };
                assert!(matches!(
                    core.create(
                        workspace,
                        herdr_nexus_core::ExternalResult::Succeeded(owned.clone())
                    ),
                    Outcome::Created(_)
                ));
                assert_eq!(
                    core.start(
                        owned.clone(),
                        Label(format!("label-{}", owned.session.0)),
                        herdr_nexus_core::ExternalResult::Succeeded(())
                    ),
                    Outcome::Done
                );
            }
        }
        let listener = std::os::unix::net::UnixListener::bind(&paths.translator)
            .expect("bind translator fixture");
        let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
        let target = pane.clone();
        let unrelated = other.clone();
        let fixture = std::thread::spawn(move || {
            let outcomes = [
                ExternalOutcome::Listed(vec![
                    PaneObservation {
                        pane: target.clone(),
                        state: State::Idle,
                    },
                    PaneObservation {
                        pane: unrelated.clone(),
                        state: State::Busy,
                    },
                ]),
                ExternalOutcome::Uncertain,
                ExternalOutcome::Listed(vec![
                    PaneObservation {
                        pane: target.clone(),
                        state: State::Busy,
                    },
                    PaneObservation {
                        pane: unrelated.clone(),
                        state: State::Idle,
                    },
                ]),
                ExternalOutcome::Listed(vec![PaneObservation {
                    pane: target,
                    state: State::Idle,
                }]),
            ];
            for outcome in outcomes {
                let (mut stream, _) = listener.accept().expect("accept list attempt");
                let _ = stream.read_frame(capacity).expect("read list attempt");
                stream
                    .write_frame(
                        &emission_herdr_nexus::Response::Executed(outcome)
                            .signalize()
                            .expect("archive outcome"),
                        capacity,
                    )
                    .expect("write list outcome");
            }
        });
        let configuration = Arc::new(validate(configuration(10)).expect("valid configuration"));
        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("subscriber pair");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("client timeout");
        let subscribed = Arc::clone(&dispatcher);
        let subscription = std::thread::spawn(move || {
            subscribed
                .subscribe(server, Session("subscription".into()), configuration)
                .is_err()
        });
        let read_response = |stream: &mut std::os::unix::net::UnixStream| {
            let body = stream
                .read_frame(capacity)
                .expect("read subscription frame");
            Signal::<Response>::from(Vec::<u8>::from(body))
                .restore()
                .expect("restore subscription frame")
        };
        assert_eq!(
            read_response(&mut client),
            Response::Subscribed(Outcome::Done)
        );
        assert_eq!(
            read_response(&mut client),
            Response::Changed(Changed_Data {
                pane: pane.clone(),
                state: State::Busy,
            })
        );
        drop(client);
        assert!(subscription.join().expect("subscription thread"));
        fixture.join().expect("translator fixture");
        drop(dispatcher);
        std::fs::remove_file(&paths.translator).expect("remove fixture socket");
        std::fs::remove_dir(directory).expect("remove fixture directory");
    }

    fn configure(store: &ConfigurationStore, candidate: Configuration) -> ConfigureOutcome {
        store.configure(candidate, || Ok(()))
    }

    #[test]
    fn valid_candidate_becomes_one_snapshot() {
        let store = ConfigurationStore::default();
        assert_eq!(
            configure(&store, configuration(750)),
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
            configure(&store, configuration(750)),
            ConfigureOutcome::Succeeded
        );
        let before = store.snapshot().expect("configured");
        let invalid = configure(&store, configuration(0));
        assert!(matches!(invalid, ConfigureOutcome::Invalid(_)));
        let after = store.snapshot().expect("still configured");
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn an_attempt_keeps_its_captured_snapshot_after_reconfiguration() {
        let store = ConfigurationStore::default();
        configure(&store, configuration(750));
        let in_flight = store.snapshot().expect("first configuration");
        configure(&store, configuration(1_500));
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

        serve_meta_once(&mut duplex, capacity, |candidate| {
            configure(&store, candidate)
        })
        .expect("serve meta request");

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

    #[test]
    fn public_socket_appears_only_after_first_valid_configuration() {
        let (place, directory) = test_place("gated");
        let bootstrap = Bootstrap::bind(&place).expect("bind meta socket");
        assert!(bootstrap.paths().meta.exists());
        assert!(!bootstrap.paths().public.exists());

        assert_eq!(
            bootstrap.configure(configuration(750)),
            ConfigureOutcome::Succeeded
        );
        assert!(bootstrap.paths().public.exists());
        drop(bootstrap);
        assert!(!directory.join(META_SOCKET_NAME).exists());
        assert!(!directory.join(PUBLIC_SOCKET_NAME).exists());
        std::fs::remove_dir(directory).expect("remove owned test directory");
    }

    #[test]
    fn occupied_public_socket_is_typed_failure_and_leaves_unconfigured() {
        let (place, directory) = test_place("occupied");
        let bootstrap = Bootstrap::bind(&place).expect("bind meta socket");
        let occupied = std::os::unix::net::UnixListener::bind(&bootstrap.paths().public)
            .expect("occupy public path");

        let outcome = bootstrap.configure(configuration(750));
        assert!(matches!(outcome, ConfigureOutcome::Unavailable(_)));
        assert!(bootstrap.snapshot().is_none());

        drop(occupied);
        std::fs::remove_file(&bootstrap.paths().public).expect("remove owned occupied socket");
        drop(bootstrap);
        std::fs::remove_dir(directory).expect("remove owned test directory");
    }

    #[test]
    fn public_create_crosses_typed_emission_socket_and_enters_owned_memory() {
        use signal_herdr_nexus::{
            Name, Outcome, Pane, PaneId, Query as PublicQuery, Response as PublicResponse, Root,
            Session, Value, Variable, Workspace,
        };
        let (place, directory) = test_place("dispatch");
        let bootstrap = Bootstrap::bind(&place).expect("bind meta socket");
        assert_eq!(
            bootstrap.configure(configuration(750)),
            ConfigureOutcome::Succeeded
        );
        let listener = std::os::unix::net::UnixListener::bind(&bootstrap.paths().translator)
            .expect("bind owned translator fixture");
        let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
        let fixture = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept emission");
            let body = stream.read_frame(capacity).expect("read execute");
            let query = Signal::<emission_herdr_nexus::Query>::from(Vec::<u8>::from(body))
                .restore()
                .expect("restore execute");
            let pane = match query {
                emission_herdr_nexus::Query::Execute(execute) => Pane {
                    session: execute.herdr.session,
                    pane_id: PaneId("owned:p1".into()),
                },
            };
            let response = emission_herdr_nexus::Response::Executed(
                emission_herdr_nexus::ExternalOutcome::Created(pane),
            );
            stream
                .write_frame(&response.signalize().expect("archive response"), capacity)
                .expect("write response");
        });
        let dispatcher = Dispatcher::new(bootstrap.paths());
        let workspace = Workspace {
            session: Session("dispatch".into()),
            root: Root(directory.display().to_string()),
            variable_vector: vec![Variable {
                name: Name("FLOW_ID".into()),
                value: Value("af8fe3".into()),
            }],
        };
        let response = dispatcher.dispatch(
            PublicQuery::Create(workspace),
            bootstrap.snapshot().expect("configured snapshot"),
        );
        assert!(matches!(
            response,
            PublicResponse::Created(Outcome::Created(_))
        ));
        fixture.join().expect("translator fixture");
        std::fs::remove_file(&bootstrap.paths().translator)
            .expect("remove owned translator fixture socket");
        drop(bootstrap);
        std::fs::remove_dir(directory).expect("remove owned test directory");
    }
}
