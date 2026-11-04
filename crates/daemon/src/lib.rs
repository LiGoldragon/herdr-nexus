//! Daemon configuration lifecycle, independent of socket serving.

use meta_signal_herdr_nexus::{BootstrapRefusal, Message, SocketPath};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub use herdr_nexus_wire::deadline::DeadlineStream;

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

    pub fn configure(&self, setting: core_meta_herdr_nexus::CoreSet) -> core_meta_herdr_nexus::Response {
        self.store.configure(setting, || {
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
    configure: impl FnOnce(core_meta_herdr_nexus::CoreSet) -> core_meta_herdr_nexus::Response,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: std::io::Read + std::io::Write,
{
    use signal::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
    let frame = stream.read_frame(capacity)?;
    let query = Signal::<herdr_nexus_wire::meta_call::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = match query {
        herdr_nexus_wire::meta_call::Query::Call(call) if same_login(&call.caller) => {
            let response = match call.input.0 {
                core_meta_herdr_nexus::Query::Configure(setting) => configure(setting),
            };
            herdr_nexus_wire::meta_call::Response::Answered(
                herdr_nexus_wire::meta_payload::Output(response),
            )
        }
        herdr_nexus_wire::meta_call::Query::Call(_) => {
            herdr_nexus_wire::meta_call::Response::Failed("caller is outside the bound login".into())
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
    let query = Signal::<herdr_nexus_wire::public_call::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = match query {
        herdr_nexus_wire::public_call::Query::Call(call) if same_login(&call.caller) => {
            herdr_nexus_wire::public_call::Response::Answered(
                herdr_nexus_wire::public_payload::Output(handle(call.input.0)),
            )
        }
        herdr_nexus_wire::public_call::Query::Call(_) => {
            herdr_nexus_wire::public_call::Response::Failed("caller is outside the bound login".into())
        }
    };
    stream.write_frame(&response.signalize()?, capacity)?;
    Ok(())
}

fn same_login(caller: &herdr_nexus_wire::transport_types::Caller) -> bool {
    caller.user_id.0 == i64::from(nix::unistd::getuid().as_raw())
}

pub struct Dispatcher {
    core: std::sync::Mutex<herdr_nexus_core::Core<herdr_nexus_core::InMemory>>,
    translator_socket: std::path::PathBuf,
    subscriptions: std::sync::Mutex<SubscriptionHub>,
    next_subscription_generation: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct SubscriptionHub {
    sessions: std::collections::HashMap<signal_herdr_nexus::Session, SessionSubscription>,
}

struct SessionSubscription {
    generation: u64,
    configuration: Arc<RuntimeConfiguration>,
    previous:
        Option<std::collections::HashMap<signal_herdr_nexus::Pane, signal_herdr_nexus::State>>,
    subscribers: Vec<std::os::unix::net::UnixStream>,
}

impl Dispatcher {
    pub fn new(paths: &SocketPaths) -> Self {
        Self {
            core: std::sync::Mutex::new(herdr_nexus_core::Core::new(
                herdr_nexus_core::InMemory::default(),
            )),
            translator_socket: paths.translator.clone(),
            subscriptions: std::sync::Mutex::new(SubscriptionHub::default()),
            next_subscription_generation: std::sync::atomic::AtomicU64::new(1),
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
                            timeout: emission_herdr_nexus::Timeout(
                                i64::try_from(configuration.start_timeout.as_millis())
                                    .expect("validated i64 millisecond timeout"),
                            ),
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

    /// Registers a nondurable stream in the session's shared poll source. The
    /// hub serializes acknowledgement and direct event writes under one lock;
    /// there is no delivery queue.
    pub fn subscribe(
        self: &Arc<Self>,
        stream: std::os::unix::net::UnixStream,
        session: signal_herdr_nexus::Session,
        configuration: Arc<RuntimeConfiguration>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use signal::{FrameWriting, Signalizable};
        use signal_herdr_nexus::Response;
        let capacity = signal::FrameCapacity::from(configuration.frame_capacity_bytes);
        let acknowledgement = {
            let mut core = self.core.lock().unwrap_or_else(|p| p.into_inner());
            Response::Subscribed(core.subscribe(
                session.clone(),
                herdr_nexus_core::ExternalResult::Succeeded(()),
            ))
        };
        let mut output = DeadlineStream::new(stream.try_clone()?, configuration.transport_timeout)?;
        let mut hub = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        output.write_frame(&acknowledgement.signalize()?, capacity)?;
        if let Some(existing) = hub.sessions.get_mut(&session) {
            existing.subscribers.push(stream);
            return Ok(());
        }
        let generation = self
            .next_subscription_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        hub.sessions.insert(
            session.clone(),
            SessionSubscription {
                generation,
                configuration: Arc::clone(&configuration),
                previous: None,
                subscribers: vec![stream],
            },
        );
        drop(hub);
        let dispatcher = Arc::clone(self);
        std::thread::spawn(move || dispatcher.poll_subscription(session, generation));
        Ok(())
    }

    fn poll_subscription(self: Arc<Self>, session: signal_herdr_nexus::Session, generation: u64) {
        use signal_herdr_nexus::{Changed_Data, Outcome, Query, Response};
        loop {
            let configuration = {
                let hub = self
                    .subscriptions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(subscription) = hub.sessions.get(&session) else {
                    return;
                };
                if subscription.generation != generation {
                    return;
                }
                Arc::clone(&subscription.configuration)
            };
            std::thread::sleep(configuration.polling);
            {
                let mut hub = self
                    .subscriptions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(subscription) = hub.sessions.get_mut(&session) else {
                    return;
                };
                if subscription.generation != generation {
                    return;
                }
                subscription.subscribers.retain(subscriber_connected);
                if subscription.subscribers.is_empty() {
                    hub.sessions.remove(&session);
                    return;
                }
            }
            let response = self.dispatch(Query::List(session.clone()), Arc::clone(&configuration));
            let complete = match response {
                Response::Listed(Outcome::Listed(known)) => Some(
                    known
                        .into_iter()
                        .map(|known| (known.pane, known.state))
                        .collect::<std::collections::HashMap<_, _>>(),
                ),
                _ => None,
            };
            let mut hub = self
                .subscriptions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(subscription) = hub.sessions.get_mut(&session) else {
                return;
            };
            if subscription.generation != generation {
                return;
            }
            if let Some(current) = complete {
                let changes = subscription
                    .previous
                    .as_ref()
                    .map(|previous| {
                        current
                            .iter()
                            .filter(|(pane, state)| previous.get(*pane) != Some(*state))
                            .map(|(pane, state)| {
                                Response::Changed(Changed_Data {
                                    pane: pane.clone(),
                                    state: state.clone(),
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for change in changes {
                    subscription.subscribers.retain_mut(|stream| {
                        write_subscription_change(stream, &configuration, &change).is_ok()
                    });
                }
                subscription.previous = Some(current);
            }
            if subscription.subscribers.is_empty() {
                hub.sessions.remove(&session);
                return;
            }
        }
    }

    #[cfg(test)]
    fn subscription_source_count(&self) -> usize {
        self.subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .sessions
            .len()
    }

    fn execute(
        &self,
        configuration: &RuntimeConfiguration,
        herdr: emission_herdr_nexus::Herdr,
    ) -> emission_herdr_nexus::ExternalOutcome {
        use signal::{ByteViewable, Restorable, Signal, Signalizable};
        use herdr_nexus_wire::transport_types::{Kind, Lane};
        let result = (|| -> Result<_, Box<dyn std::error::Error>> {
            let stream = std::os::unix::net::UnixStream::connect(&self.translator_socket)?;
            let mut stream = DeadlineStream::new(stream, configuration.transport_timeout)?;
            let request = emission_herdr_nexus::Query::Emit(herdr);
            let request = request.signalize()?;
            herdr_nexus_wire::frame::write_frame(&mut stream, &herdr_nexus_wire::frame::Frame {
                lane: Lane::EmissionSignal,
                kind: Kind::Request,
                payload: request.bytes().to_vec(),
            }, configuration.frame_capacity_bytes)?;
            let body = herdr_nexus_wire::frame::read_frame(&mut stream, configuration.frame_capacity_bytes)?;
            if body.lane != Lane::EmissionSignal || body.kind != Kind::Answered {
                return Err("translator returned a non-answer emission frame".into());
            }
            let response =
                Signal::<emission_herdr_nexus::Response>::from(body.payload).restore()?;
            match response {
                emission_herdr_nexus::Response::Emitted(outcome) => Ok(outcome),
            }
        })();
        result.unwrap_or(emission_herdr_nexus::ExternalOutcome::Uncertain)
    }
}

fn subscriber_connected(stream: &std::os::unix::net::UnixStream) -> bool {
    use std::io::Read;
    let Ok(mut probe) = stream.try_clone() else {
        return false;
    };
    if probe.set_nonblocking(true).is_err() {
        return false;
    }
    let mut byte = [0_u8; 1];
    match probe.read(&mut byte) {
        Ok(0) => false,
        // A subscription is server-to-client after its one query. Any further
        // client byte is a protocol violation, so consuming it and removing
        // the subscriber is the wanted behavior.
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(_) => false,
    }
}

fn write_subscription_change(
    stream: &std::os::unix::net::UnixStream,
    configuration: &RuntimeConfiguration,
    change: &signal_herdr_nexus::Response,
) -> Result<(), Box<dyn std::error::Error>> {
    use signal::{FrameWriting, Signalizable};
    let mut output = DeadlineStream::new(stream.try_clone()?, configuration.transport_timeout)?;
    output.write_frame(
        &change.signalize()?,
        signal::FrameCapacity::from(configuration.frame_capacity_bytes),
    )?;
    Ok(())
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
    pub polling: Duration,
    pub start_timeout: Duration,
    pub frame_capacity_bytes: usize,
    pub transport_timeout: Duration,
}

#[derive(Clone, Default)]
struct PartialConfiguration {
    polling: Option<Duration>,
    start_timeout: Option<Duration>,
    frame_capacity_bytes: Option<usize>,
    transport_timeout: Option<Duration>,
}

#[derive(Default)]
pub struct ConfigurationStore {
    partial: RwLock<PartialConfiguration>,
    current: RwLock<Option<Arc<RuntimeConfiguration>>>,
}

impl ConfigurationStore {
    pub fn configure(
        &self,
        setting: core_meta_herdr_nexus::CoreSet,
        bind_public: impl FnOnce() -> Result<(), meta_signal_herdr_nexus::BootstrapRefusal>,
    ) -> core_meta_herdr_nexus::Response {
        use core_meta_herdr_nexus::Response;
        use meta_signal_herdr_nexus::{ConfigurationRefusal, Invalid_Data, Message, Owner, Readiness, SettingName};
        let mut candidate = self.partial.read().unwrap_or_else(|p| p.into_inner()).clone();
        let (name, valid) = match setting {
            core_meta_herdr_nexus::CoreSet::StartTimeout(value) => (SettingName::StartTimeout, checked_duration(value.0).map(|value| candidate.start_timeout = Some(value)).is_some()),
            core_meta_herdr_nexus::CoreSet::Polling(value) => (SettingName::Polling, checked_duration(value.0).map(|value| candidate.polling = Some(value)).is_some()),
            core_meta_herdr_nexus::CoreSet::FrameCapacity(value) => (SettingName::FrameCapacity, usize::try_from(value.0).ok().filter(|value| (1..=signal::MAXIMUM_SIGNAL_BYTES).contains(value)).map(|value| candidate.frame_capacity_bytes = Some(value)).is_some()),
            core_meta_herdr_nexus::CoreSet::TransportTimeout(value) => (SettingName::TransportTimeout, checked_duration(value.0).map(|value| candidate.transport_timeout = Some(value)).is_some()),
        };
        let cross_valid = match (candidate.transport_timeout, candidate.start_timeout) {
            (Some(transport), Some(start)) => transport > start,
            _ => true,
        };
        if !valid || !cross_valid {
            return Response::ConfigureRefused(ConfigurationRefusal::Invalid(Invalid_Data {
                owner: Owner::Nexus,
                setting_name: name,
                message: Message(if valid { "TransportTimeout must exceed StartTimeout" } else { "invalid setting value" }.into()),
            }));
        }
        let readiness = readiness(&candidate);
        let validated = match (&candidate.polling, &candidate.start_timeout, &candidate.frame_capacity_bytes, &candidate.transport_timeout) {
            (Some(polling), Some(start_timeout), Some(frame_capacity_bytes), Some(transport_timeout)) => Some(RuntimeConfiguration {
                polling: *polling,
                start_timeout: *start_timeout,
                frame_capacity_bytes: *frame_capacity_bytes,
                transport_timeout: *transport_timeout,
            }),
            _ => None,
        };
        if validated.is_some() && self.current.read().unwrap_or_else(|p| p.into_inner()).is_none() {
            if let Err(refusal) = bind_public() {
                return Response::ConfigureRefused(ConfigurationRefusal::Unavailable(refusal));
            }
        }
        *self.partial.write().unwrap_or_else(|p| p.into_inner()) = candidate;
        if let Some(validated) = validated {
            *self.current.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(validated));
        }
        Response::Configured(readiness)
    }

    pub fn snapshot(&self) -> Option<Arc<RuntimeConfiguration>> {
        self.current.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

fn readiness(candidate: &PartialConfiguration) -> meta_signal_herdr_nexus::Readiness {
    use meta_signal_herdr_nexus::{Readiness, SettingName};
    let mut awaiting = Vec::new();
    if candidate.start_timeout.is_none() { awaiting.push(SettingName::StartTimeout); }
    if candidate.polling.is_none() { awaiting.push(SettingName::Polling); }
    if candidate.frame_capacity_bytes.is_none() { awaiting.push(SettingName::FrameCapacity); }
    if candidate.transport_timeout.is_none() { awaiting.push(SettingName::TransportTimeout); }
    if awaiting.is_empty() { Readiness::Ready } else { Readiness::Awaiting(awaiting) }
}

fn checked_duration(milliseconds: i64) -> Option<Duration> {
    let duration = Duration::from_millis(u64::try_from(milliseconds).ok()?);
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() { None } else { Some(duration) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_timeout_must_exceed_start_timeout() {
        let store = ConfigurationStore::default();
        let _ = store.configure(core_meta_herdr_nexus::CoreSet::StartTimeout(meta_signal_herdr_nexus::Timeout(50)), || Ok(()));
        let response = store.configure(core_meta_herdr_nexus::CoreSet::TransportTimeout(meta_signal_herdr_nexus::Timeout(50)), || Ok(()));
        assert!(matches!(response, core_meta_herdr_nexus::Response::ConfigureRefused(_)));
    }
}
