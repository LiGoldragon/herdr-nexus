//! Daemon configuration lifecycle, independent of socket serving.

use meta_signal_herdr_nexus::{BootstrapRefusal, Message, SocketPath};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub use herdr_nexus_wire::deadline::DeadlineStream;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketPaths {
    pub public: std::path::PathBuf,
    pub meta: std::path::PathBuf,
    pub translator: std::path::PathBuf,
    pub meta_translator: std::path::PathBuf,
}

impl From<&shared_translator::placement::Place> for SocketPaths {
    fn from(place: &shared_translator::placement::Place) -> Self {
        let directory = std::path::PathBuf::from(&place.directory.0);
        let names = herdr_nexus_wire::socket_names();
        Self {
            public: directory.join(names.public_nexus_socket.0),
            meta: directory.join(names.meta_nexus_socket.0),
            translator: directory.join(names.public_translator_socket.0),
            meta_translator: directory.join(names.meta_translator_socket.0),
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
    store: Arc<ConfigurationStore>,
    meta: OwnedListener,
    public: std::sync::Mutex<Option<OwnedListener>>,
    public_ready: std::sync::Condvar,
    admission: herdr_nexus_wire::ValidAdmission,
    translator_peer: herdr_nexus_wire::transport_types::PeerIdentity,
}

impl Bootstrap {
    pub fn bind(
        place: &shared_translator::placement::Place,
        admission: herdr_nexus_wire::ValidAdmission,
        translator_peer: herdr_nexus_wire::transport_types::PeerIdentity,
    ) -> std::io::Result<Self> {
        let paths = SocketPaths::from(place);
        let meta = OwnedListener::bind(paths.meta.clone())?;
        Ok(Self {
            paths,
            store: Arc::new(ConfigurationStore::default()),
            meta,
            public: std::sync::Mutex::new(None),
            public_ready: std::sync::Condvar::new(),
            admission,
            translator_peer,
        })
    }

    pub fn configure(
        &self,
        setting: core_meta_herdr_nexus::CoreSet,
    ) -> core_meta_herdr_nexus::Response {
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

    pub fn configuration_store(&self) -> Arc<ConfigurationStore> {
        Arc::clone(&self.store)
    }

    pub fn serve_next_meta(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (stream, _) = self.meta.listener.accept()?;
        require_peer(&stream, &self.translator_peer)?;
        let mut stream = DeadlineStream::new(stream, self.admission.deadline)?;
        serve_meta_once(
            &mut stream,
            signal::FrameCapacity::from(self.admission.frame_capacity),
            |candidate| self.configure(candidate),
        )
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
        loop {
            let stream = public
                .as_ref()
                .expect("public listener notified only after binding")
                .listener
                .accept()
                .map(|(stream, _)| stream)?;
            if require_peer(&stream, &self.translator_peer).is_ok() {
                return Ok(stream);
            }
        }
    }

    pub fn paths(&self) -> &SocketPaths {
        &self.paths
    }
}

fn require_peer(
    stream: &std::os::unix::net::UnixStream,
    expected: &herdr_nexus_wire::transport_types::PeerIdentity,
) -> std::io::Result<()> {
    let actual = herdr_nexus_wire::frame::peer_caller(stream)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if actual.user_id == expected.user_id && actual.process_id == expected.process_id {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "peer is not the paired translator",
        ))
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
    let query =
        Signal::<herdr_nexus_wire::meta_call::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = match query {
        herdr_nexus_wire::meta_call::Query::Call(call) => {
            let response = match call.input.0 {
                core_meta_herdr_nexus::Query::Configure(setting) => configure(setting),
            };
            herdr_nexus_wire::meta_call::Response::Answered(herdr_nexus_wire::meta_payload::Output(
                response,
            ))
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
    let query =
        Signal::<herdr_nexus_wire::public_call::Query>::from(Vec::<u8>::from(frame)).restore()?;
    let response = match query {
        herdr_nexus_wire::public_call::Query::Call(call) => {
            herdr_nexus_wire::public_call::Response::Answered(
                herdr_nexus_wire::public_payload::Output(handle(call.input.0)),
            )
        }
    };
    stream.write_frame(&response.signalize()?, capacity)?;
    Ok(())
}

pub fn same_login(caller: &herdr_nexus_wire::transport_types::Caller) -> bool {
    caller.user_id.0 == i64::from(nix::unistd::getuid().as_raw())
}

pub struct Dispatcher {
    state: std::sync::Mutex<DispatcherState>,
    emitter: Arc<dyn ExternalEmitter>,
    subscriptions: std::sync::Mutex<SubscriptionHub>,
    next_subscription_generation: std::sync::atomic::AtomicU64,
    configuration: Arc<ConfigurationStore>,
}

pub trait ExternalEmitter: Send + Sync {
    fn emit(
        &self,
        configuration: &RuntimeConfiguration,
        herdr: emission_herdr_nexus::Herdr,
    ) -> EmissionResult;
}

pub enum EmissionResult {
    External(emission_herdr_nexus::ExternalOutcome),
    Unavailable(signal_herdr_nexus::TransportRefusal),
}

struct SocketEmitter {
    translator_socket: std::path::PathBuf,
}

struct DispatcherState {
    core: herdr_nexus_core::Core<herdr_nexus_core::InMemory>,
    reserved: std::collections::HashMap<signal_herdr_nexus::Pane, u64>,
    revisions: std::collections::HashMap<signal_herdr_nexus::Pane, u64>,
    next_reservation: u64,
}

#[derive(Default)]
struct SubscriptionHub {
    sessions: std::collections::HashMap<signal_herdr_nexus::Session, SessionSubscription>,
}

struct SessionSubscription {
    generation: u64,
    subscribers: Vec<Subscriber>,
}

struct Subscriber {
    stream: std::os::unix::net::UnixStream,
    delivered: std::collections::HashMap<signal_herdr_nexus::Pane, signal_herdr_nexus::State>,
}

fn operation(
    query: signal_herdr_nexus::Query,
) -> herdr_nexus_core::generated::operation::Operation {
    use herdr_nexus_core::generated::operation::{
        Operation, Pressing_Data, Prompting_Data, Starting_Data,
    };
    match query {
        signal_herdr_nexus::Query::Create(workspace) => Operation::Creating(workspace),
        signal_herdr_nexus::Query::Start(start) => Operation::Starting(Starting_Data {
            pane: start.pane,
            command: start.command,
            label: start.label,
        }),
        signal_herdr_nexus::Query::Prompt(prompt) => Operation::Prompting(Prompting_Data {
            pane: prompt.pane,
            text: prompt.text,
        }),
        signal_herdr_nexus::Query::Press(press) => Operation::Pressing(Pressing_Data {
            pane: press.pane,
            keys: press.keys,
        }),
        signal_herdr_nexus::Query::Close(pane) => Operation::Closing(pane),
        signal_herdr_nexus::Query::List(session) => Operation::Listing(session),
        signal_herdr_nexus::Query::Subscribe(session) => Operation::Subscribing(session),
    }
}

impl Dispatcher {
    pub fn new(paths: &SocketPaths, configuration: Arc<ConfigurationStore>) -> Self {
        Self {
            state: std::sync::Mutex::new(DispatcherState {
                core: herdr_nexus_core::Core::new(herdr_nexus_core::InMemory::default()),
                reserved: std::collections::HashMap::new(),
                revisions: std::collections::HashMap::new(),
                next_reservation: 1,
            }),
            emitter: Arc::new(SocketEmitter {
                translator_socket: paths.translator.clone(),
            }),
            subscriptions: std::sync::Mutex::new(SubscriptionHub::default()),
            next_subscription_generation: std::sync::atomic::AtomicU64::new(1),
            configuration,
        }
    }

    #[cfg(test)]
    fn with_emitter(paths: &SocketPaths, emitter: Arc<dyn ExternalEmitter>) -> Self {
        let configuration = Arc::new(ConfigurationStore::default());
        *configuration.current.write().expect("configuration") =
            Some(Arc::new(RuntimeConfiguration {
                polling: Duration::from_millis(10),
                start_timeout: Duration::from_millis(20),
                frame_capacity_bytes: 4096,
                transport_timeout: Duration::from_millis(30),
                subscriber_capacity: 8,
            }));
        let mut dispatcher = Self::new(paths, configuration);
        dispatcher.emitter = emitter;
        dispatcher
    }

    pub fn dispatch(
        &self,
        query: signal_herdr_nexus::Query,
        configuration: Arc<RuntimeConfiguration>,
    ) -> signal_herdr_nexus::Response {
        use emission_herdr_nexus::{
            AgentPrompt_Data, AgentStart_Data, Herdr, Keys_Data, Verb, WorkspaceCreate_Data,
        };
        use herdr_nexus_core::generated::operation::Operation;
        use signal_herdr_nexus::Response;
        match operation(query) {
            Operation::Creating(workspace) => {
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
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let outcome = state.core.create(workspace, created_result(external));
                if let signal_herdr_nexus::Outcome::Created(pane) = &outcome {
                    state.revisions.insert(pane.clone(), 1);
                }
                Response::Created(outcome)
            }
            Operation::Starting(start) => {
                let reservation = match self.reserve(&start.pane) {
                    Ok(token) => token,
                    Err(outcome) => return Response::Started(outcome),
                };
                let command = start.command.clone();
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
                let mut state = self.finish_reservation(&start.pane, reservation);
                let pane = start.pane;
                let outcome = state
                    .core
                    .start(pane.clone(), command, start.label, started_result(external));
                if outcome == signal_herdr_nexus::Outcome::Done {
                    bump_revision(&mut state.revisions, pane);
                }
                Response::Started(outcome)
            }
            Operation::Prompting(prompt) => {
                let reservation = match self.reserve(&prompt.pane) {
                    Ok(token) => token,
                    Err(outcome) => return Response::Prompted(outcome),
                };
                let text = prompt.text.clone();
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
                let mut state = self.finish_reservation(&prompt.pane, reservation);
                Response::Prompted(state.core.prompt(prompt.pane, text, done_result(external)))
            }
            Operation::Pressing(press) => {
                let reservation = match self.reserve(&press.pane) {
                    Ok(token) => token,
                    Err(outcome) => return Response::Pressed(outcome),
                };
                let keys = press.keys.clone();
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
                let mut state = self.finish_reservation(&press.pane, reservation);
                Response::Pressed(state.core.press(press.pane, keys, done_result(external)))
            }
            Operation::Closing(pane) => {
                let reservation = match self.reserve(&pane) {
                    Ok(token) => token,
                    Err(outcome) => return Response::Closed(outcome),
                };
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: pane.session.clone(),
                        verb: Verb::PaneClose(pane.pane_id.clone()),
                    },
                );
                let mut state = self.finish_reservation(&pane, reservation);
                let outcome = state.core.close(pane.clone(), done_result(external));
                if outcome == signal_herdr_nexus::Outcome::Done {
                    state.revisions.remove(&pane);
                }
                Response::Closed(outcome)
            }
            Operation::Listing(session) => {
                let captured = {
                    let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    match state.core.preflight_session(&session) {
                        Ok(false) => {
                            return Response::Listed(
                                signal_herdr_nexus::Outcome::Listed(Vec::new()),
                            )
                        }
                        Ok(true) => state.revisions.clone(),
                        Err(outcome) => return Response::Listed(outcome),
                    }
                };
                let external = self.execute(
                    &configuration,
                    Herdr {
                        session: session.clone(),
                        verb: Verb::PaneList,
                    },
                );
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let external = protect_newer_state(&state, &session, &captured, external);
                let outcome = state.core.list(session.clone(), listed_result(external));
                if matches!(outcome, signal_herdr_nexus::Outcome::Listed(_)) {
                    if let Ok(owned) = state.core.owned_in_session(&session) {
                        for owned in owned {
                            bump_revision(&mut state.revisions, owned.pane);
                        }
                    }
                }
                Response::Listed(outcome)
            }
            Operation::Subscribing(session) => {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                Response::Subscribed(
                    state
                        .core
                        .subscribe(session, herdr_nexus_core::ExternalResult::Uncertain),
                )
            }
        }
    }

    fn reserve(&self, pane: &signal_herdr_nexus::Pane) -> Result<u64, signal_herdr_nexus::Outcome> {
        use signal_herdr_nexus::{AttemptInProgress, Outcome};
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.core.preflight_owned(pane)?;
        if state.reserved.contains_key(pane) {
            return Err(Outcome::AttemptInProgress(AttemptInProgress(pane.clone())));
        }
        let token = state.next_reservation;
        state.next_reservation = state.next_reservation.wrapping_add(1).max(1);
        state.reserved.insert(pane.clone(), token);
        Ok(token)
    }

    fn finish_reservation(
        &self,
        pane: &signal_herdr_nexus::Pane,
        token: u64,
    ) -> std::sync::MutexGuard<'_, DispatcherState> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let owns = state.reserved.get(pane).copied() == Some(token);
        assert!(owns, "only the reservation token owner may finalize it");
        state.reserved.remove(pane);
        state
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
        use signal_herdr_nexus::{Changed_Data, Response};
        let (acknowledgement, current) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let acknowledgement = Response::Subscribed(state.core.subscribe(
                session.clone(),
                herdr_nexus_core::ExternalResult::Succeeded(()),
            ));
            let current = state
                .core
                .owned_in_session(&session)
                .map_err(|_| std::io::Error::other("Memory refused subscription snapshot"))?
                .into_iter()
                .map(|known| (known.pane, known.state))
                .collect::<std::collections::HashMap<_, _>>();
            (acknowledgement, current)
        };
        let mut hub = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        write_subscription_change(&stream, &configuration, &acknowledgement)?;
        for (pane, state) in &current {
            write_subscription_change(
                &stream,
                &configuration,
                &Response::Changed(Changed_Data {
                    pane: pane.clone(),
                    state: state.clone(),
                }),
            )?;
        }
        if let Some(existing) = hub.sessions.get_mut(&session) {
            existing.subscribers.push(Subscriber {
                stream,
                delivered: current,
            });
            return Ok(());
        }
        let generation = self
            .next_subscription_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        hub.sessions.insert(
            session.clone(),
            SessionSubscription {
                generation,
                subscribers: vec![Subscriber {
                    stream,
                    delivered: current,
                }],
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
            let Some(configuration) = self.configuration.snapshot() else {
                return;
            };
            {
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
            }
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
                subscription
                    .subscribers
                    .retain(|subscriber| subscriber_connected(&subscriber.stream));
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
                subscription.subscribers.retain_mut(|subscriber| {
                    let changes = current
                        .iter()
                        .filter(|(pane, state)| subscriber.delivered.get(*pane) != Some(*state))
                        .map(|(pane, state)| {
                            Response::Changed(Changed_Data {
                                pane: pane.clone(),
                                state: state.clone(),
                            })
                        })
                        .collect::<Vec<_>>();
                    let delivered = changes.iter().all(|change| {
                        write_subscription_change(&subscriber.stream, &configuration, change).is_ok()
                    });
                    if delivered {
                        subscriber.delivered = current.clone();
                    }
                    delivered
                });
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
    ) -> EmissionResult {
        self.emitter.emit(configuration, herdr)
    }
}

impl ExternalEmitter for SocketEmitter {
    fn emit(
        &self,
        configuration: &RuntimeConfiguration,
        herdr: emission_herdr_nexus::Herdr,
    ) -> EmissionResult {
        use herdr_nexus_wire::transport_types::{Kind, Lane};
        use signal::{ByteViewable, Restorable, Signal, Signalizable};
        let stream = match std::os::unix::net::UnixStream::connect(&self.translator_socket) {
            Ok(stream) => stream,
            Err(error) => return unavailable_transport(&self.translator_socket, error.to_string()),
        };
        let mut stream = match DeadlineStream::new(stream, configuration.transport_timeout) {
            Ok(stream) => stream,
            Err(error) => return unavailable_transport(&self.translator_socket, error.to_string()),
        };
        let request = emission_herdr_nexus::Query::Emit(herdr);
        let request = match request.signalize() {
            Ok(request) => request,
            Err(error) => {
                return unavailable_transport(&self.translator_socket, format!("{error:?}"))
            }
        };
        if let Err(failure) = herdr_nexus_wire::frame::write_frame_witness(
            &mut stream,
            &herdr_nexus_wire::frame::Frame {
                lane: Lane::EmissionSignal,
                kind: Kind::Request,
                payload: request.bytes().to_vec(),
            },
            configuration.frame_capacity_bytes,
        ) {
            return unavailable_transport(
                &self.translator_socket,
                format!(
                    "incomplete request submission after {} bytes: {:?}",
                    failure.written, failure.error
                ),
            );
        }
        let result = (|| -> Result<_, Box<dyn std::error::Error>> {
            let body = herdr_nexus_wire::frame::read_frame(
                &mut stream,
                configuration.frame_capacity_bytes,
            )?;
            if body.lane != Lane::EmissionSignal || body.kind != Kind::Answered {
                return Err("translator returned a non-answer emission frame".into());
            }
            let response =
                Signal::<emission_herdr_nexus::Response>::from(body.payload).restore()?;
            match response {
                emission_herdr_nexus::Response::Emitted(outcome) => Ok(outcome),
            }
        })();
        EmissionResult::External(result.unwrap_or(emission_herdr_nexus::ExternalOutcome::Uncertain))
    }
}

fn bump_revision(
    revisions: &mut std::collections::HashMap<signal_herdr_nexus::Pane, u64>,
    pane: signal_herdr_nexus::Pane,
) {
    let next = revisions
        .get(&pane)
        .copied()
        .unwrap_or(0)
        .wrapping_add(1)
        .max(1);
    revisions.insert(pane, next);
}

fn protect_newer_state(
    state: &DispatcherState,
    session: &signal_herdr_nexus::Session,
    captured: &std::collections::HashMap<signal_herdr_nexus::Pane, u64>,
    external: EmissionResult,
) -> EmissionResult {
    let EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Listed(mut observed)) =
        external
    else {
        return external;
    };
    let Ok(owned) = state.core.owned_in_session(session) else {
        return EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Listed(observed));
    };
    for current in owned {
        let changed = captured.get(&current.pane) != state.revisions.get(&current.pane)
            || state.reserved.contains_key(&current.pane);
        if changed {
            observed.retain(|item| item.pane != current.pane);
            observed.push(emission_herdr_nexus::PaneObservation {
                pane: current.pane,
                state: current.state,
            });
        }
    }
    EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Listed(observed))
}

fn unavailable_transport(path: &std::path::Path, message: String) -> EmissionResult {
    EmissionResult::Unavailable(signal_herdr_nexus::TransportRefusal {
        socket_path: signal_herdr_nexus::SocketPath(path.display().to_string()),
        message: signal_herdr_nexus::Message(message),
    })
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
    let change = herdr_nexus_wire::public_call::Response::Answered(
        herdr_nexus_wire::public_payload::Output(change.clone()),
    );
    output.write_frame(
        &change.signalize()?,
        signal::FrameCapacity::from(configuration.frame_capacity_bytes),
    )?;
    Ok(())
}

fn done_result(outcome: EmissionResult) -> herdr_nexus_core::ExternalResult<()> {
    match outcome {
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Done) => {
            herdr_nexus_core::ExternalResult::Succeeded(())
        }
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Refused(refusal)) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            herdr_nexus_core::ExternalResult::Unavailable(refusal)
        }
        _ => herdr_nexus_core::ExternalResult::Uncertain,
    }
}

fn started_result(
    outcome: EmissionResult,
) -> herdr_nexus_core::ExternalResult<signal_herdr_nexus::State> {
    match outcome {
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Started(state)) => {
            herdr_nexus_core::ExternalResult::Succeeded(state)
        }
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Refused(refusal)) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            herdr_nexus_core::ExternalResult::Unavailable(refusal)
        }
        _ => herdr_nexus_core::ExternalResult::Uncertain,
    }
}

fn created_result(
    outcome: EmissionResult,
) -> herdr_nexus_core::ExternalResult<signal_herdr_nexus::Pane> {
    match outcome {
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Created(pane)) => {
            herdr_nexus_core::ExternalResult::Succeeded(pane)
        }
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Refused(refusal)) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            herdr_nexus_core::ExternalResult::Unavailable(refusal)
        }
        _ => herdr_nexus_core::ExternalResult::Uncertain,
    }
}

fn listed_result(
    outcome: EmissionResult,
) -> herdr_nexus_core::ExternalResult<Vec<emission_herdr_nexus::PaneObservation>> {
    match outcome {
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Listed(observed)) => {
            herdr_nexus_core::ExternalResult::Succeeded(observed)
        }
        EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Refused(refusal)) => {
            herdr_nexus_core::ExternalResult::Refused(refusal)
        }
        EmissionResult::Unavailable(refusal) => {
            herdr_nexus_core::ExternalResult::Unavailable(refusal)
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
    pub subscriber_capacity: usize,
}

#[derive(Clone, Default)]
struct PartialConfiguration {
    polling: Option<Duration>,
    start_timeout: Option<Duration>,
    frame_capacity_bytes: Option<usize>,
    transport_timeout: Option<Duration>,
    subscriber_capacity: Option<usize>,
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
        use meta_signal_herdr_nexus::{
            ConfigurationRefusal, Invalid_Data, Message, Owner, Readiness, SettingName,
        };
        let mut candidate = self
            .partial
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let applied = setting.clone();
        let (name, valid) = match setting {
            core_meta_herdr_nexus::CoreSet::StartTimeout(value) => (
                SettingName::StartTimeout,
                checked_duration(value.0)
                    .map(|value| candidate.start_timeout = Some(value))
                    .is_some(),
            ),
            core_meta_herdr_nexus::CoreSet::Polling(value) => (
                SettingName::Polling,
                checked_duration(value.0)
                    .map(|value| candidate.polling = Some(value))
                    .is_some(),
            ),
            core_meta_herdr_nexus::CoreSet::FrameCapacity(value) => (
                SettingName::FrameCapacity,
                usize::try_from(value.0)
                    .ok()
                    .filter(|value| (1..=signal::MAXIMUM_SIGNAL_BYTES).contains(value))
                    .map(|value| candidate.frame_capacity_bytes = Some(value))
                    .is_some(),
            ),
            core_meta_herdr_nexus::CoreSet::TransportTimeout(value) => (
                SettingName::TransportTimeout,
                checked_duration(value.0)
                    .map(|value| candidate.transport_timeout = Some(value))
                    .is_some(),
            ),
            core_meta_herdr_nexus::CoreSet::SubscriberCapacity(value) => (
                SettingName::SubscriberCapacity,
                usize::try_from(value)
                    .ok()
                    .filter(|value| *value > 0)
                    .map(|value| candidate.subscriber_capacity = Some(value))
                    .is_some(),
            ),
        };
        let cross_valid = match (candidate.transport_timeout, candidate.start_timeout) {
            (Some(transport), Some(start)) => transport > start,
            _ => true,
        };
        if !valid || !cross_valid {
            return Response::ConfigureRefused(ConfigurationRefusal::Invalid(Invalid_Data {
                owner: Owner::Nexus,
                setting_name: name,
                message: Message(
                    if valid {
                        "TransportTimeout must exceed StartTimeout"
                    } else {
                        "invalid setting value"
                    }
                    .into(),
                ),
            }));
        }
        let readiness = readiness(&candidate);
        let validated = match (
            &candidate.polling,
            &candidate.start_timeout,
            &candidate.frame_capacity_bytes,
            &candidate.transport_timeout,
            &candidate.subscriber_capacity,
        ) {
            (
                Some(polling),
                Some(start_timeout),
                Some(frame_capacity_bytes),
                Some(transport_timeout),
                Some(subscriber_capacity),
            ) => Some(RuntimeConfiguration {
                polling: *polling,
                start_timeout: *start_timeout,
                frame_capacity_bytes: *frame_capacity_bytes,
                transport_timeout: *transport_timeout,
                subscriber_capacity: *subscriber_capacity,
            }),
            _ => None,
        };
        if validated.is_some()
            && self
                .current
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
        {
            if let Err(refusal) = bind_public() {
                return Response::ConfigureRefused(ConfigurationRefusal::Unavailable(refusal));
            }
        }
        *self.partial.write().unwrap_or_else(|p| p.into_inner()) = candidate;
        if let Some(validated) = validated {
            *self.current.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(validated));
        }
        Response::Configured(core_meta_herdr_nexus::Configured_Data {
            core_set: applied,
            readiness,
        })
    }

    pub fn snapshot(&self) -> Option<Arc<RuntimeConfiguration>> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

fn readiness(candidate: &PartialConfiguration) -> meta_signal_herdr_nexus::Readiness {
    use meta_signal_herdr_nexus::{Readiness, SettingName};
    let mut awaiting = Vec::new();
    if candidate.start_timeout.is_none() {
        awaiting.push(SettingName::StartTimeout);
    }
    if candidate.polling.is_none() {
        awaiting.push(SettingName::Polling);
    }
    if candidate.frame_capacity_bytes.is_none() {
        awaiting.push(SettingName::FrameCapacity);
    }
    if candidate.transport_timeout.is_none() {
        awaiting.push(SettingName::TransportTimeout);
    }
    if candidate.subscriber_capacity.is_none() {
        awaiting.push(SettingName::SubscriberCapacity);
    }
    if awaiting.is_empty() {
        Readiness::Ready
    } else {
        Readiness::Awaiting(awaiting)
    }
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

    struct CountingEmitter(std::sync::atomic::AtomicUsize);

    impl ExternalEmitter for CountingEmitter {
        fn emit(
            &self,
            _configuration: &RuntimeConfiguration,
            _herdr: emission_herdr_nexus::Herdr,
        ) -> EmissionResult {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            EmissionResult::External(emission_herdr_nexus::ExternalOutcome::Uncertain)
        }
    }

    fn test_paths() -> SocketPaths {
        SocketPaths {
            public: "/tmp/herdr-test-public".into(),
            meta: "/tmp/herdr-test-meta".into(),
            translator: "/tmp/herdr-test-translator".into(),
            meta_translator: "/tmp/herdr-test-meta-translator".into(),
        }
    }

    fn test_configuration() -> Arc<RuntimeConfiguration> {
        Arc::new(RuntimeConfiguration {
            polling: Duration::from_millis(10),
            start_timeout: Duration::from_millis(20),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(30),
            subscriber_capacity: 8,
        })
    }

    fn test_pane() -> signal_herdr_nexus::Pane {
        signal_herdr_nexus::Pane {
            session: signal_herdr_nexus::Session("missing".into()),
            pane_id: signal_herdr_nexus::PaneId("pane".into()),
        }
    }

    #[test]
    fn four_unowned_mutations_stop_before_the_emitter() {
        use signal_herdr_nexus::{
            AgentKind, AgentName, Command, Executable, Keys, Label, Press_Data, Program,
            Prompt_Data, Query, Start_Data, Text,
        };
        let emitter = Arc::new(CountingEmitter(std::sync::atomic::AtomicUsize::new(0)));
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let pane = test_pane();
        let configuration = test_configuration();
        let command = Command {
            agent_name: AgentName("probe".into()),
            agent_kind: AgentKind::Codex,
            executable: Executable::Explicit(Program("/bin/true".into())),
            string_vector: Vec::new(),
        };
        let queries = [
            Query::Start(Start_Data {
                pane: pane.clone(),
                command,
                label: Label("label".into()),
            }),
            Query::Prompt(Prompt_Data {
                pane: pane.clone(),
                text: Text("hello".into()),
            }),
            Query::Press(Press_Data {
                pane: pane.clone(),
                keys: Keys(vec!["x".into()]),
            }),
            Query::Close(pane),
        ];
        for query in queries {
            let response = dispatcher.dispatch(query, Arc::clone(&configuration));
            let outcome = match response {
                signal_herdr_nexus::Response::Started(outcome)
                | signal_herdr_nexus::Response::Prompted(outcome)
                | signal_herdr_nexus::Response::Pressed(outcome)
                | signal_herdr_nexus::Response::Closed(outcome) => outcome,
                _ => panic!("mutation response"),
            };
            assert!(matches!(outcome, signal_herdr_nexus::Outcome::Unknown(_)));
        }
        assert_eq!(emitter.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn empty_owned_list_is_complete_without_external_observation() {
        let emitter = Arc::new(CountingEmitter(std::sync::atomic::AtomicUsize::new(0)));
        let dispatcher = Dispatcher::with_emitter(&test_paths(), emitter.clone());
        let response = dispatcher.dispatch(
            signal_herdr_nexus::Query::List(signal_herdr_nexus::Session("empty".into())),
            test_configuration(),
        );
        assert!(matches!(
            response,
            signal_herdr_nexus::Response::Listed(signal_herdr_nexus::Outcome::Listed(ref panes)) if panes.is_empty()
        ));
        assert_eq!(emitter.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    struct StaleListEmitter {
        pane: signal_herdr_nexus::Pane,
        list_started: std::sync::mpsc::Sender<()>,
        release_list: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl ExternalEmitter for StaleListEmitter {
        fn emit(
            &self,
            _configuration: &RuntimeConfiguration,
            herdr: emission_herdr_nexus::Herdr,
        ) -> EmissionResult {
            use emission_herdr_nexus::{ExternalOutcome, PaneObservation, Verb};
            EmissionResult::External(match herdr.verb {
                Verb::WorkspaceCreate(_) => ExternalOutcome::Created(self.pane.clone()),
                Verb::AgentStart(_) => ExternalOutcome::Started(signal_herdr_nexus::State::Busy),
                Verb::PaneList => {
                    self.list_started.send(()).expect("announce list");
                    self.release_list
                        .lock()
                        .expect("release lock")
                        .recv()
                        .expect("release list");
                    ExternalOutcome::Listed(vec![PaneObservation {
                        pane: self.pane.clone(),
                        state: signal_herdr_nexus::State::Open,
                    }])
                }
                _ => ExternalOutcome::Done,
            })
        }
    }

    #[test]
    fn stale_list_finishing_after_start_preserves_started_state() {
        use signal_herdr_nexus::{
            AgentKind, AgentName, Command, Executable, Label, Name, Program, Query, Root,
            Start_Data, Value, Variable, Workspace,
        };
        let pane = signal_herdr_nexus::Pane {
            session: signal_herdr_nexus::Session("race".into()),
            pane_id: signal_herdr_nexus::PaneId("pane".into()),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let emitter = Arc::new(StaleListEmitter {
            pane: pane.clone(),
            list_started: started_tx,
            release_list: std::sync::Mutex::new(release_rx),
        });
        let dispatcher = Arc::new(Dispatcher::with_emitter(&test_paths(), emitter));
        let configuration = test_configuration();
        let created = dispatcher.dispatch(
            Query::Create(Workspace {
                session: pane.session.clone(),
                root: Root("/tmp".into()),
                variable_vector: vec![Variable {
                    name: Name("A".into()),
                    value: Value("B".into()),
                }],
            }),
            Arc::clone(&configuration),
        );
        assert!(matches!(
            created,
            signal_herdr_nexus::Response::Created(signal_herdr_nexus::Outcome::Created(_))
        ));

        let listing_dispatcher = Arc::clone(&dispatcher);
        let listing_configuration = Arc::clone(&configuration);
        let session = pane.session.clone();
        let listing = std::thread::spawn(move || {
            listing_dispatcher.dispatch(Query::List(session), listing_configuration)
        });
        started_rx
            .recv()
            .expect("list entered external observation");
        let started = dispatcher.dispatch(
            Query::Start(Start_Data {
                pane: pane.clone(),
                command: Command {
                    agent_name: AgentName("probe".into()),
                    agent_kind: AgentKind::Codex,
                    executable: Executable::Explicit(Program("/bin/true".into())),
                    string_vector: Vec::new(),
                },
                label: Label("flow".into()),
            }),
            configuration,
        );
        assert!(matches!(
            started,
            signal_herdr_nexus::Response::Started(signal_herdr_nexus::Outcome::Done)
        ));
        release_tx.send(()).expect("release stale list");
        let listed = listing.join().expect("listing thread");
        let signal_herdr_nexus::Response::Listed(signal_herdr_nexus::Outcome::Listed(known)) =
            listed
        else {
            panic!("complete list");
        };
        assert_eq!(known.len(), 1);
        assert_eq!(known[0].state, signal_herdr_nexus::State::Busy);
    }

    #[test]
    fn transport_timeout_must_exceed_start_timeout() {
        let store = ConfigurationStore::default();
        let _ = store.configure(
            core_meta_herdr_nexus::CoreSet::StartTimeout(meta_signal_herdr_nexus::Timeout(50)),
            || Ok(()),
        );
        let response = store.configure(
            core_meta_herdr_nexus::CoreSet::TransportTimeout(meta_signal_herdr_nexus::Timeout(50)),
            || Ok(()),
        );
        assert!(matches!(
            response,
            core_meta_herdr_nexus::Response::ConfigureRefused(_)
        ));
    }

    struct SubscriptionEmitter {
        pane: signal_herdr_nexus::Pane,
        listings:
            std::sync::Mutex<std::collections::VecDeque<emission_herdr_nexus::ExternalOutcome>>,
        list_count: std::sync::atomic::AtomicUsize,
        list_start_timeouts: std::sync::Mutex<Vec<Duration>>,
    }

    impl ExternalEmitter for SubscriptionEmitter {
        fn emit(
            &self,
            configuration: &RuntimeConfiguration,
            herdr: emission_herdr_nexus::Herdr,
        ) -> EmissionResult {
            use emission_herdr_nexus::{ExternalOutcome, Verb};
            EmissionResult::External(match herdr.verb {
                Verb::WorkspaceCreate(_) => ExternalOutcome::Created(self.pane.clone()),
                Verb::AgentStart(_) => ExternalOutcome::Started(signal_herdr_nexus::State::Busy),
                Verb::PaneList => {
                    self.list_start_timeouts
                        .lock()
                        .expect("list configurations")
                        .push(configuration.start_timeout);
                    self.list_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    self.listings
                        .lock()
                        .expect("listings")
                        .pop_front()
                        .unwrap_or(ExternalOutcome::Listed(vec![
                            emission_herdr_nexus::PaneObservation {
                                pane: self.pane.clone(),
                                state: signal_herdr_nexus::State::Open,
                            },
                        ]))
                }
                _ => ExternalOutcome::Done,
            })
        }
    }

    fn started_dispatcher(
        listings: Vec<emission_herdr_nexus::ExternalOutcome>,
    ) -> (
        Arc<Dispatcher>,
        Arc<SubscriptionEmitter>,
        signal_herdr_nexus::Pane,
    ) {
        use signal_herdr_nexus::{
            AgentKind, AgentName, Command, Executable, Label, Program, Query, Root, Start_Data,
            Workspace,
        };
        let pane = signal_herdr_nexus::Pane {
            session: signal_herdr_nexus::Session("subscription".into()),
            pane_id: signal_herdr_nexus::PaneId("pane".into()),
        };
        let emitter = Arc::new(SubscriptionEmitter {
            pane: pane.clone(),
            listings: std::sync::Mutex::new(listings.into()),
            list_count: std::sync::atomic::AtomicUsize::new(0),
            list_start_timeouts: std::sync::Mutex::new(Vec::new()),
        });
        let dispatcher = Arc::new(Dispatcher::with_emitter(&test_paths(), emitter.clone()));
        let configuration = test_configuration();
        let created = dispatcher.dispatch(
            Query::Create(Workspace {
                session: pane.session.clone(),
                root: Root("/tmp".into()),
                variable_vector: Vec::new(),
            }),
            Arc::clone(&configuration),
        );
        assert!(matches!(
            created,
            signal_herdr_nexus::Response::Created(signal_herdr_nexus::Outcome::Created(_))
        ));
        let started = dispatcher.dispatch(
            Query::Start(Start_Data {
                pane: pane.clone(),
                command: Command {
                    agent_name: AgentName("probe".into()),
                    agent_kind: AgentKind::Codex,
                    executable: Executable::Explicit(Program("/bin/true".into())),
                    string_vector: Vec::new(),
                },
                label: Label("flow".into()),
            }),
            configuration,
        );
        assert!(matches!(
            started,
            signal_herdr_nexus::Response::Started(signal_herdr_nexus::Outcome::Done)
        ));
        (dispatcher, emitter, pane)
    }

    fn read_subscription(
        stream: &mut std::os::unix::net::UnixStream,
    ) -> herdr_nexus_wire::public_call::Response {
        use signal::{FrameReading, Restorable, Signal};
        let body = stream
            .read_frame(signal::FrameCapacity::from(4096))
            .expect("subscription frame");
        Signal::<herdr_nexus_wire::public_call::Response>::from(Vec::from(body))
            .restore()
            .expect("typed response")
    }

    #[test]
    fn subscription_acks_then_reports_current_and_one_harness_loss() {
        use emission_herdr_nexus::{ExternalOutcome, PaneObservation};
        let (dispatcher, _emitter, pane) = started_dispatcher(vec![
            ExternalOutcome::Uncertain,
            ExternalOutcome::Listed(vec![PaneObservation {
                pane: pane_for("subscription", "pane"),
                state: signal_herdr_nexus::State::Open,
            }]),
        ]);
        let (server, mut client) = std::os::unix::net::UnixStream::pair().expect("pair");
        client
            .set_read_timeout(Some(Duration::from_millis(250)))
            .expect("timeout");
        dispatcher
            .subscribe(server, pane.session.clone(), test_configuration())
            .expect("subscribe");
        assert!(
            matches!(read_subscription(&mut client), herdr_nexus_wire::public_call::Response::Answered(ref output) if matches!(output.0, signal_herdr_nexus::Response::Subscribed(signal_herdr_nexus::Outcome::Done)))
        );
        assert!(
            matches!(read_subscription(&mut client), herdr_nexus_wire::public_call::Response::Answered(ref output) if matches!(&output.0, signal_herdr_nexus::Response::Changed(change) if change.pane == pane && change.state == signal_herdr_nexus::State::Busy))
        );
        assert!(
            matches!(read_subscription(&mut client), herdr_nexus_wire::public_call::Response::Answered(ref output) if matches!(&output.0, signal_herdr_nexus::Response::Changed(change) if change.pane == pane && change.state == signal_herdr_nexus::State::Dead))
        );
        client
            .set_read_timeout(Some(Duration::from_millis(35)))
            .expect("short timeout");
        use signal::FrameReading;
        assert!(
            client
                .read_frame(signal::FrameCapacity::from(4096))
                .is_err(),
            "repeated loss must not duplicate Dead"
        );
    }

    fn pane_for(session: &str, pane: &str) -> signal_herdr_nexus::Pane {
        signal_herdr_nexus::Pane {
            session: signal_herdr_nexus::Session(session.into()),
            pane_id: signal_herdr_nexus::PaneId(pane.into()),
        }
    }

    #[test]
    fn two_subscribers_share_one_session_poll_source() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let (server_one, _client_one) = std::os::unix::net::UnixStream::pair().expect("pair one");
        let (server_two, _client_two) = std::os::unix::net::UnixStream::pair().expect("pair two");
        dispatcher
            .subscribe(server_one, pane.session.clone(), test_configuration())
            .expect("first");
        dispatcher
            .subscribe(server_two, pane.session.clone(), test_configuration())
            .expect("second");
        assert_eq!(dispatcher.subscription_source_count(), 1);
        std::thread::sleep(Duration::from_millis(35));
        let polls = emitter.list_count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            (1..=5).contains(&polls),
            "one shared source should poll at one cadence, got {polls}"
        );
    }

    #[test]
    fn late_subscriber_cursor_does_not_repeat_its_current_state() {
        use emission_herdr_nexus::{ExternalOutcome, PaneObservation};
        use signal_herdr_nexus::{AgentKind, AgentName, Command, Executable, Label, Program, Query, Start_Data};
        let pane = pane_for("subscription", "pane");
        let (dispatcher, _emitter, _) = started_dispatcher(vec![
            ExternalOutcome::Listed(vec![PaneObservation { pane: pane.clone(), state: signal_herdr_nexus::State::Idle }]),
            ExternalOutcome::Listed(vec![PaneObservation { pane: pane.clone(), state: signal_herdr_nexus::State::Busy }]),
        ]);
        let slow = Arc::new(RuntimeConfiguration {
            polling: Duration::from_millis(100),
            start_timeout: Duration::from_millis(20),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(30),
            subscriber_capacity: 8,
        });
        *dispatcher.configuration.current.write().expect("configuration") = Some(Arc::clone(&slow));
        let (server_a, mut client_a) = std::os::unix::net::UnixStream::pair().expect("pair a");
        client_a.set_read_timeout(Some(Duration::from_millis(300))).expect("timeout a");
        dispatcher.subscribe(server_a, pane.session.clone(), Arc::clone(&slow)).expect("subscribe a");
        let _ = read_subscription(&mut client_a); // acknowledgement
        let _ = read_subscription(&mut client_a); // current Busy
        assert!(matches!(read_subscription(&mut client_a), herdr_nexus_wire::public_call::Response::Answered(ref output) if matches!(&output.0, signal_herdr_nexus::Response::Changed(change) if change.state == signal_herdr_nexus::State::Idle)));

        let started = dispatcher.dispatch(Query::Start(Start_Data {
            pane: pane.clone(),
            command: Command {
                agent_name: AgentName("probe".into()),
                agent_kind: AgentKind::Codex,
                executable: Executable::Explicit(Program("/bin/true".into())),
                string_vector: Vec::new(),
            },
            label: Label("flow".into()),
        }), Arc::clone(&slow));
        assert!(matches!(started, signal_herdr_nexus::Response::Started(signal_herdr_nexus::Outcome::Done)));
        let (server_b, mut client_b) = std::os::unix::net::UnixStream::pair().expect("pair b");
        client_b.set_read_timeout(Some(Duration::from_millis(300))).expect("timeout b");
        dispatcher.subscribe(server_b, pane.session.clone(), Arc::clone(&slow)).expect("subscribe b");
        let _ = read_subscription(&mut client_b); // acknowledgement
        assert!(matches!(read_subscription(&mut client_b), herdr_nexus_wire::public_call::Response::Answered(ref output) if matches!(&output.0, signal_herdr_nexus::Response::Changed(change) if change.state == signal_herdr_nexus::State::Busy)));
        assert!(matches!(read_subscription(&mut client_a), herdr_nexus_wire::public_call::Response::Answered(ref output) if matches!(&output.0, signal_herdr_nexus::Response::Changed(change) if change.state == signal_herdr_nexus::State::Busy)));
        client_b.set_read_timeout(Some(Duration::from_millis(35))).expect("short timeout b");
        use signal::FrameReading;
        assert!(client_b.read_frame(signal::FrameCapacity::from(4096)).is_err(), "late subscriber must not receive its current Busy twice");
    }

    #[test]
    fn active_subscription_reads_replaced_configuration() {
        let (dispatcher, emitter, pane) = started_dispatcher(Vec::new());
        let (server, _client) = std::os::unix::net::UnixStream::pair().expect("pair");
        dispatcher.subscribe(server, pane.session, test_configuration()).expect("subscribe");
        while emitter.list_count.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            std::thread::sleep(Duration::from_millis(2));
        }
        let replacement = Arc::new(RuntimeConfiguration {
            polling: Duration::from_millis(10),
            start_timeout: Duration::from_millis(25),
            frame_capacity_bytes: 4096,
            transport_timeout: Duration::from_millis(40),
            subscriber_capacity: 8,
        });
        *dispatcher.configuration.current.write().expect("configuration") = Some(replacement);
        let deadline = Instant::now() + Duration::from_millis(300);
        while !emitter.list_start_timeouts.lock().expect("configurations")
            .iter().any(|timeout| *timeout == Duration::from_millis(25))
        {
            assert!(Instant::now() < deadline, "subscription did not adopt replaced configuration");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
