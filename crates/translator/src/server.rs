use crate::runner::{self, Captured};
use emission_herdr_nexus::{
    AgentListing, AgentPrompt_Data, AgentStart_Data, ExternalOutcome, Herdr, Keys_Data,
    PaneListing, TabId, TerminalId, Verb, WorkspaceCreate_Data, WorkspaceId,
};
use serde::Deserialize;
use shared_translator::placement::Place;
use shared_translator::{ByteViewable, FrameCapacity, Restorable, Signal, Signalizable};
use signal_herdr_nexus::{
    AgentKind, ErrorCode, Executable, ExitStatus, Message, Pane, PaneId, Refusal, Session, State,
};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAXIMUM_SIGNAL_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy)]
pub enum Endpoint {
    Public,
    Meta,
}

pub fn run(
    bootstrap: &herdr_nexus_wire::transport_types::TranslatorBootstrap,
) -> Result<(), ServerError> {
    let place = &bootstrap.place;
    let admission = herdr_nexus_wire::ValidAdmission::try_from(&bootstrap.admission)
        .map_err(|message| ServerError::Signal(message.into()))?;
    let bootstrap_budget = validated_budget(&bootstrap.budget_value)
        .ok_or_else(|| ServerError::Signal("invalid bootstrap text budget".into()))?;
    let names = herdr_nexus_wire::socket_names();
    let public =
        OwnedSocket::bind(Path::new(&place.directory.0).join(names.public_translator_socket.0))?;
    let meta =
        OwnedSocket::bind(Path::new(&place.directory.0).join(names.meta_translator_socket.0))?;
    let configuration = std::sync::Arc::new(TranslatorConfiguration::default());
    let nexus_peer = bootstrap.peer_identity.clone();
    let meta_peer = nexus_peer.clone();
    let meta_configuration = std::sync::Arc::clone(&configuration);
    let meta_place = place.clone();
    std::thread::spawn(move || {
        serve_listener(
            meta,
            meta_configuration,
            meta_place,
            Endpoint::Meta,
            admission,
            bootstrap_budget,
            meta_peer,
        )
    });
    serve_listener(
        public,
        configuration,
        place.clone(),
        Endpoint::Public,
        admission,
        bootstrap_budget,
        nexus_peer,
    );
    Ok(())
}

fn serve_listener(
    bound: OwnedSocket,
    configuration: std::sync::Arc<TranslatorConfiguration>,
    place: Place,
    endpoint: Endpoint,
    admission: herdr_nexus_wire::ValidAdmission,
    bootstrap_budget: (usize, usize),
    nexus_peer: herdr_nexus_wire::transport_types::PeerIdentity,
) {
    for stream in bound.listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let configuration = std::sync::Arc::clone(&configuration);
        let place = place.clone();
        let nexus_peer = nexus_peer.clone();
        std::thread::spawn(move || {
            if let Err(error) = serve_one_paired(
                &mut stream,
                &configuration,
                &place,
                endpoint,
                admission,
                bootstrap_budget,
                Some(&nexus_peer),
            ) {
                eprintln!("herdr translator request failed: {error}");
            }
        });
    }
}

pub fn serve_one(
    stream: &mut UnixStream,
    configuration: &TranslatorConfiguration,
    place: &Place,
    endpoint: Endpoint,
    admission: herdr_nexus_wire::ValidAdmission,
    bootstrap_budget: (usize, usize),
) -> Result<(), ServerError> {
    serve_one_paired(
        stream,
        configuration,
        place,
        endpoint,
        admission,
        bootstrap_budget,
        None,
    )
}

fn serve_one_paired(
    stream: &mut UnixStream,
    configuration: &TranslatorConfiguration,
    place: &Place,
    endpoint: Endpoint,
    admission: herdr_nexus_wire::ValidAdmission,
    bootstrap_budget: (usize, usize),
    nexus_peer: Option<&herdr_nexus_wire::transport_types::PeerIdentity>,
) -> Result<(), ServerError> {
    use crate::transport::{self, Frame};
    use crate::transport_types::{Kind, Lane};
    let caller = crate::transport::peer_caller(stream).map_err(ServerError::Transport)?;
    let mut io = herdr_nexus_wire::deadline::DeadlineStream::new(
        stream.try_clone().map_err(ServerError::Io)?,
        admission.deadline,
    )
    .map_err(ServerError::Io)?;
    let request =
        transport::read_frame(&mut io, admission.frame_capacity).map_err(ServerError::Transport)?;
    if request.kind != Kind::Request {
        return Err(ServerError::Transport(
            crate::transport::TransportError::InvalidLength,
        ));
    }
    let lane = request.lane.clone();
    let allowed = match endpoint {
        Endpoint::Public => matches!(&lane, Lane::PublicText | Lane::EmissionSignal),
        Endpoint::Meta => lane == Lane::MetaText,
    };
    if !allowed {
        return write_failed_frame(
            &mut io,
            lane,
            "lane is unavailable on this endpoint",
            admission.frame_capacity,
        );
    }
    if caller.user_id.0 != i64::from(nix::unistd::getuid().as_raw()) {
        return write_failed_frame(
            &mut io,
            lane,
            "caller is outside the bound login",
            admission.frame_capacity,
        );
    }
    if request.lane == Lane::EmissionSignal
        && !nexus_peer.is_some_and(|expected| {
            caller.user_id == expected.user_id && caller.process_id == expected.process_id
        })
    {
        return write_failed_frame(
            &mut io,
            lane,
            "caller is not the paired Nexus",
            admission.frame_capacity,
        );
    }
    if request.lane == Lane::PublicText {
        let Some((mut budget, timeout)) = configuration.text_context() else {
            return write_failed_frame(
                &mut io,
                lane,
                "translator is awaiting settings",
                admission.frame_capacity,
            );
        };
        let query = (|| -> Result<_, ServerError> {
            let text = request.text().map_err(ServerError::Transport)?;
            decode_public(text, &mut budget)
        })();
        let query = match query {
            Ok(query) => query,
            Err(error) => {
                drop(io);
                let mut output = herdr_nexus_wire::deadline::DeadlineStream::new(
                    stream.try_clone().map_err(ServerError::Io)?, timeout,
                ).map_err(ServerError::Io)?;
                return write_failed_frame(&mut output, lane, &error.to_string(), admission.frame_capacity);
            }
        };
        if matches!(query, signal_herdr_nexus::Query::Subscribe(_)) {
            drop(io);
            return forward_public_stream(
                stream,
                configuration,
                place,
                caller,
                query,
                admission.frame_capacity,
            );
        }
        let response = match forward_public(place, caller, query, timeout) {
            Ok(response) => Frame {
                lane: Lane::PublicText,
                kind: Kind::Answered,
                payload: shared_translator::encode(&response).into_bytes(),
            },
            Err(error) => Frame {
                lane: Lane::PublicText,
                kind: Kind::Failed,
                payload: error.to_string().into_bytes(),
            },
        };
        drop(io);
        let mut output = herdr_nexus_wire::deadline::DeadlineStream::new(
            stream.try_clone().map_err(ServerError::Io)?, timeout,
        ).map_err(ServerError::Io)?;
        return transport::write_frame(&mut output, &response, admission.frame_capacity)
            .map_err(ServerError::Transport);
    }
    let response = (|| -> Result<Frame, ServerError> {
        Ok(match request.lane {
            Lane::EmissionSignal => {
                let query = Signal::<emission_herdr_nexus::Query>::from(request.payload)
                    .restore()
                    .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
                let Some(runtime) = configuration.runtime() else {
                    return Err(ServerError::Signal(
                        "translator is awaiting settings".into(),
                    ));
                };
                let emission_herdr_nexus::Query::Emit(herdr) = query;
                let response = emission_herdr_nexus::Response::Emitted(execute(&runtime, &herdr))
                    .signalize()
                    .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
                Frame {
                    lane: Lane::EmissionSignal,
                    kind: Kind::Answered,
                    payload: response.bytes().to_vec(),
                }
            }
            Lane::MetaText => {
                let text = request.text().map_err(ServerError::Transport)?;
                let response = configure_text(
                    configuration,
                    place,
                    caller.clone(),
                    text,
                    bootstrap_budget,
                    admission.deadline,
                )?;
                Frame {
                    lane: Lane::MetaText,
                    kind: Kind::Answered,
                    payload: response.into_bytes(),
                }
            }
            Lane::PublicText => {
                unreachable!("public text is handled as a potentially streaming request above")
            }
        })
    })();
    let response = match response {
        Ok(response) => response,
        Err(error) => Frame {
            lane,
            kind: Kind::Failed,
            payload: error.to_string().into_bytes(),
        },
    };
    drop(io);
    let response_timeout = configuration.call_timeout().unwrap_or(admission.deadline);
    let mut output = herdr_nexus_wire::deadline::DeadlineStream::new(
        stream.try_clone().map_err(ServerError::Io)?, response_timeout,
    ).map_err(ServerError::Io)?;
    transport::write_frame(&mut output, &response, admission.frame_capacity)
        .map_err(ServerError::Transport)
}

fn write_failed_frame(
    stream: &mut impl std::io::Write,
    lane: crate::transport_types::Lane,
    message: &str,
    admission_limit: usize,
) -> Result<(), ServerError> {
    crate::transport::write_frame(
        stream,
        &crate::transport::Frame {
            lane,
            kind: crate::transport_types::Kind::Failed,
            payload: message.as_bytes().to_vec(),
        },
        admission_limit,
    )
    .map_err(ServerError::Transport)
}

#[derive(Default)]
pub struct TranslatorConfiguration {
    partial: std::sync::RwLock<PartialTranslatorConfiguration>,
}

#[derive(Clone, Default)]
struct PartialTranslatorConfiguration {
    executable: Option<PathBuf>,
    call_timeout: Option<Duration>,
    output_limit: Option<usize>,
    text_budget: Option<(usize, usize)>,
}

impl TranslatorConfiguration {
    fn call_timeout(&self) -> Option<Duration> {
        self.partial.read().ok()?.call_timeout
    }
    fn runtime(&self) -> Option<Runtime> {
        let partial = self.partial.read().ok()?.clone();
        Runtime::new(
            partial.executable?,
            partial.call_timeout?,
            partial.output_limit?,
        )
        .filter(|_| partial.text_budget.is_some())
    }

    #[cfg(feature = "text")]
    fn text_context(&self) -> Option<(datom_codec::Budget, Duration)> {
        let partial = self.partial.read().ok()?.clone();
        let (nodes, depth) = partial.text_budget?;
        Some((
            datom_codec::Budget {
                remaining: i64::try_from(nodes).ok()?,
                reader: protos::ReaderBudget { remaining: nodes },
                depth: 0,
                maximum_depth: i64::try_from(depth).ok()?,
            },
            partial.call_timeout?,
        ))
    }

    fn apply(&self, setting: meta_signal_herdr_nexus::Set) -> meta_signal_herdr_nexus::Response {
        use meta_signal_herdr_nexus::{
            Configured_Data, Owner, Readiness, Response, Set, SettingName,
        };
        let applied = setting.clone();
        let mut partial = self
            .partial
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let invalid = match setting {
            Set::HerdrPath(value) if Path::new(&value.0).is_absolute() => {
                partial.executable = Some(PathBuf::from(value.0));
                None
            }
            Set::CallTimeout(value) => positive_duration(value.0)
                .map(|value| {
                    partial.call_timeout = Some(value);
                })
                .is_none()
                .then_some(SettingName::CallTimeout),
            Set::OutputLimit(value) => usize::try_from(value.0)
                .ok()
                .filter(|value| *value > 0)
                .map(|value| {
                    partial.output_limit = Some(value);
                })
                .is_none()
                .then_some(SettingName::OutputLimit),
            Set::TextBudget(value) => {
                let nodes = usize::try_from(value.node_limit.0)
                    .ok()
                    .filter(|value| *value > 0 && *value <= signal::MAXIMUM_SIGNAL_BYTES);
                let depth = usize::try_from(value.depth_limit.0)
                    .ok()
                    .filter(|value| *value > 0 && *value <= signal::MAXIMUM_SIGNAL_DEPTH);
                match (nodes, depth) {
                    (Some(nodes), Some(depth)) => {
                        partial.text_budget = Some((nodes, depth));
                        None
                    }
                    _ => Some(SettingName::TextBudget),
                }
            }
            Set::HerdrPath(_) => Some(SettingName::HerdrPath),
            other => {
                return Response::ConfigureRefused(invalid_setting(
                    Owner::Translator,
                    setting_name(&other),
                    "setting belongs to Nexus",
                ))
            }
        };
        if let Some(name) = invalid {
            return Response::ConfigureRefused(invalid_setting(
                Owner::Translator,
                name,
                "invalid setting value",
            ));
        }
        let mut awaiting = Vec::new();
        if partial.executable.is_none() {
            awaiting.push(SettingName::HerdrPath);
        }
        if partial.call_timeout.is_none() {
            awaiting.push(SettingName::CallTimeout);
        }
        if partial.output_limit.is_none() {
            awaiting.push(SettingName::OutputLimit);
        }
        if partial.text_budget.is_none() {
            awaiting.push(SettingName::TextBudget);
        }
        Response::Configured(Configured_Data {
            set: applied,
            owner: Owner::Translator,
            readiness: if awaiting.is_empty() {
                Readiness::Ready
            } else {
                Readiness::Awaiting(awaiting)
            },
        })
    }
}

#[cfg(feature = "text")]
fn configure_text(
    configuration: &TranslatorConfiguration,
    place: &Place,
    caller: crate::transport_types::Caller,
    text: &str,
    bootstrap_limits: (usize, usize),
    bootstrap_timeout: Duration,
) -> Result<String, ServerError> {
    use datom_codec::{Actualizing, Potential};
    let mut budget = budget(bootstrap_limits);
    let mut potential = Potential::<meta_signal_herdr_nexus::Query>::from(text);
    let query = potential
        .actualize(&mut budget)
        .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    let meta_signal_herdr_nexus::Query::Configure(setting) = query;
    let response = match setting {
        setting @ (meta_signal_herdr_nexus::Set::HerdrPath(_)
        | meta_signal_herdr_nexus::Set::CallTimeout(_)
        | meta_signal_herdr_nexus::Set::OutputLimit(_)
        | meta_signal_herdr_nexus::Set::TextBudget(_)) => configuration.apply(setting),
        setting => {
            let timeout = configuration.call_timeout().unwrap_or(bootstrap_timeout);
            forward_meta(place, caller, core_setting(setting), timeout)?
        }
    };
    Ok(shared_translator::encode(&response))
}

#[cfg(feature = "text")]
fn budget((nodes, depth): (usize, usize)) -> datom_codec::Budget {
    datom_codec::Budget {
        remaining: i64::try_from(nodes).expect("validated node budget fits i64"),
        reader: protos::ReaderBudget { remaining: nodes },
        depth: 0,
        maximum_depth: i64::try_from(depth).expect("validated depth budget fits i64"),
    }
}

fn validated_budget(value: &meta_signal_herdr_nexus::BudgetValue) -> Option<(usize, usize)> {
    let nodes = usize::try_from(value.node_limit.0)
        .ok()
        .filter(|value| *value > 0 && *value <= signal::MAXIMUM_SIGNAL_BYTES)?;
    let depth = usize::try_from(value.depth_limit.0)
        .ok()
        .filter(|value| *value > 0 && *value <= signal::MAXIMUM_SIGNAL_DEPTH)?;
    Some((nodes, depth))
}

#[cfg(feature = "text")]
fn decode_public(
    text: &str,
    budget: &mut datom_codec::Budget,
) -> Result<signal_herdr_nexus::Query, ServerError> {
    use datom_codec::{Actualizing, Potential};
    let mut potential = Potential::<signal_herdr_nexus::Query>::from(text);
    potential
        .actualize(budget)
        .map_err(|error| ServerError::Signal(format!("{error:?}")))
}

fn core_setting(setting: meta_signal_herdr_nexus::Set) -> core_meta_herdr_nexus::CoreSet {
    match setting {
        meta_signal_herdr_nexus::Set::StartTimeout(value) => {
            core_meta_herdr_nexus::CoreSet::StartTimeout(value)
        }
        meta_signal_herdr_nexus::Set::Polling(value) => {
            core_meta_herdr_nexus::CoreSet::Polling(value)
        }
        meta_signal_herdr_nexus::Set::FrameCapacity(value) => {
            core_meta_herdr_nexus::CoreSet::FrameCapacity(value)
        }
        meta_signal_herdr_nexus::Set::TransportTimeout(value) => {
            core_meta_herdr_nexus::CoreSet::TransportTimeout(value)
        }
        meta_signal_herdr_nexus::Set::SubscriberCapacity(value) => {
            core_meta_herdr_nexus::CoreSet::SubscriberCapacity(value)
        }
        _ => unreachable!("translator settings are handled before core conversion"),
    }
}

fn forward_public(
    place: &Place,
    caller: crate::transport_types::Caller,
    input: signal_herdr_nexus::Query,
    timeout: Duration,
) -> Result<signal_herdr_nexus::Response, ServerError> {
    let request =
        herdr_nexus_wire::public_call::Query::Call(herdr_nexus_wire::public_call::Call_Data {
            caller,
            input: herdr_nexus_wire::public_payload::Input(input),
        });
    let response: herdr_nexus_wire::public_call::Response = binary_call(
        Path::new(&place.directory.0).join("herdr.sock"),
        request,
        timeout,
    )?;
    match response {
        herdr_nexus_wire::public_call::Response::Answered(output) => Ok(output.0),
        herdr_nexus_wire::public_call::Response::Failed(message) => {
            Err(ServerError::Signal(message))
        }
    }
}

fn forward_public_stream(
    client: &UnixStream,
    configuration: &TranslatorConfiguration,
    place: &Place,
    caller: crate::transport_types::Caller,
    input: signal_herdr_nexus::Query,
    admission_limit: usize,
) -> Result<(), ServerError> {
    use shared_translator::{FrameReading, FrameWriting};
    let request =
        herdr_nexus_wire::public_call::Query::Call(herdr_nexus_wire::public_call::Call_Data {
            caller,
            input: herdr_nexus_wire::public_payload::Input(input),
        });
    let core = UnixStream::connect(Path::new(&place.directory.0).join("herdr.sock"))
        .map_err(ServerError::Io)?;
    let timeout = configuration
        .call_timeout()
        .ok_or_else(|| ServerError::Signal("translator is awaiting CallTimeout".into()))?;
    let mut core_write = herdr_nexus_wire::deadline::DeadlineStream::new(
        core.try_clone().map_err(ServerError::Io)?,
        timeout,
    )
    .map_err(ServerError::Io)?;
    let request = request
        .signalize()
        .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    core_write
        .write_frame(&request, FrameCapacity::from(MAXIMUM_SIGNAL_BYTES))
        .map_err(ServerError::Frame)?;
    loop {
        let timeout = configuration
            .call_timeout()
            .ok_or_else(|| ServerError::Signal("translator is awaiting CallTimeout".into()))?;
        let Some(body) = read_subscription_signal_frame(
            core.try_clone().map_err(ServerError::Io)?,
            timeout,
            MAXIMUM_SIGNAL_BYTES,
        )? else {
            return Ok(());
        };
        let response = Signal::<herdr_nexus_wire::public_call::Response>::from(body)
            .restore()
            .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
        let response = match response {
            herdr_nexus_wire::public_call::Response::Answered(output) => output.0,
            herdr_nexus_wire::public_call::Response::Failed(message) => {
                return Err(ServerError::Signal(message));
            }
        };
        let frame = crate::transport::Frame {
            lane: crate::transport_types::Lane::PublicText,
            kind: crate::transport_types::Kind::Answered,
            payload: shared_translator::encode(&response).into_bytes(),
        };
        let mut client_write = herdr_nexus_wire::deadline::DeadlineStream::new(
            client.try_clone().map_err(ServerError::Io)?,
            timeout,
        )
        .map_err(ServerError::Io)?;
        crate::transport::write_frame(&mut client_write, &frame, admission_limit)
            .map_err(ServerError::Transport)?;
    }
}

/// A subscription may remain quiet for its whole useful lifetime.  The first
/// byte therefore has no idle deadline.  Once a frame starts, the remaining
/// prefix and body share one absolute deadline, so a partial or drip-fed frame
/// cannot retain the connection indefinitely.
fn read_subscription_signal_frame(
    mut stream: UnixStream,
    progress_timeout: Duration,
    capacity: usize,
) -> Result<Option<Vec<u8>>, ServerError> {
    use std::io::Read;

    stream.set_read_timeout(None).map_err(ServerError::Io)?;
    let mut prefix = [0_u8; 4];
    match stream.read(&mut prefix[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("a one-byte buffer yields at most one byte"),
        Err(error) => return Err(ServerError::Io(error)),
    }
    let mut progressing = herdr_nexus_wire::deadline::DeadlineStream::new(stream, progress_timeout)
        .map_err(ServerError::Io)?;
    progressing
        .read_exact(&mut prefix[1..])
        .map_err(ServerError::Io)?;
    let length = usize::try_from(u32::from_be_bytes(prefix))
        .map_err(|_| ServerError::Signal("subscription frame length is not representable".into()))?;
    if length > capacity {
        return Err(ServerError::Signal(format!(
            "subscription frame length {length} exceeds capacity {capacity}"
        )));
    }
    let mut body = vec![0_u8; length];
    progressing.read_exact(&mut body).map_err(ServerError::Io)?;
    Ok(Some(body))
}

fn forward_meta(
    place: &Place,
    caller: crate::transport_types::Caller,
    input: core_meta_herdr_nexus::CoreSet,
    timeout: Duration,
) -> Result<meta_signal_herdr_nexus::Response, ServerError> {
    let request =
        herdr_nexus_wire::meta_call::Query::Call(herdr_nexus_wire::meta_call::Call_Data {
            caller,
            input: herdr_nexus_wire::meta_payload::Input(core_meta_herdr_nexus::Query::Configure(
                input,
            )),
        });
    let response: herdr_nexus_wire::meta_call::Response = binary_call(
        Path::new(&place.directory.0).join("herdr-meta.sock"),
        request,
        timeout,
    )?;
    match response {
        herdr_nexus_wire::meta_call::Response::Answered(output) => match output.0 {
            core_meta_herdr_nexus::Response::Configured(configured) => {
                Ok(meta_signal_herdr_nexus::Response::Configured(
                    meta_signal_herdr_nexus::Configured_Data {
                        set: public_setting(configured.core_set),
                        owner: meta_signal_herdr_nexus::Owner::Nexus,
                        readiness: configured.readiness,
                    },
                ))
            }
            core_meta_herdr_nexus::Response::ConfigureRefused(refusal) => {
                Ok(meta_signal_herdr_nexus::Response::ConfigureRefused(refusal))
            }
        },
        herdr_nexus_wire::meta_call::Response::Failed(message) => Err(ServerError::Signal(message)),
    }
}

fn public_setting(setting: core_meta_herdr_nexus::CoreSet) -> meta_signal_herdr_nexus::Set {
    match setting {
        core_meta_herdr_nexus::CoreSet::StartTimeout(value) =>
            meta_signal_herdr_nexus::Set::StartTimeout(value),
        core_meta_herdr_nexus::CoreSet::Polling(value) =>
            meta_signal_herdr_nexus::Set::Polling(value),
        core_meta_herdr_nexus::CoreSet::FrameCapacity(value) =>
            meta_signal_herdr_nexus::Set::FrameCapacity(value),
        core_meta_herdr_nexus::CoreSet::TransportTimeout(value) =>
            meta_signal_herdr_nexus::Set::TransportTimeout(value),
        core_meta_herdr_nexus::CoreSet::SubscriberCapacity(value) =>
            meta_signal_herdr_nexus::Set::SubscriberCapacity(value),
    }
}

fn binary_call<Q, R>(path: PathBuf, query: Q, timeout: Duration) -> Result<R, ServerError>
where
    Q: signal::Signalizable,
    R: rkyv::Archive,
    Signal<R>: shared_translator::Restorable<R>,
{
    use shared_translator::{FrameReading, FrameWriting};
    let stream = UnixStream::connect(path).map_err(ServerError::Io)?;
    let mut stream = herdr_nexus_wire::deadline::DeadlineStream::new(stream, timeout)
        .map_err(ServerError::Io)?;
    let request = query
        .signalize()
        .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    let capacity = FrameCapacity::from(MAXIMUM_SIGNAL_BYTES);
    stream
        .write_frame(&request, capacity)
        .map_err(ServerError::Frame)?;
    let response = stream.read_frame(capacity).map_err(ServerError::Frame)?;
    Signal::<R>::from(Vec::from(response))
        .restore()
        .map_err(|error| ServerError::Signal(format!("{error:?}")))
}

fn setting_name(setting: &meta_signal_herdr_nexus::Set) -> meta_signal_herdr_nexus::SettingName {
    use meta_signal_herdr_nexus::{Set, SettingName};
    match setting {
        Set::HerdrPath(_) => SettingName::HerdrPath,
        Set::CallTimeout(_) => SettingName::CallTimeout,
        Set::OutputLimit(_) => SettingName::OutputLimit,
        Set::TextBudget(_) => SettingName::TextBudget,
        Set::StartTimeout(_) => SettingName::StartTimeout,
        Set::Polling(_) => SettingName::Polling,
        Set::FrameCapacity(_) => SettingName::FrameCapacity,
        Set::TransportTimeout(_) => SettingName::TransportTimeout,
        Set::SubscriberCapacity(_) => SettingName::SubscriberCapacity,
    }
}

fn invalid_setting(
    owner: meta_signal_herdr_nexus::Owner,
    setting_name: meta_signal_herdr_nexus::SettingName,
    message: &str,
) -> meta_signal_herdr_nexus::ConfigurationRefusal {
    meta_signal_herdr_nexus::ConfigurationRefusal::Invalid(meta_signal_herdr_nexus::Invalid_Data {
        owner,
        setting_name,
        message: meta_signal_herdr_nexus::Message(message.into()),
    })
}

/// Executes one typed external emission with a captured configuration snapshot.
pub fn execute(runtime: &Runtime, herdr: &Herdr) -> ExternalOutcome {
    let Some(invocation) = invocation(&herdr.session, &herdr.verb, runtime) else {
        return ExternalOutcome::Uncertain;
    };
    let Some(captured) = run_invocation(runtime, &invocation) else {
        return ExternalOutcome::Uncertain;
    };
    decode(&invocation.decoder, captured, &herdr.session)
}

#[derive(Clone)]
pub struct Runtime {
    pub executable: PathBuf,
    pub call_timeout: Duration,
    pub output_limit: usize,
}

impl Runtime {
    pub fn new(executable: PathBuf, call_timeout: Duration, output_limit: usize) -> Option<Self> {
        if executable.as_os_str().is_empty() || call_timeout.is_zero() || output_limit == 0 {
            return None;
        }
        Some(Self {
            executable,
            call_timeout,
            output_limit,
        })
    }
}

fn positive_duration(milliseconds: i64) -> Option<Duration> {
    let milliseconds = u64::try_from(milliseconds).ok()?;
    (milliseconds != 0).then(|| Duration::from_millis(milliseconds))
}

trait Arguable {
    fn arguments(&self) -> Vec<OsString>;
}

impl Arguable for WorkspaceCreate_Data {
    fn arguments(&self) -> Vec<OsString> {
        let mut arguments = vec![
            OsString::from("workspace"),
            OsString::from("create"),
            OsString::from("--cwd"),
            OsString::from(&self.root.0),
        ];
        for variable in &self.variable_vector {
            arguments.push(OsString::from("--env"));
            arguments.push(OsString::from(format!(
                "{}={}",
                variable.name.0, variable.value.0
            )));
        }
        arguments
    }
}

impl Arguable for AgentStart_Data {
    fn arguments(&self) -> Vec<OsString> {
        let mut arguments = vec![
            OsString::from("agent"),
            OsString::from("start"),
            OsString::from(&self.command.agent_name.0),
            OsString::from("--kind"),
            OsString::from(agent_kind(&self.command.agent_kind)),
            OsString::from("--pane"),
            OsString::from(&self.pane_id.0),
        ];
        if let Executable::Explicit(program) = &self.command.executable {
            arguments.push(OsString::from("--executable"));
            arguments.push(OsString::from(&program.0));
        }
        arguments.push(OsString::from("--timeout"));
        arguments.push(OsString::from(self.timeout.0.to_string()));
        if !self.command.string_vector.is_empty() {
            arguments.push(OsString::from("--"));
            arguments.extend(self.command.string_vector.iter().map(OsString::from));
        }
        arguments
    }
}

impl Arguable for AgentPrompt_Data {
    fn arguments(&self) -> Vec<OsString> {
        vec![
            OsString::from("agent"),
            OsString::from("prompt"),
            OsString::from(&self.pane_id.0),
            OsString::from(&self.text.0),
        ]
    }
}

impl Arguable for Keys_Data {
    fn arguments(&self) -> Vec<OsString> {
        let mut arguments = vec![
            OsString::from("pane"),
            OsString::from("send-keys"),
            OsString::from(&self.pane_id.0),
        ];
        arguments.extend(self.keys.0.iter().map(OsString::from));
        arguments
    }
}

struct PaneCloseArgument<'a>(&'a PaneId);

impl Arguable for PaneCloseArgument<'_> {
    fn arguments(&self) -> Vec<OsString> {
        vec![
            OsString::from("pane"),
            OsString::from("close"),
            OsString::from(&self.0.0),
        ]
    }
}

struct PaneListArgument;

impl Arguable for PaneListArgument {
    fn arguments(&self) -> Vec<OsString> {
        vec![OsString::from("pane"), OsString::from("list")]
    }
}

struct AgentListArgument;

impl Arguable for AgentListArgument {
    fn arguments(&self) -> Vec<OsString> {
        vec![OsString::from("agent"), OsString::from("list")]
    }
}

struct Invocation {
    arguments: Vec<OsString>,
    timeout: Duration,
    decoder: Decoder,
}

enum Decoder {
    WorkspaceCreate,
    AgentStart {
        pane_id: String,
        name: String,
        kind: &'static str,
    },
    AgentPrompt {
        pane_id: String,
    },
    Keys,
    PaneClose,
    PaneList,
    AgentList,
}

fn invocation(session: &Session, verb: &Verb, runtime: &Runtime) -> Option<Invocation> {
    let mut arguments = vec![OsString::from("--session"), OsString::from(&session.0)];
    let (specific, timeout, decoder) = match verb {
        Verb::WorkspaceCreate(data) => (data.arguments(), runtime.call_timeout, Decoder::WorkspaceCreate),
        Verb::AgentStart(data) => {
            let start_timeout = positive_duration(data.timeout.0)?;
            let kind = agent_kind(&data.command.agent_kind);
            (
                data.arguments(),
                start_timeout,
                Decoder::AgentStart {
                    pane_id: data.pane_id.0.clone(),
                    name: data.command.agent_name.0.clone(),
                    kind,
                },
            )
        }
        Verb::AgentPrompt(data) => (
            data.arguments(),
            runtime.call_timeout,
            Decoder::AgentPrompt {
                pane_id: data.pane_id.0.clone(),
            },
        ),
        Verb::Keys(data) => (data.arguments(), runtime.call_timeout, Decoder::Keys),
        Verb::PaneClose(pane_id) => (
            PaneCloseArgument(pane_id).arguments(),
            runtime.call_timeout,
            Decoder::PaneClose,
        ),
        Verb::PaneList => (
            PaneListArgument.arguments(),
            runtime.call_timeout,
            Decoder::PaneList,
        ),
        Verb::AgentList => (
            AgentListArgument.arguments(),
            runtime.call_timeout,
            Decoder::AgentList,
        ),
    };
    arguments.extend(specific);
    Some(Invocation {
        arguments,
        timeout,
        decoder,
    })
}

fn run_invocation(runtime: &Runtime, invocation: &Invocation) -> Option<Captured> {
    let deadline = Instant::now().checked_add(invocation.timeout)?;
    match runner::run(
        &runtime.executable,
        &invocation.arguments,
        deadline,
        runtime.output_limit,
    ) {
        Ok(captured) => Some(captured),
        Err(error) => {
            eprintln!("herdr client execution failed: {error}");
            None
        }
    }
}

fn decode(decoder: &Decoder, captured: Captured, session: &Session) -> ExternalOutcome {
    if !captured.status.success() {
        return refusal_or_uncertain(&captured, decoder.response_id());
    }
    match decoder {
        Decoder::WorkspaceCreate => decode_workspace(&captured.stdout, session),
        Decoder::AgentStart {
            pane_id,
            name,
            kind,
        } => decode_start(&captured.stdout, pane_id, name, kind),
        Decoder::AgentPrompt { pane_id } => decode_prompt(&captured.stdout, pane_id),
        // Herdr's send-keys CLI deliberately emits no success JSON.
        Decoder::Keys if captured.stdout.is_empty() => ExternalOutcome::Done,
        Decoder::Keys => ExternalOutcome::Uncertain,
        Decoder::PaneClose => decode_ok(&captured.stdout, Some("cli:pane:close")),
        Decoder::PaneList => decode_panes(&captured.stdout, session),
        Decoder::AgentList => decode_agents(&captured.stdout, session),
    }
}

impl Decoder {
    fn response_id(&self) -> &'static str {
        match self {
            Self::WorkspaceCreate => "cli:workspace:create",
            Self::AgentStart { .. } => "cli:agent:start",
            Self::AgentPrompt { .. } => "cli:agent:prompt",
            Self::Keys => "cli:pane:send-keys",
            Self::PaneClose => "cli:pane:close",
            Self::PaneList => "cli:pane:list",
            Self::AgentList => "cli:agent:list",
        }
    }
}

fn refusal_or_uncertain(captured: &Captured, expected_id: &str) -> ExternalOutcome {
    // Herdr's failure contract is one JSON envelope on stderr. Stdout is the
    // success channel and is never tried as a fallback failure language.
    let envelope = serde_json::from_slice::<ErrorEnvelope>(&captured.stderr);
    let Ok(envelope) = envelope else {
        eprintln!(
            "herdr refusal was not its stderr JSON contract (status={:?}, stdout_bytes={}, stderr_bytes={})",
            captured.status.code(),
            captured.stdout.len(),
            captured.stderr.len(),
        );
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != expected_id {
        return ExternalOutcome::Uncertain;
    }
    ExternalOutcome::Refused(Refusal {
        error_code: ErrorCode(envelope.error.code),
        exit_status_option: captured
            .status
            .code()
            .map(|code| ExitStatus(i64::from(code))),
        message: Message(envelope.error.message),
    })
}

#[derive(Deserialize)]
struct Envelope<T> {
    id: String,
    result: T,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    #[allow(dead_code)]
    id: String,
    error: ErrorBody,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[derive(Clone, Deserialize)]
struct PaneJson {
    pane_id: String,
    terminal_id: String,
    workspace_id: String,
    tab_id: String,
    #[allow(dead_code)]
    focused: bool,
    #[allow(dead_code)]
    agent_status: AgentStatus,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Clone, Deserialize)]
struct AgentJson {
    terminal_id: String,
    name: Option<String>,
    agent: Option<String>,
    agent_status: AgentStatus,
    workspace_id: String,
    tab_id: String,
    pane_id: String,
    #[allow(dead_code)]
    focused: bool,
    #[serde(default)]
    interactive_ready: bool,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

#[derive(Deserialize)]
struct WorkspaceResult {
    #[serde(rename = "type")]
    kind: String,
    #[allow(dead_code)]
    workspace: serde_json::Value,
    #[allow(dead_code)]
    tab: serde_json::Value,
    root_pane: PaneJson,
}

#[derive(Deserialize)]
struct StartResult {
    #[serde(rename = "type")]
    kind: String,
    agent: AgentJson,
    #[allow(dead_code)]
    argv: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    executable: Option<String>,
}

#[derive(Deserialize)]
struct PromptResult {
    #[serde(rename = "type")]
    kind: String,
    agent: AgentJson,
}

#[derive(Deserialize)]
struct OkResult {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct PaneListResult {
    #[serde(rename = "type")]
    kind: String,
    panes: Vec<PaneJson>,
}

#[derive(Deserialize)]
struct AgentListResult {
    #[serde(rename = "type")]
    kind: String,
    agents: Vec<AgentJson>,
}

fn decode_workspace(bytes: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<WorkspaceResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:workspace:create" || envelope.result.kind != "workspace_created" {
        return ExternalOutcome::Uncertain;
    }
    ExternalOutcome::Created(Pane {
        session: session.clone(),
        pane_id: PaneId(envelope.result.root_pane.pane_id),
    })
}

fn decode_start(bytes: &[u8], pane_id: &str, name: &str, kind: &str) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<StartResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    let result = envelope.result;
    if envelope.id != "cli:agent:start"
        || result.kind != "agent_started"
        || result.agent.pane_id != pane_id
        || result.agent.name.as_deref() != Some(name)
        || result.agent.agent.as_deref() != Some(kind)
        || !result.agent.interactive_ready
    {
        return ExternalOutcome::Uncertain;
    }
    ExternalOutcome::Started(agent_state(&result.agent))
}

fn decode_prompt(bytes: &[u8], pane_id: &str) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<PromptResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id == "cli:agent:prompt"
        && envelope.result.kind == "agent_prompted"
        && envelope.result.agent.pane_id == pane_id
    {
        ExternalOutcome::Done
    } else {
        ExternalOutcome::Uncertain
    }
}

fn decode_ok(bytes: &[u8], expected_id: Option<&str>) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<OkResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.result.kind == "ok" && expected_id.is_none_or(|expected| envelope.id == expected) {
        ExternalOutcome::Done
    } else {
        ExternalOutcome::Uncertain
    }
}

fn decode_panes(bytes: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<PaneListResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:pane:list" || envelope.result.kind != "pane_list" {
        return ExternalOutcome::Uncertain;
    }
    let mut pane_ids = HashSet::new();
    for pane in &envelope.result.panes {
        if !pane_ids.insert(pane.pane_id.as_str()) {
            return ExternalOutcome::Uncertain;
        }
    }
    ExternalOutcome::Panes(
        envelope
            .result
            .panes
            .into_iter()
            .map(|pane| PaneListing {
                pane: Pane {
                    session: session.clone(),
                    pane_id: PaneId(pane.pane_id),
                },
                terminal_id: TerminalId(pane.terminal_id),
                workspace_id: WorkspaceId(pane.workspace_id),
                tab_id: TabId(pane.tab_id),
            })
            .collect(),
    )
}

fn decode_agents(bytes: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(envelope) = serde_json::from_slice::<Envelope<AgentListResult>>(bytes) else {
        return ExternalOutcome::Uncertain;
    };
    if envelope.id != "cli:agent:list" || envelope.result.kind != "agent_list" {
        return ExternalOutcome::Uncertain;
    }
    let mut pane_ids = HashSet::new();
    let mut listings = Vec::with_capacity(envelope.result.agents.len());
    for agent in envelope.result.agents {
        if !pane_ids.insert(agent.pane_id.clone()) {
            return ExternalOutcome::Uncertain;
        }
        let state = agent_state(&agent);
        listings.push(AgentListing {
            pane: Pane {
                session: session.clone(),
                pane_id: PaneId(agent.pane_id),
            },
            terminal_id: TerminalId(agent.terminal_id),
            workspace_id: WorkspaceId(agent.workspace_id),
            tab_id: TabId(agent.tab_id),
            state,
        });
    }
    ExternalOutcome::Agents(listings)
}

fn agent_state(agent: &AgentJson) -> State {
    match agent.agent_status {
        AgentStatus::Working | AgentStatus::Blocked => State::Busy,
        AgentStatus::Idle | AgentStatus::Done if agent.interactive_ready => State::Idle,
        AgentStatus::Idle | AgentStatus::Done | AgentStatus::Unknown => State::Open,
    }
}

fn agent_kind(kind: &AgentKind) -> &'static str {
    match kind {
        AgentKind::Pi => "pi",
        AgentKind::Claude => "claude",
        AgentKind::Codex => "codex",
        AgentKind::Gemini => "gemini",
        AgentKind::Cursor => "cursor",
        AgentKind::Devin => "devin",
        AgentKind::Agy => "agy",
        AgentKind::Cline => "cline",
        AgentKind::Omp => "omp",
        AgentKind::Mastracode => "mastracode",
        AgentKind::Opencode => "opencode",
        AgentKind::Copilot => "copilot",
        AgentKind::Kimi => "kimi",
        AgentKind::Kiro => "kiro",
        AgentKind::Droid => "droid",
        AgentKind::Amp => "amp",
        AgentKind::Grok => "grok",
        AgentKind::Hermes => "hermes",
        AgentKind::Kilo => "kilo",
        AgentKind::Qodercli => "qodercli",
        AgentKind::Qwen => "qwen",
        AgentKind::Maki => "maki",
    }
}

struct OwnedSocket {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl OwnedSocket {
    fn bind(path: PathBuf) -> Result<Self, ServerError> {
        let listener = UnixListener::bind(&path).map_err(ServerError::Io)?;
        let metadata = fs::metadata(&path).map_err(ServerError::Io)?;
        Ok(Self {
            listener,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        let Ok(metadata) = fs::metadata(&self.path) else {
            return;
        };
        if metadata.dev() == self.device && metadata.ino() == self.inode {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Frame(shared_translator::FrameError),
    Transport(crate::transport::TransportError),
    Signal(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ServerError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_request(
        configuration: &TranslatorConfiguration,
        text: &str,
    ) -> crate::transport::Frame {
        use crate::transport::{read_frame, write_frame, Frame};
        use crate::transport_types::{Kind, Lane};
        let (mut client, mut server) = UnixStream::pair().expect("socket pair");
        write_frame(
            &mut client,
            &Frame {
                lane: Lane::MetaText,
                kind: Kind::Request,
                payload: text.as_bytes().to_vec(),
            },
            MAXIMUM_SIGNAL_BYTES,
        )
        .expect("request frame");
        let place = Place {
            metaflow_name: shared_translator::placement::MetaflowName("test".into()),
            directory: shared_translator::placement::Directory("/tmp".into()),
        };
        serve_one(
            &mut server,
            configuration,
            &place,
            Endpoint::Meta,
            herdr_nexus_wire::ValidAdmission {
                deadline: Duration::from_secs(1),
                frame_capacity: MAXIMUM_SIGNAL_BYTES,
            },
            (MAXIMUM_SIGNAL_BYTES, signal::MAXIMUM_SIGNAL_DEPTH),
        )
        .expect("isolated request served");
        read_frame(&mut client, MAXIMUM_SIGNAL_BYTES).expect("response frame")
    }

    #[test]
    fn meta_lane_is_rejected_on_the_public_endpoint() {
        use crate::transport::{read_frame, write_frame, Frame};
        use crate::transport_types::{Kind, Lane};
        let (mut client, mut server) = UnixStream::pair().expect("socket pair");
        write_frame(
            &mut client,
            &Frame {
                lane: Lane::MetaText,
                kind: Kind::Request,
                payload: b"Configure.HerdrPath./bin/true".to_vec(),
            },
            MAXIMUM_SIGNAL_BYTES,
        )
        .expect("request frame");
        let place = Place {
            metaflow_name: shared_translator::placement::MetaflowName("test".into()),
            directory: shared_translator::placement::Directory("/tmp".into()),
        };
        serve_one(
            &mut server,
            &TranslatorConfiguration::default(),
            &place,
            Endpoint::Public,
            herdr_nexus_wire::ValidAdmission {
                deadline: Duration::from_secs(1),
                frame_capacity: MAXIMUM_SIGNAL_BYTES,
            },
            (MAXIMUM_SIGNAL_BYTES, signal::MAXIMUM_SIGNAL_DEPTH),
        )
        .expect("wrong lane answered");
        assert_eq!(
            read_frame(&mut client, MAXIMUM_SIGNAL_BYTES)
                .expect("response")
                .kind,
            Kind::Failed
        );
    }

    #[test]
    fn same_login_impostor_cannot_submit_an_emission() {
        use crate::transport::{read_frame, write_frame, Frame};
        use crate::transport_types::{Kind, Lane, PeerIdentity, ProcessId, UserId};
        let (mut client, mut server) = UnixStream::pair().expect("socket pair");
        write_frame(
            &mut client,
            &Frame {
                lane: Lane::EmissionSignal,
                kind: Kind::Request,
                payload: b"not decoded".to_vec(),
            },
            MAXIMUM_SIGNAL_BYTES,
        )
        .expect("request frame");
        let place = Place {
            metaflow_name: shared_translator::placement::MetaflowName("test".into()),
            directory: shared_translator::placement::Directory("/tmp".into()),
        };
        let expected = PeerIdentity {
            user_id: UserId(i64::from(nix::unistd::getuid().as_raw())),
            process_id: ProcessId(i64::from(std::process::id()) + 1),
        };
        serve_one_paired(
            &mut server,
            &TranslatorConfiguration::default(),
            &place,
            Endpoint::Public,
            herdr_nexus_wire::ValidAdmission {
                deadline: Duration::from_secs(1),
                frame_capacity: MAXIMUM_SIGNAL_BYTES,
            },
            (MAXIMUM_SIGNAL_BYTES, signal::MAXIMUM_SIGNAL_DEPTH),
            Some(&expected),
        )
        .expect("impostor receives typed failure");
        assert_eq!(
            read_frame(&mut client, MAXIMUM_SIGNAL_BYTES)
                .expect("response")
                .kind,
            Kind::Failed
        );
    }

    #[test]
    fn malformed_meta_request_is_failed_and_next_valid_set_is_answered() {
        use crate::transport_types::Kind;
        let configuration = TranslatorConfiguration::default();
        let malformed = meta_request(&configuration, "this is not datom");
        assert_eq!(malformed.kind, Kind::Failed);

        let query =
            meta_signal_herdr_nexus::Query::Configure(meta_signal_herdr_nexus::Set::HerdrPath(
                meta_signal_herdr_nexus::PathValue("/usr/bin/herdr".into()),
            ));
        let answered = meta_request(&configuration, &shared_translator::encode(&query));
        assert_eq!(answered.kind, Kind::Answered);
        let text = std::str::from_utf8(&answered.payload).expect("UTF-8 response");
        assert!(text.starts_with("Configured."), "{text}");
    }

    #[test]
    fn herdr_path_must_be_absolute_and_failed_set_preserves_prior_value() {
        use meta_signal_herdr_nexus::{PathValue, Response, Set};
        let configuration = TranslatorConfiguration::default();
        assert!(matches!(
            configuration.apply(Set::HerdrPath(PathValue("/usr/bin/herdr".into()))),
            Response::Configured(_)
        ));
        assert!(matches!(
            configuration.apply(Set::HerdrPath(PathValue("relative/herdr".into()))),
            Response::ConfigureRefused(_)
        ));
        assert_eq!(
            configuration
                .partial
                .read()
                .expect("configuration lock")
                .executable
                .as_deref(),
            Some(Path::new("/usr/bin/herdr"))
        );
    }

    fn pane(id: &str, status: &str) -> String {
        format!(
            r#"{{"pane_id":"{id}","terminal_id":"t-{id}","workspace_id":"w","tab_id":"tab","focused":false,"agent_status":"{status}","revision":1}}"#
        )
    }

    fn agent(id: &str, status: &str, ready: bool) -> String {
        format!(
            r#"{{"pane_id":"{id}","terminal_id":"t-{id}","workspace_id":"w","tab_id":"tab","focused":false,"agent_status":"{status}","interactive_ready":{ready},"revision":1}}"#
        )
    }

    #[test]
    fn separate_list_decoders_preserve_readiness_and_identity() {
        let panes = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{},{},{}]}}}}"#,
            pane("idle", "idle"),
            pane("blocked", "blocked"),
            pane("plain", "unknown")
        );
        let agents = format!(
            r#"{{"id":"cli:agent:list","result":{{"type":"agent_list","agents":[{},{}]}}}}"#,
            agent("idle", "idle", true),
            agent("blocked", "blocked", false)
        );
        let ExternalOutcome::Panes(panes) = decode_panes(panes.as_bytes(), &Session("main".into()))
        else {
            panic!("pane listing expected");
        };
        assert_eq!(panes.len(), 3);
        let ExternalOutcome::Agents(agents) = decode_agents(agents.as_bytes(), &Session("main".into()))
        else {
            panic!("agent listing expected");
        };
        let states: HashMap<_, _> = agents
            .into_iter()
            .map(|item| (item.pane.pane_id.0, item.state))
            .collect();
        assert_eq!(states["idle"], State::Idle);
        assert_eq!(states["blocked"], State::Busy);
    }

    #[test]
    fn duplicate_entries_are_uncertain_within_each_external_result() {
        let one = pane("p", "unknown");
        let duplicate = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{one},{one}]}}}}"#
        );
        assert!(matches!(
            decode_panes(duplicate.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));

        let duplicate_agent = agent("other", "idle", true);
        let agents = format!(
            r#"{{"id":"cli:agent:list","result":{{"type":"agent_list","agents":[{duplicate_agent},{duplicate_agent}]}}}}"#,
        );
        assert!(matches!(
            decode_agents(agents.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));
    }

    #[test]
    fn unknown_status_is_uncertain() {
        let panes = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{}]}}}}"#,
            pane("p", "impossible")
        );
        assert!(matches!(
            decode_panes(panes.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));
    }

    #[test]
    fn start_accepts_the_minimal_and_convenience_envelopes() {
        let agent = r#"{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"idle","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":2}"#;
        let minimal = format!(
            r#"{{"id":"cli:agent:start","result":{{"type":"agent_started","agent":{agent},"argv":["codex"]}}}}"#
        );
        let convenience = format!(
            r#"{{"id":"cli:agent:start","result":{{"type":"agent_started","agent":{agent},"argv":["codex"],"executable":"codex"}}}}"#
        );
        assert_eq!(
            decode_start(minimal.as_bytes(), "w:p", "probe", "codex"),
            ExternalOutcome::Started(State::Idle)
        );
        assert_eq!(
            decode_start(convenience.as_bytes(), "w:p", "probe", "codex"),
            ExternalOutcome::Started(State::Idle)
        );
    }

    #[test]
    fn occupied_socket_is_preserved() {
        let directory = std::env::temp_dir().join(format!(
            "herdr-translator-socket-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        fs::create_dir(&directory).expect("temporary directory");
        let path = directory.join("herdr-translator-test.sock");
        let first = OwnedSocket::bind(path.clone()).expect("first bind");
        assert!(OwnedSocket::bind(path.clone()).is_err());
        assert!(path.exists());
        drop(first);
        assert!(!path.exists());
        fs::remove_dir(directory).expect("remove temporary directory");
    }

    #[test]
    fn subscription_frame_may_be_quiet_beyond_call_timeout() {
        use std::io::Write;
        let (mut writer, reader) = UnixStream::pair().expect("socket pair");
        let reading = std::thread::spawn(move || {
            read_subscription_signal_frame(reader, Duration::from_millis(30), 32)
        });
        std::thread::sleep(Duration::from_millis(75));
        writer.write_all(&3_u32.to_be_bytes()).expect("prefix");
        writer.write_all(b"new").expect("body");
        assert_eq!(
            reading.join().expect("reader thread").expect("frame"),
            Some(b"new".to_vec())
        );
    }

    #[test]
    fn subscription_partial_frame_has_one_absolute_progress_deadline() {
        use std::io::Write;
        let (mut writer, reader) = UnixStream::pair().expect("socket pair");
        writer.write_all(&[0]).expect("first prefix byte");
        let started = Instant::now();
        let failure = read_subscription_signal_frame(reader, Duration::from_millis(30), 32)
            .expect_err("partial frame must time out");
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(matches!(failure, ServerError::Io(_)));
    }

    #[test]
    fn subscription_peer_shutdown_ends_the_stream() {
        let (writer, reader) = UnixStream::pair().expect("socket pair");
        drop(writer);
        assert_eq!(
            read_subscription_signal_frame(reader, Duration::from_millis(30), 32)
                .expect("clean shutdown"),
            None
        );
    }
}
