use crate::runner::{self, Captured};
use emission_herdr_nexus::{ExternalOutcome, Herdr, PaneObservation, Verb};
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

const SOCKET_NAME: &str = "herdr-translator.sock";
const MAXIMUM_SIGNAL_BYTES: usize = 8 * 1024 * 1024;

pub fn run(place: &Place) -> Result<(), ServerError> {
    let socket = Path::new(&place.directory.0).join(SOCKET_NAME);
    let bound = OwnedSocket::bind(socket)?;
    let configuration = std::sync::Arc::new(TranslatorConfiguration::default());
    for stream in bound.listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let configuration = std::sync::Arc::clone(&configuration);
        let place = place.clone();
        std::thread::spawn(move || {
            let _ = serve_one(&mut stream, &configuration, &place);
        });
    }
    Ok(())
}

pub fn serve_one(
    stream: &mut UnixStream,
    configuration: &TranslatorConfiguration,
    place: &Place,
) -> Result<(), ServerError> {
    use crate::transport::{self, Frame};
    use crate::transport_types::{Kind, Lane};
    let request = transport::read_frame(stream, MAXIMUM_SIGNAL_BYTES)
        .map_err(ServerError::Transport)?;
    if request.kind != Kind::Request {
        return Err(ServerError::Transport(crate::transport::TransportError::InvalidLength));
    }
    let lane = request.lane.clone();
    let response = (|| -> Result<Frame, ServerError> { Ok(match request.lane {
        Lane::EmissionSignal => {
            let query = Signal::<emission_herdr_nexus::Query>::from(request.payload)
                .restore()
                .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
            let Some(runtime) = configuration.runtime() else {
                return Err(ServerError::Signal("translator is awaiting settings".into()));
            };
            let emission_herdr_nexus::Query::Emit(herdr) = query;
            let response = emission_herdr_nexus::Response::Emitted(execute(&runtime, &herdr))
                .signalize()
                .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
            Frame { lane: Lane::EmissionSignal, kind: Kind::Answered, payload: response.bytes().to_vec() }
        }
        Lane::MetaText => {
            let caller = crate::transport::peer_caller(stream).map_err(ServerError::Transport)?;
            let text = request.text().map_err(ServerError::Transport)?;
            let response = configure_text(configuration, place, caller, text)?;
            Frame { lane: Lane::MetaText, kind: Kind::Answered, payload: response.into_bytes() }
        }
        Lane::PublicText => {
            let Some((mut budget, timeout)) = configuration.text_context() else {
                return Err(ServerError::Signal("translator is awaiting settings".into()));
            };
            let caller = crate::transport::peer_caller(stream).map_err(ServerError::Transport)?;
            let text = request.text().map_err(ServerError::Transport)?;
            let query = decode_public(text, &mut budget)?;
            let response = forward_public(place, caller, query, timeout)?;
            Frame { lane: Lane::PublicText, kind: Kind::Answered, payload: shared_translator::encode(&response).into_bytes() }
        }
    }) })();
    let response = match response {
        Ok(response) => response,
        Err(error) => Frame {
            lane,
            kind: Kind::Failed,
            payload: error.to_string().into_bytes(),
        },
    };
    transport::write_frame(stream, &response, MAXIMUM_SIGNAL_BYTES)
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
    fn runtime(&self) -> Option<Runtime> {
        let partial = self.partial.read().ok()?.clone();
        Runtime::new(partial.executable?, partial.call_timeout?, partial.output_limit?)
            .filter(|_| partial.text_budget.is_some())
    }

    #[cfg(feature = "text")]
    fn text_context(&self) -> Option<(datom_codec::Budget, Duration)> {
        let partial = self.partial.read().ok()?.clone();
        let (nodes, depth) = partial.text_budget?;
        Some((datom_codec::Budget {
            remaining: i64::try_from(nodes).ok()?,
            reader: protos::ReaderBudget { remaining: nodes },
            depth: 0,
            maximum_depth: i64::try_from(depth).ok()?,
        }, partial.call_timeout?))
    }

    fn apply(&self, setting: meta_signal_herdr_nexus::Set) -> meta_signal_herdr_nexus::Response {
        use meta_signal_herdr_nexus::{Configured_Data, Owner, Readiness, Response, Set, SettingName};
        let mut partial = self.partial.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        let invalid = match setting {
            Set::HerdrPath(value) if Path::new(&value.0).is_absolute() => { partial.executable = Some(PathBuf::from(value.0)); None }
            Set::CallTimeout(value) => positive_duration(value.0).map(|value| { partial.call_timeout = Some(value); }).is_none().then_some(SettingName::CallTimeout),
            Set::OutputLimit(value) => usize::try_from(value.0).ok().filter(|value| *value > 0).map(|value| { partial.output_limit = Some(value); }).is_none().then_some(SettingName::OutputLimit),
            Set::TextBudget(value) => {
                let nodes = usize::try_from(value.node_limit.0).ok().filter(|value| *value > 0 && *value <= signal::MAXIMUM_SIGNAL_BYTES);
                let depth = usize::try_from(value.depth_limit.0).ok().filter(|value| *value > 0 && *value <= signal::MAXIMUM_SIGNAL_DEPTH);
                match (nodes, depth) { (Some(nodes), Some(depth)) => { partial.text_budget = Some((nodes, depth)); None }, _ => Some(SettingName::TextBudget) }
            }
            Set::HerdrPath(_) => Some(SettingName::HerdrPath),
            other => return Response::ConfigureRefused(invalid_setting(Owner::Translator, setting_name(&other), "setting belongs to Nexus")),
        };
        if let Some(name) = invalid {
            return Response::ConfigureRefused(invalid_setting(Owner::Translator, name, "invalid setting value"));
        }
        let mut awaiting = Vec::new();
        if partial.executable.is_none() { awaiting.push(SettingName::HerdrPath); }
        if partial.call_timeout.is_none() { awaiting.push(SettingName::CallTimeout); }
        if partial.output_limit.is_none() { awaiting.push(SettingName::OutputLimit); }
        if partial.text_budget.is_none() { awaiting.push(SettingName::TextBudget); }
        Response::Configured(Configured_Data { owner: Owner::Translator, readiness: if awaiting.is_empty() { Readiness::Ready } else { Readiness::Awaiting(awaiting) } })
    }
}

#[cfg(feature = "text")]
fn configure_text(
    configuration: &TranslatorConfiguration,
    place: &Place,
    caller: crate::transport_types::Caller,
    text: &str,
) -> Result<String, ServerError> {
    use datom_codec::{Actualizing, Potential};
    let mut budget = bootstrap_budget();
    let mut potential = Potential::<meta_signal_herdr_nexus::Query>::from(text);
    let query = potential.actualize(&mut budget)
        .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    let meta_signal_herdr_nexus::Query::Configure(setting) = query;
    let response = match setting {
        setting @ (meta_signal_herdr_nexus::Set::HerdrPath(_)
            | meta_signal_herdr_nexus::Set::CallTimeout(_)
            | meta_signal_herdr_nexus::Set::OutputLimit(_)
            | meta_signal_herdr_nexus::Set::TextBudget(_)) => configuration.apply(setting),
        setting => {
            let Some((_, timeout)) = configuration.text_context() else {
                return Ok(shared_translator::encode(&meta_signal_herdr_nexus::Response::ConfigureRefused(
                    invalid_setting(meta_signal_herdr_nexus::Owner::Translator, setting_name(&setting), "translator is awaiting settings"),
                )));
            };
            forward_meta(place, caller, core_setting(setting), timeout)?
        }
    };
    Ok(shared_translator::encode(&response))
}

#[cfg(feature = "text")]
fn bootstrap_budget() -> datom_codec::Budget {
    datom_codec::Budget {
        remaining: i64::try_from(signal::MAXIMUM_SIGNAL_BYTES).expect("signal bound fits budget"),
        reader: protos::ReaderBudget { remaining: signal::MAXIMUM_SIGNAL_BYTES },
        depth: 0,
        maximum_depth: i64::try_from(signal::MAXIMUM_SIGNAL_DEPTH).expect("signal depth fits budget"),
    }
}

#[cfg(feature = "text")]
fn decode_public(text: &str, budget: &mut datom_codec::Budget) -> Result<signal_herdr_nexus::Query, ServerError> {
    use datom_codec::{Actualizing, Potential};
    let mut potential = Potential::<signal_herdr_nexus::Query>::from(text);
    potential.actualize(budget).map_err(|error| ServerError::Signal(format!("{error:?}")))
}

fn core_setting(setting: meta_signal_herdr_nexus::Set) -> core_meta_herdr_nexus::CoreSet {
    match setting {
        meta_signal_herdr_nexus::Set::StartTimeout(value) => core_meta_herdr_nexus::CoreSet::StartTimeout(value),
        meta_signal_herdr_nexus::Set::Polling(value) => core_meta_herdr_nexus::CoreSet::Polling(value),
        meta_signal_herdr_nexus::Set::FrameCapacity(value) => core_meta_herdr_nexus::CoreSet::FrameCapacity(value),
        meta_signal_herdr_nexus::Set::TransportTimeout(value) => core_meta_herdr_nexus::CoreSet::TransportTimeout(value),
        _ => unreachable!("translator settings are handled before core conversion"),
    }
}

fn forward_public(
    place: &Place,
    caller: crate::transport_types::Caller,
    input: signal_herdr_nexus::Query,
    timeout: Duration,
) -> Result<signal_herdr_nexus::Response, ServerError> {
    let request = herdr_nexus_wire::public_call::Query::Call(
        herdr_nexus_wire::public_call::Call_Data {
            caller,
            input: herdr_nexus_wire::public_payload::Input(input),
        },
    );
    let response: herdr_nexus_wire::public_call::Response =
        binary_call(Path::new(&place.directory.0).join("herdr.sock"), request, timeout)?;
    match response {
        herdr_nexus_wire::public_call::Response::Answered(output) => Ok(output.0),
        herdr_nexus_wire::public_call::Response::Failed(message) => Err(ServerError::Signal(message)),
    }
}

fn forward_meta(
    place: &Place,
    caller: crate::transport_types::Caller,
    input: core_meta_herdr_nexus::CoreSet,
    timeout: Duration,
) -> Result<meta_signal_herdr_nexus::Response, ServerError> {
    let request = herdr_nexus_wire::meta_call::Query::Call(
        herdr_nexus_wire::meta_call::Call_Data {
            caller,
            input: herdr_nexus_wire::meta_payload::Input(core_meta_herdr_nexus::Query::Configure(input)),
        },
    );
    let response: herdr_nexus_wire::meta_call::Response =
        binary_call(Path::new(&place.directory.0).join("herdr-meta.sock"), request, timeout)?;
    match response {
        herdr_nexus_wire::meta_call::Response::Answered(output) => match output.0 {
            core_meta_herdr_nexus::Response::Configured(readiness) => {
                Ok(meta_signal_herdr_nexus::Response::Configured(meta_signal_herdr_nexus::Configured_Data {
                    owner: meta_signal_herdr_nexus::Owner::Nexus,
                    readiness,
                }))
            }
            core_meta_herdr_nexus::Response::ConfigureRefused(refusal) => {
                Ok(meta_signal_herdr_nexus::Response::ConfigureRefused(refusal))
            }
        },
        herdr_nexus_wire::meta_call::Response::Failed(message) => Err(ServerError::Signal(message)),
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
    let request = query.signalize().map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    let capacity = FrameCapacity::from(MAXIMUM_SIGNAL_BYTES);
    stream.write_frame(&request, capacity).map_err(ServerError::Frame)?;
    let response = stream.read_frame(capacity).map_err(ServerError::Frame)?;
    Signal::<R>::from(Vec::from(response)).restore()
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
    }
}

fn invalid_setting(owner: meta_signal_herdr_nexus::Owner, setting_name: meta_signal_herdr_nexus::SettingName, message: &str) -> meta_signal_herdr_nexus::ConfigurationRefusal {
    meta_signal_herdr_nexus::ConfigurationRefusal::Invalid(meta_signal_herdr_nexus::Invalid_Data {
        owner, setting_name, message: meta_signal_herdr_nexus::Message(message.into()),
    })
}

/// Executes one typed external emission with a captured configuration snapshot.
pub fn execute(runtime: &Runtime, herdr: &Herdr) -> ExternalOutcome {
    match &herdr.verb {
        Verb::PaneList => execute_list(runtime, &herdr.session),
        verb => {
            let Some(invocation) = invocation(&herdr.session, verb, runtime) else {
                return ExternalOutcome::Uncertain;
            };
            let Some(captured) = run_invocation(runtime, &invocation) else {
                return ExternalOutcome::Uncertain;
            };
            decode(&invocation.decoder, captured, &herdr.session)
        }
    }
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
        Verb::WorkspaceCreate(data) => {
            let mut values = vec![
                OsString::from("workspace"),
                OsString::from("create"),
                OsString::from("--cwd"),
                OsString::from(&data.root.0),
            ];
            for variable in &data.variable_vector {
                values.push(OsString::from("--env"));
                values.push(OsString::from(format!(
                    "{}={}",
                    variable.name.0, variable.value.0
                )));
            }
            (values, runtime.call_timeout, Decoder::WorkspaceCreate)
        }
        Verb::AgentStart(data) => {
            let start_timeout = positive_duration(data.timeout.0)?;
            let kind = agent_kind(&data.command.agent_kind);
            let mut values = vec![
                OsString::from("agent"),
                OsString::from("start"),
                OsString::from(&data.command.agent_name.0),
                OsString::from("--kind"),
                OsString::from(kind),
                OsString::from("--pane"),
                OsString::from(&data.pane_id.0),
            ];
            if let Executable::Explicit(program) = &data.command.executable {
                values.push(OsString::from("--executable"));
                values.push(OsString::from(&program.0));
            }
            values.push(OsString::from("--timeout"));
            values.push(OsString::from(
                start_timeout.as_millis().to_string(),
            ));
            if !data.command.string_vector.is_empty() {
                values.push(OsString::from("--"));
                values.extend(data.command.string_vector.iter().map(OsString::from));
            }
            (
                values,
            start_timeout,
                Decoder::AgentStart {
                    pane_id: data.pane_id.0.clone(),
                    name: data.command.agent_name.0.clone(),
                    kind,
                },
            )
        }
        Verb::AgentPrompt(data) => (
            vec![
                OsString::from("agent"),
                OsString::from("prompt"),
                OsString::from(&data.pane_id.0),
                OsString::from(&data.text.0),
            ],
            runtime.call_timeout,
            Decoder::AgentPrompt {
                pane_id: data.pane_id.0.clone(),
            },
        ),
        Verb::Keys(data) => {
            let mut values = vec![
                OsString::from("pane"),
                OsString::from("send-keys"),
                OsString::from(&data.pane_id.0),
            ];
            values.extend(data.keys.0.iter().map(OsString::from));
            (values, runtime.call_timeout, Decoder::Keys)
        }
        Verb::PaneClose(pane_id) => (
            vec![
                OsString::from("pane"),
                OsString::from("close"),
                OsString::from(&pane_id.0),
            ],
            runtime.call_timeout,
            Decoder::PaneClose,
        ),
        Verb::PaneList => unreachable!("list has a complete-snapshot path"),
    };
    arguments.extend(specific);
    Some(Invocation {
        arguments,
        timeout,
        decoder,
    })
}

fn list_invocation(session: &Session, runtime: &Runtime, agent: bool) -> Invocation {
    let noun = if agent { "agent" } else { "pane" };
    Invocation {
        arguments: vec![
            OsString::from("--session"),
            OsString::from(&session.0),
            OsString::from(noun),
            OsString::from("list"),
        ],
        timeout: runtime.call_timeout,
        decoder: if agent {
            Decoder::AgentList
        } else {
            Decoder::PaneList
        },
    }
}

fn run_invocation(runtime: &Runtime, invocation: &Invocation) -> Option<Captured> {
    let deadline = Instant::now().checked_add(invocation.timeout)?;
    runner::run(
        &runtime.executable,
        &invocation.arguments,
        deadline,
        runtime.output_limit,
    )
    .ok()
}

fn execute_list(runtime: &Runtime, session: &Session) -> ExternalOutcome {
    let panes_invocation = list_invocation(session, runtime, false);
    let Some(panes) = run_invocation(runtime, &panes_invocation) else {
        return ExternalOutcome::Uncertain;
    };
    if !panes.status.success() {
        return refusal_or_uncertain(&panes);
    }
    let agents_invocation = list_invocation(session, runtime, true);
    let Some(agents) = run_invocation(runtime, &agents_invocation) else {
        return ExternalOutcome::Uncertain;
    };
    if !agents.status.success() {
        return refusal_or_uncertain(&agents);
    }
    decode_complete_snapshot(&panes.stdout, &agents.stdout, session)
}

fn decode(decoder: &Decoder, captured: Captured, session: &Session) -> ExternalOutcome {
    if !captured.status.success() {
        return refusal_or_uncertain(&captured);
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
        Decoder::Keys => decode_ok(&captured.stdout, None),
        Decoder::PaneClose => decode_ok(&captured.stdout, Some("cli:pane:close")),
        Decoder::PaneList | Decoder::AgentList => ExternalOutcome::Uncertain,
    }
}

fn refusal_or_uncertain(captured: &Captured) -> ExternalOutcome {
    let envelope = serde_json::from_slice::<ErrorEnvelope>(&captured.stderr)
        .or_else(|_| serde_json::from_slice::<ErrorEnvelope>(&captured.stdout));
    let Ok(envelope) = envelope else {
        return ExternalOutcome::Uncertain;
    };
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

fn decode_complete_snapshot(panes: &[u8], agents: &[u8], session: &Session) -> ExternalOutcome {
    let Ok(pane_envelope) = serde_json::from_slice::<Envelope<PaneListResult>>(panes) else {
        return ExternalOutcome::Uncertain;
    };
    let Ok(agent_envelope) = serde_json::from_slice::<Envelope<AgentListResult>>(agents) else {
        return ExternalOutcome::Uncertain;
    };
    if pane_envelope.id != "cli:pane:list"
        || pane_envelope.result.kind != "pane_list"
        || agent_envelope.id != "cli:agent:list"
        || agent_envelope.result.kind != "agent_list"
    {
        return ExternalOutcome::Uncertain;
    }

    let panes = pane_envelope.result.panes;
    let mut pane_ids = HashSet::new();
    for pane in &panes {
        if !pane_ids.insert(pane.pane_id.as_str()) {
            return ExternalOutcome::Uncertain;
        }
    }
    let panes_by_id: HashMap<_, _> = panes
        .iter()
        .map(|pane| (pane.pane_id.as_str(), pane))
        .collect();
    let mut agents_by_id = HashMap::new();
    for agent in agent_envelope.result.agents {
        let Some(pane) = panes_by_id.get(agent.pane_id.as_str()) else {
            return ExternalOutcome::Uncertain;
        };
        if agent.terminal_id != pane.terminal_id
            || agent.workspace_id != pane.workspace_id
            || agent.tab_id != pane.tab_id
            || agents_by_id.insert(agent.pane_id.clone(), agent).is_some()
        {
            return ExternalOutcome::Uncertain;
        }
    }

    ExternalOutcome::Listed(
        panes
            .into_iter()
            .map(|pane| {
                let state = agents_by_id
                    .get(&pane.pane_id)
                    .map(agent_state)
                    .unwrap_or(State::Open);
                PaneObservation {
                    pane: Pane {
                        session: session.clone(),
                        pane_id: PaneId(pane.pane_id),
                    },
                    state,
                }
            })
            .collect(),
    )
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
        serve_one(&mut server, configuration, &place).expect("isolated request served");
        read_frame(&mut client, MAXIMUM_SIGNAL_BYTES).expect("response frame")
    }

    #[test]
    fn malformed_meta_request_is_failed_and_next_valid_set_is_answered() {
        use crate::transport_types::Kind;
        let configuration = TranslatorConfiguration::default();
        let malformed = meta_request(&configuration, "this is not datom");
        assert_eq!(malformed.kind, Kind::Failed);

        let query = meta_signal_herdr_nexus::Query::Configure(
            meta_signal_herdr_nexus::Set::HerdrPath(
                meta_signal_herdr_nexus::PathValue("/usr/bin/herdr".into()),
            ),
        );
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
    fn complete_snapshot_maps_readiness_and_missing_agent() {
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
        let ExternalOutcome::Listed(observed) =
            decode_complete_snapshot(panes.as_bytes(), agents.as_bytes(), &Session("main".into()))
        else {
            panic!("complete snapshot expected");
        };
        let states: HashMap<_, _> = observed
            .into_iter()
            .map(|item| (item.pane.pane_id.0, item.state))
            .collect();
        assert_eq!(states["idle"], State::Idle);
        assert_eq!(states["blocked"], State::Busy);
        assert_eq!(states["plain"], State::Open);
    }

    #[test]
    fn duplicate_or_orphaned_snapshot_entries_are_uncertain() {
        let one = pane("p", "unknown");
        let duplicate = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{one},{one}]}}}}"#
        );
        let empty_agents = br#"{"id":"cli:agent:list","result":{"type":"agent_list","agents":[]}}"#;
        assert!(matches!(
            decode_complete_snapshot(duplicate.as_bytes(), empty_agents, &Session("main".into())),
            ExternalOutcome::Uncertain
        ));

        let panes =
            format!(r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{one}]}}}}"#);
        let agents = format!(
            r#"{{"id":"cli:agent:list","result":{{"type":"agent_list","agents":[{}]}}}}"#,
            agent("other", "idle", true)
        );
        assert!(matches!(
            decode_complete_snapshot(panes.as_bytes(), agents.as_bytes(), &Session("main".into())),
            ExternalOutcome::Uncertain
        ));
    }

    #[test]
    fn unknown_status_is_uncertain() {
        let panes = format!(
            r#"{{"id":"cli:pane:list","result":{{"type":"pane_list","panes":[{}]}}}}"#,
            pane("p", "impossible")
        );
        let agents = br#"{"id":"cli:agent:list","result":{"type":"agent_list","agents":[]}}"#;
        assert!(matches!(
            decode_complete_snapshot(panes.as_bytes(), agents, &Session("main".into())),
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
        let path = directory.join(SOCKET_NAME);
        let first = OwnedSocket::bind(path.clone()).expect("first bind");
        assert!(OwnedSocket::bind(path.clone()).is_err());
        assert!(path.exists());
        drop(first);
        assert!(!path.exists());
        fs::remove_dir(directory).expect("remove temporary directory");
    }
}
