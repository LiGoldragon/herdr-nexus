use crate::runner::{self, Captured};
use emission_herdr_nexus::{ExternalOutcome, Herdr, PaneObservation, Verb};
use meta_signal_herdr_nexus::Configuration;
use serde::Deserialize;
use shared_translator::placement::Place;
use shared_translator::{
    FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable,
};
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
    for stream in bound.listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let _ = serve_one(&mut stream);
    }
    Ok(())
}

pub fn serve_one(stream: &mut UnixStream) -> Result<(), ServerError> {
    let maximum = FrameCapacity::from(MAXIMUM_SIGNAL_BYTES);
    let body = stream.read_frame(maximum).map_err(ServerError::Frame)?;
    let request = Signal::<emission_herdr_nexus::Query>::from(Vec::from(body))
        .restore()
        .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    let emission_herdr_nexus::Query::Execute(data) = request;
    let response_capacity = frame_capacity(&data.configuration).unwrap_or(maximum);
    let outcome = execute(&data.configuration, &data.herdr);
    let response = emission_herdr_nexus::Response::Executed(outcome)
        .signalize()
        .map_err(|error| ServerError::Signal(format!("{error:?}")))?;
    stream
        .write_frame(&response, response_capacity)
        .map_err(ServerError::Frame)
}

/// Executes one typed external emission with a captured configuration snapshot.
pub fn execute(configuration: &Configuration, herdr: &Herdr) -> ExternalOutcome {
    let Some(runtime) = Runtime::from_configuration(configuration) else {
        return ExternalOutcome::Uncertain;
    };
    match &herdr.verb {
        Verb::PaneList => execute_list(&runtime, &herdr.session),
        verb => {
            let invocation = invocation(&herdr.session, verb, &runtime);
            let Some(captured) = run_invocation(&runtime, &invocation) else {
                return ExternalOutcome::Uncertain;
            };
            decode(&invocation.decoder, captured, &herdr.session)
        }
    }
}

#[derive(Clone)]
struct Runtime {
    executable: PathBuf,
    polling: Duration,
    call_timeout: Duration,
    start_timeout: Duration,
    output_limit: usize,
}

impl Runtime {
    fn from_configuration(configuration: &Configuration) -> Option<Self> {
        let polling = positive_duration(configuration.polling_milliseconds.0)?;
        let call_timeout = positive_duration(configuration.call_timeout_milliseconds.0)?;
        let start_timeout = positive_duration(configuration.start_timeout_milliseconds.0)?;
        let output_limit = usize::try_from(configuration.external_output_limit_bytes.0).ok()?;
        let frame = usize::try_from(configuration.frame_capacity_bytes.0).ok()?;
        if configuration.herdr_executable.0.is_empty()
            || output_limit == 0
            || !(1..=MAXIMUM_SIGNAL_BYTES).contains(&frame)
        {
            return None;
        }
        Some(Self {
            executable: PathBuf::from(&configuration.herdr_executable.0),
            polling,
            call_timeout,
            start_timeout,
            output_limit,
        })
    }
}

fn positive_duration(milliseconds: i64) -> Option<Duration> {
    let milliseconds = u64::try_from(milliseconds).ok()?;
    (milliseconds != 0).then(|| Duration::from_millis(milliseconds))
}

fn frame_capacity(configuration: &Configuration) -> Option<FrameCapacity> {
    let capacity = usize::try_from(configuration.frame_capacity_bytes.0).ok()?;
    (1..=MAXIMUM_SIGNAL_BYTES)
        .contains(&capacity)
        .then(|| FrameCapacity::from(capacity))
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

fn invocation(session: &Session, verb: &Verb, runtime: &Runtime) -> Invocation {
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
                runtime.start_timeout.as_millis().to_string(),
            ));
            if !data.command.string_vector.is_empty() {
                values.push(OsString::from("--"));
                values.extend(data.command.string_vector.iter().map(OsString::from));
            }
            (
                values,
                runtime.start_timeout,
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
    Invocation {
        arguments,
        timeout,
        decoder,
    }
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
        runtime.polling,
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
    #[allow(dead_code)]
    executable: String,
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
    ExternalOutcome::Done
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
