//! Typed external herdr command vocabulary. Signal/socket attachment is kept
//! separate until generated herdr contracts land.
use serde::{Deserialize, Serialize};
use std::{fmt, path::PathBuf};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentKind { Pi, Claude, Codex, Gemini, Cursor, Devin, Agy, Cline, Omp, Mastracode, Opencode, Copilot, Kimi, Kiro, Droid, Amp, Grok, Hermes, Kilo, Qodercli, Qwen, Maki }

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentName(String);
impl AgentName {
    pub fn new(value: impl Into<String>) -> Result<Self, AgentNameError> {
        let value = value.into();
        let bytes = value.as_bytes();
        if bytes.is_empty() || bytes.len() > 32 || !bytes[0].is_ascii_lowercase() || !bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'_') { return Err(AgentNameError); }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)] pub struct AgentNameError;
impl fmt::Display for AgentNameError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("agent name must start lowercase and contain 1..32 lowercase letters, digits, '-' or '_' ") } }
impl std::error::Error for AgentNameError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Executable { Configured, Explicit(PathBuf) }
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentStart { pub name: AgentName, pub kind: AgentKind, pub executable: Executable, pub pane_id: String, pub arguments: Vec<String>, pub timeout_ms: Option<u64> }
impl AgentStart {
    pub fn argv(&self) -> Vec<String> {
        let mut out = vec!["agent".into(), "start".into(), self.name.0.clone(), "--kind".into(), self.kind.to_string(), "--pane".into(), self.pane_id.clone()];
        if let Executable::Explicit(path) = &self.executable { out.extend(["--executable".into(), path.display().to_string()]); }
        if let Some(timeout) = self.timeout_ms { out.extend(["--timeout".into(), timeout.to_string()]); }
        if !self.arguments.is_empty() { out.push("--".into()); out.extend(self.arguments.clone()); }
        out
    }
}
impl fmt::Display for AgentKind { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(match self { Self::Pi=>"pi",Self::Claude=>"claude",Self::Codex=>"codex",Self::Gemini=>"gemini",Self::Cursor=>"cursor",Self::Devin=>"devin",Self::Agy=>"agy",Self::Cline=>"cline",Self::Omp=>"omp",Self::Mastracode=>"mastracode",Self::Opencode=>"opencode",Self::Copilot=>"copilot",Self::Kimi=>"kimi",Self::Kiro=>"kiro",Self::Droid=>"droid",Self::Amp=>"amp",Self::Grok=>"grok",Self::Hermes=>"hermes",Self::Kilo=>"kilo",Self::Qodercli=>"qodercli",Self::Qwen=>"qwen",Self::Maki=>"maki"}) } }

#[derive(Clone, Debug, Deserialize)] pub struct HerdrError { pub code: String, pub message: String }
#[derive(Clone, Debug, Deserialize)] pub struct ErrorEnvelope { pub error: HerdrError, pub id: String }
#[derive(Clone, Debug, Deserialize)] pub struct AgentStarted { pub agent: String, pub agent_status: String, pub interactive_ready: bool, pub name: String, pub pane_id: String, #[serde(flatten)] pub extra: serde_json::Value }
#[derive(Clone, Debug, Deserialize)] pub struct ResultEnvelope<T> { pub id: String, pub result: T }
#[derive(Clone, Debug, Deserialize)] pub struct AgentStartedResult { #[serde(rename = "type")] pub kind: String, pub agent: AgentStarted, pub argv: Vec<String>, pub executable: String }

pub fn parse_agent_started(input: &[u8]) -> Result<ResultEnvelope<AgentStartedResult>, serde_json::Error> { serde_json::from_slice(input) }
pub fn parse_error(input: &[u8]) -> Result<ErrorEnvelope, serde_json::Error> { serde_json::from_slice(input) }

#[derive(Clone, Debug, Deserialize)]
pub struct WorkspaceCreatedResult { #[serde(rename = "type")] pub kind: String, pub workspace: serde_json::Value, pub root_pane: RootPane }
#[derive(Clone, Debug, Deserialize)] pub struct RootPane { pub pane_id: String }
#[derive(Clone, Debug, Eq, PartialEq)] pub enum StartWitnessError { NotReady, WrongResultType }
pub fn witness_agent_started(result: AgentStartedResult) -> Result<AgentStartedResult, StartWitnessError> {
    if result.kind != "agent_started" { return Err(StartWitnessError::WrongResultType); }
    if !result.agent.interactive_ready { return Err(StartWitnessError::NotReady); }
    Ok(result)
}

/// A one-request ordinary herdr connection. Subscriptions use another client
/// connection and are intentionally not multiplexed here.
pub struct Client {
    socket: std::path::PathBuf,
    capacity: shared_translator::FrameCapacity,
}
impl Client {
    pub fn new(socket: impl Into<std::path::PathBuf>, capacity: shared_translator::FrameCapacity) -> Self { Self { socket: socket.into(), capacity } }
    pub fn request(&self, query: signal_herdr_nexus::Query) -> Result<signal_herdr_nexus::Response, ClientError> {
        use shared_translator::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(&self.socket).map_err(ClientError::Io)?;
        let signal = query.signalize().map_err(|e| ClientError::Signal(format!("{e:?}")))?;
        stream.write_frame(&signal, self.capacity).map_err(ClientError::Frame)?;
        let body = stream.read_frame(self.capacity).map_err(ClientError::Frame)?;
        Signal::<signal_herdr_nexus::Response>::from(Vec::<u8>::from(body)).restore().map_err(|e| ClientError::Signal(format!("{e:?}")))
    }
}
#[derive(Debug)] pub enum ClientError { Io(std::io::Error), Frame(shared_translator::FrameError), Signal(String) }
impl std::fmt::Display for ClientError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "herdr translator client error: {self:?}") } }
impl std::error::Error for ClientError {}

#[cfg(feature = "text")]
pub fn request_text(client: &Client, text: &str, budget: &mut datom_codec::Budget) -> Result<String, ClientError> {
    use datom_codec::{Actualizing, Potential};
    let mut potential = Potential::<signal_herdr_nexus::Query>::from(text);
    let query = potential.actualize(budget).map_err(|e| ClientError::Signal(format!("{e:?}")))?;
    let response = client.request(query)?;
    Ok(shared_translator::encode(&response))
}

impl Client {
    /// Send one typed Emission execute request over an ordinary connection.
    pub fn execute(&self, configuration: meta_signal_herdr_nexus::Configuration, herdr: emission_herdr_nexus::Herdr) -> Result<emission_herdr_nexus::ExternalOutcome, ClientError> {
        use emission_herdr_nexus::{Query, Response};
        use shared_translator::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(&self.socket).map_err(ClientError::Io)?;
        let request = Query::Execute(emission_herdr_nexus::Execute_Data { configuration, herdr });
        let signal = request.signalize().map_err(|e| ClientError::Signal(format!("{e:?}")))?;
        stream.write_frame(&signal, self.capacity).map_err(ClientError::Frame)?;
        let body = stream.read_frame(self.capacity).map_err(ClientError::Frame)?;
        let response = Signal::<Response>::from(Vec::<u8>::from(body)).restore().map_err(|e| ClientError::Signal(format!("{e:?}")))?;
        match response { Response::Executed(outcome) => Ok(outcome) }
    }
}
