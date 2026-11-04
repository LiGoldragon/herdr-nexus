//! Typed clients and the external herdr translator boundary.

pub mod runner;
pub mod server;

use shared_translator::FrameCapacity;

/// A one-request ordinary herdr connection. Subscriptions use a dedicated
/// connection and are never multiplexed here.
pub struct Client {
    socket: std::path::PathBuf,
    capacity: FrameCapacity,
}

impl Client {
    pub fn new(socket: impl Into<std::path::PathBuf>, capacity: FrameCapacity) -> Self {
        Self {
            socket: socket.into(),
            capacity,
        }
    }

    pub fn request(
        &self,
        query: signal_herdr_nexus::Query,
    ) -> Result<signal_herdr_nexus::Response, ClientError> {
        use shared_translator::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(&self.socket).map_err(ClientError::Io)?;
        let signal = query
            .signalize()
            .map_err(|error| ClientError::Signal(format!("{error:?}")))?;
        stream
            .write_frame(&signal, self.capacity)
            .map_err(ClientError::Frame)?;
        let body = stream
            .read_frame(self.capacity)
            .map_err(ClientError::Frame)?;
        Signal::<signal_herdr_nexus::Response>::from(Vec::<u8>::from(body))
            .restore()
            .map_err(|error| ClientError::Signal(format!("{error:?}")))
    }

    pub fn execute(
        &self,
        configuration: meta_signal_herdr_nexus::Configuration,
        herdr: emission_herdr_nexus::Herdr,
    ) -> Result<emission_herdr_nexus::ExternalOutcome, ClientError> {
        use emission_herdr_nexus::{Execute_Data, Query, Response};
        use shared_translator::{FrameReading, FrameWriting, Restorable, Signal, Signalizable};
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(&self.socket).map_err(ClientError::Io)?;
        let request = Query::Execute(Execute_Data {
            configuration,
            herdr,
        });
        let signal = request
            .signalize()
            .map_err(|error| ClientError::Signal(format!("{error:?}")))?;
        stream
            .write_frame(&signal, self.capacity)
            .map_err(ClientError::Frame)?;
        let body = stream
            .read_frame(self.capacity)
            .map_err(ClientError::Frame)?;
        let response = Signal::<Response>::from(Vec::<u8>::from(body))
            .restore()
            .map_err(|error| ClientError::Signal(format!("{error:?}")))?;
        match response {
            Response::Executed(outcome) => Ok(outcome),
        }
    }

    #[cfg(feature = "text")]
    pub fn request_text(
        &self,
        text: &str,
        budget: &mut datom_codec::Budget,
    ) -> Result<String, ClientError> {
        use datom_codec::{Actualizing, Potential};
        let mut potential = Potential::<signal_herdr_nexus::Query>::from(text);
        let query = potential
            .actualize(budget)
            .map_err(|error| ClientError::Signal(format!("{error:?}")))?;
        let response = self.request(query)?;
        Ok(shared_translator::encode(&response))
    }
}

#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Frame(shared_translator::FrameError),
    Signal(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "herdr translator client error: {self:?}")
    }
}

impl std::error::Error for ClientError {}

#[cfg(feature = "text")]
pub use shared_translator::{placement, registry};
