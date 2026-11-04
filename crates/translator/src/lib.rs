//! Text-front translator and typed herdr external boundary.

pub mod runner;
pub mod server;
pub use herdr_nexus_wire::frame as transport;
pub use herdr_nexus_wire::transport_types;

use herdr_nexus_wire::frame::{Frame, TransportError};
use herdr_nexus_wire::transport_types::{Kind, Lane};

/// One text-lane connection to the translator. The CLI carries text only;
/// typed signal begins behind the translator.
pub struct TextClient {
    socket: std::path::PathBuf,
    admission_limit: usize,
}

impl TextClient {
    pub fn new(socket: impl Into<std::path::PathBuf>, admission_limit: usize) -> Self {
        Self { socket: socket.into(), admission_limit }
    }

    pub fn request(&self, lane: Lane, text: &str) -> Result<String, ClientError> {
        if lane == Lane::EmissionSignal {
            return Err(ClientError::Transport(TransportError::WrongLane));
        }
        let mut stream = std::os::unix::net::UnixStream::connect(&self.socket)
            .map_err(ClientError::Io)?;
        transport::write_frame(&mut stream, &Frame {
            lane: lane.clone(), kind: Kind::Request, payload: text.as_bytes().to_vec(),
        }, self.admission_limit).map_err(ClientError::Transport)?;
        let response = transport::read_frame(&mut stream, self.admission_limit)
            .map_err(ClientError::Transport)?;
        if response.lane != lane {
            return Err(ClientError::Protocol("translator answered on another lane".into()));
        }
        let text = String::from_utf8(response.payload)
            .map_err(|_| ClientError::Protocol("translator returned non-UTF-8 text".into()))?;
        match response.kind {
            Kind::Answered => Ok(text),
            Kind::Failed => Err(ClientError::Protocol(text)),
            Kind::Request => Err(ClientError::Protocol("translator answered with request kind".into())),
        }
    }
}

#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Transport(TransportError),
    Protocol(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "herdr translator client error: {self:?}")
    }
}
impl std::error::Error for ClientError {}

#[cfg(feature = "text")]
pub use shared_translator::{placement, registry};
