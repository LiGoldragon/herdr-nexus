//! Text-front translator and typed herdr external boundary.

pub mod observation;
pub mod runner;
pub mod server;
pub mod generated {
    pub mod argument;
}
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
        Self {
            socket: socket.into(),
            admission_limit,
        }
    }

    pub fn request(&self, lane: Lane, text: &str) -> Result<String, ClientError> {
        let mut first = None;
        self.request_stream(lane, text, |answer| {
            if first.is_none() {
                first = Some(answer.to_owned());
            }
            false
        })?;
        first.ok_or_else(|| ClientError::Protocol("translator closed without an answer".into()))
    }

    /// Delivers each answer in order. Returning false stops after that answer;
    /// returning true keeps the nondurable subscription connection open.
    pub fn request_stream(
        &self,
        lane: Lane,
        text: &str,
        mut receive: impl FnMut(&str) -> bool,
    ) -> Result<(), ClientError> {
        if matches!(lane, Lane::EmissionSignal | Lane::ObservationSignal) {
            return Err(ClientError::Transport(TransportError::WrongLane));
        }
        let mut stream =
            std::os::unix::net::UnixStream::connect(&self.socket).map_err(ClientError::Io)?;
        transport::write_frame(
            &mut stream,
            &Frame {
                lane: lane.clone(),
                kind: Kind::Request,
                payload: text.as_bytes().to_vec(),
            },
            self.admission_limit,
        )
        .map_err(ClientError::Transport)?;
        let mut received = false;
        loop {
            let response = match transport::read_frame(&mut stream, self.admission_limit) {
                Ok(response) => response,
                Err(TransportError::Io(error))
                    if received && error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(())
                }
                Err(error) => return Err(ClientError::Transport(error)),
            };
            if response.lane != lane {
                return Err(ClientError::Protocol(
                    "translator answered on another lane".into(),
                ));
            }
            let text = String::from_utf8(response.payload)
                .map_err(|_| ClientError::Protocol("translator returned non-UTF-8 text".into()))?;
            match response.kind {
                Kind::Answered if receive(&text) => {
                    received = true;
                }
                Kind::Answered => return Ok(()),
                Kind::Failed => return Err(ClientError::Protocol(text)),
                Kind::Request => {
                    return Err(ClientError::Protocol(
                        "translator answered with request kind".into(),
                    ))
                }
            }
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
pub use shared_translator::placement;

/// The one construction of a textualization budget used by every translator
/// entry point and request decoder.
#[cfg(feature = "text")]
pub fn text_budget(nodes: usize, depth: usize) -> datom_codec::Budget {
    datom_codec::Budget {
        remaining: i64::try_from(nodes).expect("validated text nodes fit i64"),
        reader: protos::ReaderBudget::try_from(nodes)
            .expect("validated text nodes fit the reader budget"),
        depth: 0,
        maximum_depth: i64::try_from(depth).expect("validated text depth fits i64"),
    }
}

#[cfg(feature = "text")]
pub fn maximum_text_budget() -> datom_codec::Budget {
    text_budget(signal::MAXIMUM_SIGNAL_BYTES, signal::MAXIMUM_SIGNAL_DEPTH)
}
