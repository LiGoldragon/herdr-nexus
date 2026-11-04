use crate::transport_types::{Kind, Lane};
use std::io::{Read, Write};

pub const HEADER_BYTES: usize = shared_translator::unix::HEADER_BYTES;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub lane: Lane,
    pub kind: Kind,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn text(&self) -> Result<&str, TransportError> {
        match self.lane {
            Lane::PublicText | Lane::MetaText => {
                std::str::from_utf8(&self.payload).map_err(|_| TransportError::InvalidUtf8)
            }
            Lane::EmissionSignal => Err(TransportError::WrongLane),
        }
    }
}

#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    InvalidLimit,
    InvalidLength,
    TooLarge,
    UnknownLane(u8),
    UnknownKind(u8),
    InvalidUtf8,
    WrongLane,
    PeerCredential(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "herdr transport error: {self:?}")
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<shared_translator::unix::Error> for TransportError {
    fn from(error: shared_translator::unix::Error) -> Self {
        match error {
            shared_translator::unix::Error::Io(error) => Self::Io(error),
            shared_translator::unix::Error::InvalidLimit => Self::InvalidLimit,
            shared_translator::unix::Error::InvalidLength => Self::InvalidLength,
            shared_translator::unix::Error::TooLarge => Self::TooLarge,
            shared_translator::unix::Error::PeerCredential(message) => Self::PeerCredential(message),
        }
    }
}

pub fn read_frame(input: &mut impl Read, admission_limit: usize) -> Result<Frame, TransportError> {
    let raw = shared_translator::unix::read_frame(input, admission_limit)?;
    let lane = decode_lane(raw.lane)?;
    let kind = decode_kind(raw.kind)?;
    let frame = Frame {
        lane,
        kind,
        payload: raw.payload,
    };
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text()?;
    }
    Ok(frame)
}

/// Waits without an idle deadline for the first response byte, then bounds
/// all remaining frame progress by one absolute deadline.
pub fn read_frame_after_idle(
    stream: std::os::unix::net::UnixStream,
    progress_timeout: std::time::Duration,
    admission_limit: usize,
) -> Result<Option<Frame>, TransportError> {
    let Some(raw) = shared_translator::unix::read_frame_after_idle(
        stream,
        progress_timeout,
        admission_limit,
    )? else { return Ok(None) };
    let lane = decode_lane(raw.lane)?;
    let kind = decode_kind(raw.kind)?;
    let frame = Frame {
        lane,
        kind,
        payload: raw.payload,
    };
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text()?;
    }
    Ok(Some(frame))
}

pub fn write_frame(
    output: &mut impl Write,
    frame: &Frame,
    admission_limit: usize,
) -> Result<(), TransportError> {
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text()?;
    }
    shared_translator::unix::write_frame(
        output,
        &shared_translator::unix::Frame {
            lane: encode_lane(&frame.lane),
            kind: encode_kind(&frame.kind),
            payload: frame.payload.clone(),
        },
        admission_limit,
    ).map_err(Into::into)
}

#[derive(Debug)]
pub struct IncompleteSubmission {
    pub written: usize,
    pub error: TransportError,
}

/// Writes one frame while witnessing whether every byte was submitted. Any
/// failure returned here proves the request was incomplete; loss after `Ok`
/// is the separate possibly-executed case.
pub fn write_frame_witness(
    output: &mut impl Write,
    frame: &Frame,
    admission_limit: usize,
) -> Result<(), IncompleteSubmission> {
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text().map_err(|error| IncompleteSubmission { written: 0, error })?;
    }
    shared_translator::unix::write_frame_witness(
        output,
        &shared_translator::unix::Frame {
            lane: encode_lane(&frame.lane),
            kind: encode_kind(&frame.kind),
            payload: frame.payload.clone(),
        },
        admission_limit,
    ).map_err(|failure| IncompleteSubmission { written: failure.written, error: failure.error.into() })
}

pub fn peer_caller(stream: &std::os::unix::net::UnixStream) -> Result<shared_translator::Caller, TransportError> {
    let credentials = shared_translator::unix::peer_credentials(stream)?;
    Ok(shared_translator::Caller {
        process_id: credentials.process_id,
        user_id: credentials.user_id,
    })
}

fn decode_lane(value: u8) -> Result<Lane, TransportError> {
    match value {
        1 => Ok(Lane::PublicText),
        2 => Ok(Lane::MetaText),
        3 => Ok(Lane::EmissionSignal),
        other => Err(TransportError::UnknownLane(other)),
    }
}

fn encode_lane(lane: &Lane) -> u8 {
    match lane {
        Lane::PublicText => 1,
        Lane::MetaText => 2,
        Lane::EmissionSignal => 3,
    }
}

fn decode_kind(value: u8) -> Result<Kind, TransportError> {
    match value {
        0 => Ok(Kind::Request),
        1 => Ok(Kind::Answered),
        2 => Ok(Kind::Failed),
        other => Err(TransportError::UnknownKind(other)),
    }
}

fn encode_kind(kind: &Kind) -> u8 {
    match kind {
        Kind::Request => 0,
        Kind::Answered => 1,
        Kind::Failed => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn total_length_includes_lane_and_kind() {
        let frame = Frame {
            lane: Lane::PublicText,
            kind: Kind::Request,
            payload: b"List.main".to_vec(),
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame, 128).expect("write");
        assert_eq!(u32::from_be_bytes(bytes[..4].try_into().unwrap()), 11);
        assert_eq!(read_frame(&mut Cursor::new(bytes), 128).unwrap(), frame);
    }

    #[test]
    fn rejects_oversize_before_reading_body() {
        let mut bytes = Cursor::new((129_u32).to_be_bytes().to_vec());
        assert!(matches!(
            read_frame(&mut bytes, 128),
            Err(TransportError::TooLarge)
        ));
    }

    #[test]
    fn text_lane_requires_utf8_and_unknown_lane_is_not_guessed() {
        let invalid_text = Frame {
            lane: Lane::PublicText,
            kind: Kind::Request,
            payload: vec![0xff],
        };
        assert!(matches!(
            write_frame(&mut Vec::new(), &invalid_text, 128),
            Err(TransportError::InvalidUtf8)
        ));
        let mut unknown = vec![0, 0, 0, 2, 9, 0];
        assert!(matches!(
            read_frame(&mut Cursor::new(&mut unknown), 128),
            Err(TransportError::UnknownLane(9))
        ));
    }

    #[test]
    fn submission_witness_distinguishes_partial_from_complete() {
        struct StopsAfter { limit: usize, bytes: Vec<u8> }
        impl Write for StopsAfter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.bytes.len() == self.limit {
                    return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "fixture stop"));
                }
                let count = bytes.len().min(self.limit - self.bytes.len());
                self.bytes.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let frame = Frame { lane: Lane::EmissionSignal, kind: Kind::Request, payload: vec![1, 2, 3] };
        let mut partial = StopsAfter { limit: 5, bytes: Vec::new() };
        let failure = write_frame_witness(&mut partial, &frame, 64).expect_err("partial fixture");
        assert_eq!(failure.written, 5);
        let mut complete = StopsAfter { limit: 9, bytes: Vec::new() };
        write_frame_witness(&mut complete, &frame, 64).expect("complete submission");
    }

    #[test]
    fn idle_before_first_response_byte_has_no_lifetime_timeout() {
        let (reader, mut writer) = std::os::unix::net::UnixStream::pair().expect("pair");
        let expected = Frame {
            lane: Lane::EmissionSignal,
            kind: Kind::Answered,
            payload: vec![1, 2, 3],
        };
        let sent = expected.clone();
        let peer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(60));
            write_frame(&mut writer, &sent, 64).expect("late complete response");
        });
        let began = std::time::Instant::now();
        assert_eq!(
            read_frame_after_idle(reader, std::time::Duration::from_millis(10), 64)
                .expect("idle response"),
            Some(expected)
        );
        assert!(began.elapsed() >= std::time::Duration::from_millis(50));
        peer.join().expect("peer");
    }

    #[test]
    fn progress_after_first_response_byte_has_one_absolute_deadline() {
        let (reader, mut writer) = std::os::unix::net::UnixStream::pair().expect("pair");
        let peer = std::thread::spawn(move || {
            writer.write_all(&[0]).expect("first prefix byte");
            std::thread::sleep(std::time::Duration::from_millis(80));
        });
        let began = std::time::Instant::now();
        assert!(matches!(
            read_frame_after_idle(reader, std::time::Duration::from_millis(15), 64),
            Err(TransportError::Io(_))
        ));
        assert!(began.elapsed() < std::time::Duration::from_millis(70));
        peer.join().expect("peer");
    }
}
