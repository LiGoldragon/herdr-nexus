use crate::transport_types::{Caller, Kind, Lane};
use shared_translator::{GroupId, ProcessId, UserId};
use std::io::{Read, Write};

pub const HEADER_BYTES: usize = 2;

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

pub fn read_frame(input: &mut impl Read, admission_limit: usize) -> Result<Frame, TransportError> {
    validate_limit(admission_limit)?;
    let mut prefix = [0_u8; 4];
    input.read_exact(&mut prefix)?;
    let total =
        usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| TransportError::InvalidLength)?;
    if total < HEADER_BYTES {
        return Err(TransportError::InvalidLength);
    }
    if total > admission_limit {
        return Err(TransportError::TooLarge);
    }
    let mut body = vec![0_u8; total];
    input.read_exact(&mut body)?;
    let lane = decode_lane(body[0])?;
    let kind = decode_kind(body[1])?;
    let frame = Frame {
        lane,
        kind,
        payload: body.split_off(HEADER_BYTES),
    };
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text()?;
    }
    Ok(frame)
}

/// Waits without an idle deadline for the first response byte, then bounds
/// all remaining frame progress by one absolute deadline.
pub fn read_frame_after_idle(
    mut stream: std::os::unix::net::UnixStream,
    progress_timeout: std::time::Duration,
    admission_limit: usize,
) -> Result<Option<Frame>, TransportError> {
    validate_limit(admission_limit)?;
    stream.set_read_timeout(None)?;
    let mut prefix = [0_u8; 4];
    match stream.read(&mut prefix[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("a one-byte buffer yields at most one byte"),
        Err(error) => return Err(TransportError::Io(error)),
    }
    let mut progressing = crate::deadline::DeadlineStream::new(stream, progress_timeout)?;
    progressing.read_exact(&mut prefix[1..])?;
    let total =
        usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| TransportError::InvalidLength)?;
    if total < HEADER_BYTES {
        return Err(TransportError::InvalidLength);
    }
    if total > admission_limit {
        return Err(TransportError::TooLarge);
    }
    let mut body = vec![0_u8; total];
    progressing.read_exact(&mut body)?;
    let lane = decode_lane(body[0])?;
    let kind = decode_kind(body[1])?;
    let frame = Frame {
        lane,
        kind,
        payload: body.split_off(HEADER_BYTES),
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
    validate_limit(admission_limit)?;
    if matches!(frame.lane, Lane::PublicText | Lane::MetaText) {
        frame.text()?;
    }
    let total = frame
        .payload
        .len()
        .checked_add(HEADER_BYTES)
        .ok_or(TransportError::TooLarge)?;
    if total > admission_limit {
        return Err(TransportError::TooLarge);
    }
    let total = u32::try_from(total).map_err(|_| TransportError::TooLarge)?;
    output.write_all(&total.to_be_bytes())?;
    output.write_all(&[encode_lane(&frame.lane), encode_kind(&frame.kind)])?;
    output.write_all(&frame.payload)?;
    output.flush()?;
    Ok(())
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
    let mut bytes = Vec::new();
    write_frame(&mut bytes, frame, admission_limit).map_err(|error| IncompleteSubmission {
        written: 0,
        error,
    })?;
    let mut written = 0;
    while written < bytes.len() {
        match output.write(&bytes[written..]) {
            Ok(0) => {
                return Err(IncompleteSubmission {
                    written,
                    error: TransportError::Io(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "request frame write returned zero",
                    )),
                })
            }
            Ok(count) => written += count,
            Err(error) => {
                return Err(IncompleteSubmission {
                    written,
                    error: TransportError::Io(error),
                })
            }
        }
    }
    output.flush().map_err(|error| IncompleteSubmission {
        written,
        error: TransportError::Io(error),
    })
}

pub fn peer_caller(stream: &std::os::unix::net::UnixStream) -> Result<Caller, TransportError> {
    let credentials = nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
        .map_err(|error| TransportError::PeerCredential(error.to_string()))?;
    Ok(Caller {
        user_id: UserId(i64::from(credentials.uid())),
        group_id: GroupId(i64::from(credentials.gid())),
        process_id: ProcessId(i64::from(credentials.pid())),
    })
}

fn validate_limit(limit: usize) -> Result<(), TransportError> {
    if (HEADER_BYTES..=signal::MAXIMUM_SIGNAL_BYTES).contains(&limit) {
        Ok(())
    } else {
        Err(TransportError::InvalidLimit)
    }
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
