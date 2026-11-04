//! Shared typed ingress and bounded lane framing for the herdr Nexus.

pub mod frame;
pub mod deadline;
pub mod meta_call;
pub mod meta_payload;
pub mod public_call;
pub mod public_payload;
pub mod transport_types;

pub fn socket_names() -> transport_types::SocketNames {
    transport_types::SocketNames {
        public_nexus_socket: transport_types::PublicNexusSocket("herdr.sock".into()),
        meta_nexus_socket: transport_types::MetaNexusSocket("herdr-meta.sock".into()),
        public_translator_socket: transport_types::PublicTranslatorSocket(
            "herdr-translator.sock".into(),
        ),
        meta_translator_socket: transport_types::MetaTranslatorSocket(
            "herdr-meta-translator.sock".into(),
        ),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ValidAdmission {
    pub deadline: std::time::Duration,
    pub frame_capacity: usize,
    pub retry_delay: std::time::Duration,
}

impl TryFrom<&transport_types::Admission> for ValidAdmission {
    type Error = &'static str;

    fn try_from(value: &transport_types::Admission) -> Result<Self, Self::Error> {
        let milliseconds = u64::try_from(value.frame_deadline_milliseconds.0)
            .ok()
            .filter(|value| *value > 0)
            .ok_or("frame deadline must be positive and representable")?;
        let deadline = std::time::Duration::from_millis(milliseconds);
        std::time::Instant::now()
            .checked_add(deadline)
            .ok_or("frame deadline overflows the monotonic clock")?;
        let frame_capacity = usize::try_from(value.frame_capacity_bytes.0)
            .ok()
            .filter(|value| (frame::HEADER_BYTES..=signal::MAXIMUM_SIGNAL_BYTES).contains(value))
            .ok_or("frame capacity is outside the shared wire bound")?;
        let retry_milliseconds = u64::try_from(value.retry_delay_milliseconds.0)
            .ok()
            .filter(|value| *value > 0)
            .ok_or("retry delay must be positive and representable")?;
        let retry_delay = std::time::Duration::from_millis(retry_milliseconds);
        std::time::Instant::now()
            .checked_add(retry_delay)
            .ok_or("retry delay overflows the monotonic clock")?;
        Ok(Self { deadline, frame_capacity, retry_delay })
    }
}

fn recoverable_accept(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::ConnectionAborted
    ) || matches!(
        error.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
    )
}

pub fn accept_with_retry(
    listener: &std::os::unix::net::UnixListener,
    retry_delay: std::time::Duration,
) -> std::io::Result<(std::os::unix::net::UnixStream, std::os::unix::net::SocketAddr)> {
    accept_with_retry_using(
        || listener.accept(),
        |delay| std::thread::sleep(delay),
        retry_delay,
    )
}

fn accept_with_retry_using<T>(
    mut accept: impl FnMut() -> std::io::Result<T>,
    mut wait: impl FnMut(std::time::Duration),
    retry_delay: std::time::Duration,
) -> std::io::Result<T> {
    loop {
        match accept() {
            Ok(value) => return Ok(value),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) if recoverable_accept(&error) => {
                eprintln!("socket accept temporarily unavailable: {error}");
                wait(retry_delay);
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod accept_tests {
    use super::*;

    #[test]
    fn transient_accept_waits_then_serves_the_next_client() {
        let mut attempts = 0;
        let mut waits = Vec::new();
        let value = accept_with_retry_using(
            || {
                attempts += 1;
                if attempts == 1 {
                    Err(std::io::Error::from_raw_os_error(libc::EMFILE))
                } else {
                    Ok("valid client")
                }
            },
            |delay| waits.push(delay),
            std::time::Duration::from_millis(7),
        )
        .expect("second client accepted");
        assert_eq!(value, "valid client");
        assert_eq!(waits, [std::time::Duration::from_millis(7)]);
    }

    #[test]
    fn invalid_listener_is_a_visible_fatal_error() {
        let error = accept_with_retry_using(
            || Err::<(), _>(std::io::Error::from_raw_os_error(libc::EBADF)),
            |_| panic!("permanent error must not retry"),
            std::time::Duration::from_millis(7),
        )
        .expect_err("bad listener is fatal");
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    }
}
