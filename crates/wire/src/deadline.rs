//! Absolute-deadline Unix stream adapter for framed attempts.

use std::time::{Duration, Instant};

pub struct DeadlineStream {
    stream: std::os::unix::net::UnixStream,
    deadline: Instant,
}

impl DeadlineStream {
    pub fn new(stream: std::os::unix::net::UnixStream, timeout: Duration) -> std::io::Result<Self> {
        let deadline = Instant::now().checked_add(timeout)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "deadline overflow"))?;
        Ok(Self { stream, deadline })
    }

    fn remaining(&self) -> std::io::Result<Duration> {
        self.deadline.checked_duration_since(Instant::now()).filter(|value| !value.is_zero())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "attempt deadline elapsed"))
    }
}

impl std::io::Read for DeadlineStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.stream.set_read_timeout(Some(self.remaining()?))?;
        std::io::Read::read(&mut self.stream, buffer)
    }
}

impl std::io::Write for DeadlineStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        std::io::Write::write(&mut self.stream, buffer)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        std::io::Write::flush(&mut self.stream)
    }
}
