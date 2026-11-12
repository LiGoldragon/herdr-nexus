//! Bounded FIFO whose state changes and peer EOF wake the same descriptor wait.
use crate::observation_ports::Deliverable;
use nix::poll::{poll, PollFd, PollFlags};
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::sync::Mutex;

struct QueueState<T> {
    pending: VecDeque<T>,
    capacity: usize,
    closed: bool,
}

pub struct WakeQueue<T> {
    state: Mutex<QueueState<T>>,
    wake_reader: UnixStream,
    wake_writer: UnixStream,
}

impl<T> WakeQueue<T> {
    pub fn new(capacity: usize) -> io::Result<Self> {
        if capacity == 0 { return Err(io::Error::new(io::ErrorKind::InvalidInput, "zero queue capacity")); }
        let (wake_reader, wake_writer) = UnixStream::pair()?;
        wake_reader.set_nonblocking(true)?;
        wake_writer.set_nonblocking(true)?;
        Ok(Self { state: Mutex::new(QueueState { pending: VecDeque::new(), capacity, closed: false }), wake_reader, wake_writer })
    }

    fn wake(&self) -> io::Result<()> {
        match (&self.wake_writer).write(&[1]) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn is_open(&self) -> bool {
        !self.state.lock().unwrap_or_else(|p| p.into_inner()).closed
    }
    pub fn set_capacity(&self, capacity: usize) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.capacity = capacity;
        if capacity == 0 || state.pending.len() > capacity {
            state.closed = true;
            state.pending.clear();
            drop(state);
            let _ = self.wake();
        }
    }
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        state.pending.clear();
        drop(state);
        let _ = self.wake();
    }

    /// EOF wins over buffered output: cancellation must not wait for a backlog.
    pub fn receive(&self, peer: &UnixStream) -> io::Result<Option<T>> {
        loop {
            let mut descriptors = [PollFd::new(peer.as_fd(), PollFlags::POLLIN)];
            poll(&mut descriptors, 0_u8)?;
            if descriptors[0].revents().is_some_and(|flags| !flags.is_empty()) {
                self.close();
                return Ok(None);
            }
            {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if state.closed { return Ok(None); }
                if let Some(item) = state.pending.pop_front() { return Ok(Some(item)); }
            }
            let mut descriptors = [
                PollFd::new(self.wake_reader.as_fd(), PollFlags::POLLIN),
                PollFd::new(peer.as_fd(), PollFlags::POLLIN),
            ];
            match poll(&mut descriptors, None::<u16>) {
                Ok(_) => (),
                Err(nix::errno::Errno::EINTR) => continue,
                Err(error) => return Err(error.into()),
            }
            if descriptors[1].revents().is_some_and(|flags| !flags.is_empty()) {
                self.close();
                return Ok(None);
            }
            let mut bytes = [0; 256];
            while (&self.wake_reader).read(&mut bytes).is_ok_and(|count| count > 0) {}
        }
    }
}

impl<T> Deliverable for WakeQueue<T> {
    type Item = T;
    type Failure = io::Error;
    fn deliver(&self, item: T) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed || state.pending.len() == state.capacity {
            state.closed = true;
            state.pending.clear();
            drop(state);
            self.wake()?;
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "subscriber closed or overflowed"));
        }
        state.pending.push_back(item);
        drop(state);
        self.wake()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    #[test]
    fn quiet_eof_wakes_without_a_timer() {
        let (client, peer) = UnixStream::pair().unwrap();
        let queue = Arc::new(WakeQueue::<()>::new(2).unwrap());
        let worker = Arc::clone(&queue);
        let (done, result) = mpsc::channel();
        let thread = std::thread::spawn(move || done.send(worker.receive(&peer).unwrap()).unwrap());
        drop(client);
        assert_eq!(result.recv_timeout(Duration::from_secs(2)).unwrap(), None);
        thread.join().unwrap();
    }
    #[test]
    fn queue_notification_and_overflow_are_explicit() {
        let (_client, peer) = UnixStream::pair().unwrap();
        let queue = WakeQueue::new(1).unwrap();
        queue.deliver(7).unwrap();
        assert_eq!(queue.receive(&peer).unwrap(), Some(7));
        queue.deliver(8).unwrap();
        assert!(queue.deliver(9).is_err());
        assert_eq!(queue.receive(&peer).unwrap(), None);
    }
}
