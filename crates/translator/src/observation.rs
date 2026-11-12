//! Herdr JSON stays in this adapter. Cues invalidate; they never assert state/readiness.
use crate::server::Runtime;
use herdr_nexus_wire::observation_ports::{Cancellable, Deliverable, SourceObservable};
use herdr_nexus_wire::observation_stream::{
    Invalidated_Data, InvalidationCause, ObservationScope, Observe_Data, Response,
};
use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use signal_herdr_nexus::observation_status::{FailureMessage, SocketPath, SourceFailure};
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Instant;

pub struct HerdrObservationAdapter<'a> {
    pub runtime: &'a Runtime,
}

fn protocol(message: impl Into<String>) -> SourceFailure {
    SourceFailure::ProtocolRefused(message.into())
}
fn transport(path: &std::path::Path, error: impl std::fmt::Display) -> SourceFailure {
    use signal_herdr_nexus::observation_status::TransportRefused_Data;
    SourceFailure::TransportRefused(TransportRefused_Data {
        socket_path: SocketPath(path.display().to_string()),
        failure_message: FailureMessage(error.to_string()),
    })
}

/// A socket wait answers readiness/EOF/cancellation, never a discovery timer.
fn wait<C: Cancellable>(
    stream: &UnixStream,
    interest: PollFlags,
    cancellation: &C,
    deadline: Option<Instant>,
) -> Result<bool, SourceFailure> {
    loop {
        let timeout = match deadline {
            Some(deadline) => {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(SourceFailure::DeadlineExpired)?;
                PollTimeout::try_from(remaining)
                    .map_err(|_| protocol("unrepresentable transport deadline"))?
            }
            None => PollTimeout::NONE,
        };
        let mut fds = [
            PollFd::new(stream.as_fd(), interest),
            PollFd::new(cancellation.descriptor(), PollFlags::POLLIN),
        ];
        match poll(&mut fds, timeout) {
            Err(nix::errno::Errno::EINTR) => continue,
            Err(error) => return Err(protocol(error.to_string())),
            Ok(0) => return Err(SourceFailure::DeadlineExpired),
            Ok(_) => (),
        }
        if fds[1].revents().is_some_and(|flags| !flags.is_empty()) {
            return Ok(false);
        }
        if fds[0].revents().is_some_and(|flags| !flags.is_empty()) {
            return Ok(true);
        }
    }
}

impl SourceObservable for HerdrObservationAdapter<'_> {
    type Request = Observe_Data;
    type Observation = Response;
    type Failure = SourceFailure;
    fn observe<D, C>(
        &self,
        request: Observe_Data,
        delivery: &D,
        cancellation: &C,
    ) -> Result<(), SourceFailure>
    where
        D: Deliverable<Item = Response, Failure = SourceFailure>,
        C: Cancellable,
    {
        if request.source_generation.0 <= 0 {
            return Err(protocol("nonpositive observation generation"));
        }
        // One demand-caused bounded discovery call; never repeat it while quiet.
        let arguments = ["session", "list", "--json"].map(std::ffi::OsString::from);
        let deadline = Instant::now()
            .checked_add(self.runtime.call_timeout)
            .ok_or(SourceFailure::DeadlineExpired)?;
        let captured = crate::runner::run(
            &self.runtime.executable,
            &arguments,
            deadline,
            self.runtime.output_limit,
        )
        .map_err(|error| protocol(error.to_string()))?;
        if !captured.status.success() {
            return Err(protocol(
                String::from_utf8_lossy(&captured.stderr).into_owned(),
            ));
        }
        let listing: serde_json::Value = serde_json::from_slice(&captured.stdout)
            .map_err(|error| protocol(error.to_string()))?;
        let path = listing
            .get("sessions")
            .and_then(|value| value.as_array())
            .and_then(|sessions| {
                sessions.iter().find(|entry| {
                    entry.get("name").and_then(|v| v.as_str()) == Some(&request.session.0)
                })
            })
            .and_then(|entry| entry.get("socket_path"))
            .and_then(|value| value.as_str())
            .map(PathBuf::from)
            .ok_or_else(|| protocol("session listing omitted the requested socket"))?;
        use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, UnixAddr};
        let descriptor = socket(
            AddressFamily::Unix,
            SockType::Stream,
            SockFlag::SOCK_NONBLOCK | SockFlag::SOCK_CLOEXEC,
            None,
        )
        .map_err(|error| transport(&path, error))?;
        connect(
            descriptor.as_raw_fd(),
            &UnixAddr::new(&path).map_err(|error| transport(&path, error))?,
        )
        .map_err(|error| transport(&path, error))?;
        let stream = UnixStream::from(descriptor);
        self.observe_connected(stream, request, delivery, cancellation)
    }
}

impl HerdrObservationAdapter<'_> {
    /// Connected boundary also permits a real socket producer in isolated tests.
    pub fn observe_connected<D, C>(
        &self,
        mut stream: UnixStream,
        request: Observe_Data,
        delivery: &D,
        cancellation: &C,
    ) -> Result<(), SourceFailure>
    where
        D: Deliverable<Item = Response, Failure = SourceFailure>,
        C: Cancellable,
    {
        if request.source_generation.0 <= 0 {
            return Err(protocol("nonpositive observation generation"));
        }
        stream
            .set_nonblocking(true)
            .map_err(|error| protocol(error.to_string()))?;
        let generation = request.source_generation;
        let mut selections = [
            "pane.created",
            "pane.closed",
            "pane.updated",
            "pane.moved",
            "pane.exited",
            "pane.agent_detected",
        ]
        .into_iter()
        .map(|kind| serde_json::json!({"type":kind}))
        .collect::<Vec<_>>();
        let panes = request
            .pane_id_vector
            .into_iter()
            .map(|p| p.0)
            .collect::<std::collections::BTreeSet<_>>();
        selections.extend(
            panes
                .into_iter()
                .map(|pane| serde_json::json!({"type":"pane.agent_status_changed","pane_id":pane})),
        );
        let text = format!(
            "{}\n",
            serde_json::json!({"id":generation.0,"method":"events.subscribe","params":{"subscriptions":selections}})
        );
        if text.len() > self.runtime.output_limit {
            return Err(SourceFailure::ResourceLimit(
                "subscription request exceeds output limit".into(),
            ));
        }
        let mut remaining = text.as_bytes();
        let attach_deadline = Instant::now()
            .checked_add(self.runtime.call_timeout)
            .ok_or(SourceFailure::DeadlineExpired)?;
        while !remaining.is_empty() {
            if !wait(
                &stream,
                PollFlags::POLLOUT,
                cancellation,
                Some(attach_deadline),
            )? {
                return Ok(());
            }
            match stream.write(remaining) {
                Ok(0) => return Err(SourceFailure::Closed),
                Ok(count) => remaining = &remaining[count..],
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                Err(error) => return Err(protocol(error.to_string())),
            }
        }
        let mut attached = false;
        let mut pending = Vec::new();
        let mut progress_deadline = None;
        loop {
            let deadline = if attached {
                progress_deadline
            } else {
                Some(attach_deadline)
            };
            if !wait(&stream, PollFlags::POLLIN, cancellation, deadline)? {
                return Ok(());
            }
            let mut chunk = [0; 8192];
            let count = match stream.read(&mut chunk) {
                Ok(0) => return Err(SourceFailure::Closed),
                Ok(count) => count,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                Err(error) => return Err(protocol(error.to_string())),
            };
            if pending.is_empty() {
                progress_deadline = Instant::now().checked_add(self.runtime.call_timeout);
            }
            if pending.len().saturating_add(count) > self.runtime.output_limit {
                return Err(SourceFailure::ResourceLimit(
                    "Herdr event buffer exceeded limit".into(),
                ));
            }
            pending.extend_from_slice(&chunk[..count]);
            while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                let line = pending.drain(..=end).collect::<Vec<_>>();
                let value: serde_json::Value =
                    serde_json::from_slice(&line).map_err(|error| protocol(error.to_string()))?;
                if !attached {
                    if value.get("id").and_then(|id| id.as_i64()) != Some(generation.0)
                        || value.pointer("/result/type").and_then(|v| v.as_str())
                            != Some("subscription_started")
                    {
                        return Err(protocol("Herdr did not acknowledge this subscription"));
                    }
                    attached = true;
                    delivery.deliver(Response::Attached(generation))?;
                } else {
                    let kind = value
                        .get("event")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| protocol("event kind missing"))?;
                    let cause = match kind {
                        "pane.created" => InvalidationCause::Created,
                        "pane.closed" => InvalidationCause::Closed,
                        "pane.exited" => InvalidationCause::Exited,
                        "pane.agent_detected" => InvalidationCause::DetectionChanged,
                        "pane.agent_status_changed" => InvalidationCause::StatusChanged,
                        "pane.updated" => InvalidationCause::PresentationChanged,
                        "pane.moved" => InvalidationCause::Moved,
                        _ => return Err(protocol(format!("unrequested event: {kind}"))),
                    };
                    let scope = value
                        .pointer("/data/pane_id")
                        .and_then(|v| v.as_str())
                        .map(|pane| {
                            ObservationScope::PaneId(signal_herdr_nexus::PaneId(pane.into()))
                        })
                        .unwrap_or(ObservationScope::WholeSession);
                    delivery.deliver(Response::Invalidated(Invalidated_Data {
                        source_generation: generation,
                        observation_scope: scope,
                        invalidation_cause: cause,
                    }))?;
                }
            }
            if pending.is_empty() {
                progress_deadline = None;
            }
        }
    }
}

pub struct PeerCancellation<'a>(pub &'a UnixStream);
impl Cancellable for PeerCancellation<'_> {
    fn descriptor(&self) -> std::os::fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

struct FrameDelivery<'a> {
    peer: &'a UnixStream,
    capacity: usize,
    timeout: std::time::Duration,
}
impl Deliverable for FrameDelivery<'_> {
    type Item = Response;
    type Failure = SourceFailure;
    fn deliver(&self, response: Response) -> Result<(), SourceFailure> {
        use signal::{ByteViewable, Signalizable};
        let bytes = response
            .signalize()
            .map_err(|error| protocol(format!("{error:?}")))?;
        let mut stream = herdr_nexus_wire::deadline::DeadlineStream::new(
            self.peer
                .try_clone()
                .map_err(|error| protocol(error.to_string()))?,
            self.timeout,
        )
        .map_err(|error| protocol(error.to_string()))?;
        herdr_nexus_wire::frame::write_frame(
            &mut stream,
            &herdr_nexus_wire::frame::Frame {
                lane: herdr_nexus_wire::transport_types::Lane::ObservationSignal,
                kind: herdr_nexus_wire::transport_types::Kind::Answered,
                payload: bytes.bytes().to_vec(),
            },
            self.capacity,
        )
        .map_err(|error| protocol(error.to_string()))
    }
}

pub fn serve(
    runtime: &Runtime,
    request: Observe_Data,
    peer: &UnixStream,
    capacity: usize,
) -> Result<(), SourceFailure> {
    let delivery = FrameDelivery {
        peer,
        capacity,
        timeout: runtime.call_timeout,
    };
    let generation = request.source_generation;
    let adapter = HerdrObservationAdapter { runtime };
    if let Err(failure) = adapter.observe(request, &delivery, &PeerCancellation(peer)) {
        delivery.deliver(Response::Detached(
            herdr_nexus_wire::observation_stream::Detached_Data {
                source_generation: generation,
                source_failure: failure,
            },
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Capture(Mutex<Vec<Response>>);
    impl Deliverable for Capture {
        type Item = Response;
        type Failure = SourceFailure;
        fn deliver(&self, value: Response) -> Result<(), SourceFailure> {
            self.0.lock().unwrap().push(value);
            Ok(())
        }
    }
    #[test]
    fn retained_close_only_invalidates_and_exit_is_not_dead() {
        use std::io::BufRead;
        let (source, mut producer) = UnixStream::pair().unwrap();
        let (_control, cancel) = UnixStream::pair().unwrap();
        let worker = std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(producer.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let request: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(
                request
                    .pointer("/params/subscriptions")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .len(),
                7
            );
            producer.write_all(b"{\"id\":1,\"result\":{\"type\":\"subscription_started\"}}\n{\"event\":\"pane.closed\",\"data\":{\"pane_id\":\"p\"}}\n{\"event\":\"pane.exited\",\"data\":{\"pane_id\":\"p\"}}\n").unwrap();
        });
        let runtime =
            Runtime::new("/unused".into(), std::time::Duration::from_secs(2), 16384).unwrap();
        let capture = Capture(Mutex::new(vec![]));
        let result = HerdrObservationAdapter { runtime: &runtime }.observe_connected(
            source,
            Observe_Data {
                session: signal_herdr_nexus::Session("main".into()),
                source_generation: signal_herdr_nexus::SourceGeneration(1),
                pane_id_vector: vec![signal_herdr_nexus::PaneId("p".into())],
            },
            &capture,
            &PeerCancellation(&cancel),
        );
        assert_eq!(result, Err(SourceFailure::Closed));
        worker.join().unwrap();
        let seen = capture.0.into_inner().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(matches!(seen[0], Response::Attached(_)));
        assert!(seen[1..]
            .iter()
            .all(|value| matches!(value, Response::Invalidated(_))));
    }
}
