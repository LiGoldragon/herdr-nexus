use crate::transport_types::{Listener, ListenerPublication, ListenerPublished, PublicationChannel, PublicationDescriptor, SocketIdentity};
use shared_translator::{ByteViewable, Restorable, Signal, Signalizable};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::time::Duration;

const MAXIMUM_PACKET_BYTES: usize = 4096;

pub struct ParentChannel { descriptor: RawFd, owned: OwnedFd }
pub struct ChildChannel { descriptor: RawFd, owned: OwnedFd }

pub fn pair() -> Result<(ParentChannel, ChildChannel), String> {
    let (parent, child) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::SeqPacket,
        None,
        nix::sys::socket::SockFlag::SOCK_CLOEXEC,
    ).map_err(|error| format!("create listener publication channel: {error}"))?;
    Ok((
        ParentChannel { descriptor: parent.as_raw_fd(), owned: parent },
        ChildChannel { descriptor: child.as_raw_fd(), owned: child },
    ))
}

impl ParentChannel {
    pub fn set_deadline(&self, timeout: Duration) -> Result<(), String> {
        let timeout = nix::sys::time::TimeVal::new(
            timeout.as_secs().try_into().map_err(|_| "publication timeout seconds")?,
            timeout.subsec_micros().into(),
        );
        nix::sys::socket::setsockopt(&self.owned, nix::sys::socket::sockopt::ReceiveTimeout, &timeout)
            .map_err(|error| format!("bound publication receive timeout: {error}"))?;
        nix::sys::socket::setsockopt(&self.owned, nix::sys::socket::sockopt::SendTimeout, &timeout)
            .map_err(|error| format!("bound publication send timeout: {error}"))
    }

    pub fn receive(&self) -> Result<ListenerPublication, String> { receive_signal(self.descriptor) }

    pub fn try_receive(&self) -> Result<Option<ListenerPublication>, String> {
        let mut bytes = vec![0_u8; MAXIMUM_PACKET_BYTES];
        match nix::sys::socket::recv(self.descriptor, &mut bytes, nix::sys::socket::MsgFlags::MSG_DONTWAIT) {
            Ok(0) => Err("listener publication channel closed".into()),
            Ok(count) => {
                bytes.truncate(count);
                Signal::<ListenerPublication>::from(bytes).restore().map(Some)
                    .map_err(|error| format!("invalid listener publication: {error:?}"))
            }
            Err(nix::errno::Errno::EAGAIN) => Ok(None),
            Err(error) => Err(format!("receive listener publication: {error}")),
        }
    }

    pub fn acknowledge(&self, listener: Listener) -> Result<(), String> {
        send_signal(self.descriptor, &ListenerPublished(listener))
    }
}

impl ChildChannel {
    pub fn prepare_for_spawn(&self) -> Result<PublicationChannel, String> {
        rustix::io::fcntl_setfd(&self.owned, rustix::io::FdFlags::empty())
            .map_err(|error| format!("make publication channel inheritable: {error}"))?;
        Ok(PublicationChannel(PublicationDescriptor(i64::from(self.descriptor))))
    }
}

pub struct InheritedChannel { descriptor: RawFd, owned: OwnedFd, timeout: Duration }

impl InheritedChannel {
    pub fn validate(channel: &PublicationChannel, timeout: Duration) -> Result<Self, String> {
        let descriptor = i32::try_from(channel.0.0).ok().filter(|value| *value >= 3)
            .ok_or_else(|| "publication descriptor is outside the inherited range".to_owned())?;
        nix::sys::socket::getsockname::<nix::sys::socket::UnixAddr>(descriptor)
            .map_err(|error| format!("publication descriptor is not a Unix socket: {error}"))?;
        let retained = duplicate_inherited_descriptor(descriptor)?;
        let socket_type = nix::sys::socket::getsockopt(&retained, nix::sys::socket::sockopt::SockType)
            .map_err(|error| format!("inspect publication descriptor socket type: {error}"))?;
        if socket_type != nix::sys::socket::SockType::SeqPacket {
            return Err("publication descriptor is not a Unix sequenced-packet socket".into());
        }
        nix::unistd::close(descriptor)
            .map_err(|error| format!("close inherited publication descriptor: {error}"))?;
        Ok(Self { descriptor: retained.as_raw_fd(), owned: retained, timeout })
    }

    pub fn publish(&self, listener: Listener, socket_identity: SocketIdentity) -> Result<(), String> {
        send_signal(self.descriptor, &ListenerPublication { listener: listener.clone(), socket_identity })?;
        let acknowledged: ListenerPublished = receive_signal_bounded(self.descriptor, self.timeout)?;
        if acknowledged.0 != listener {
            return Err("listener acknowledgement names another listener".into());
        }
        Ok(())
    }
}

#[allow(unsafe_code)]
fn duplicate_inherited_descriptor(descriptor: RawFd) -> Result<OwnedFd, String> {
    // The pair parent created this private endpoint, made it inheritable only for this child,
    // and retains the peer until bootstrap finishes. This runs during single-threaded bootstrap,
    // before any owner can close or replace the inherited descriptor. The borrow exists only for
    // the kernel duplication call; the returned OwnedFd is the retained lifetime authority.
    let inherited = unsafe { std::os::fd::BorrowedFd::borrow_raw(descriptor) };
    rustix::io::fcntl_dupfd_cloexec(inherited, 3)
        .map_err(|error| format!("retain publication descriptor: {error}"))
}

fn send_signal<T: Signalizable>(descriptor: RawFd, value: &T) -> Result<(), String> {
    let signal = value.signalize().map_err(|error| format!("listener publication signal: {error:?}"))?;
    let bytes = signal.bytes();
    if bytes.len() > MAXIMUM_PACKET_BYTES { return Err("listener publication exceeds private packet bound".into()); }
    let sent = nix::sys::socket::send(descriptor, bytes, nix::sys::socket::MsgFlags::MSG_NOSIGNAL)
        .map_err(|error| format!("send listener publication: {error}"))?;
    if sent != bytes.len() { return Err("listener publication packet was truncated".into()); }
    Ok(())
}

fn receive_signal<T: Restorable>(descriptor: RawFd) -> Result<T, String> {
    let mut bytes = vec![0_u8; MAXIMUM_PACKET_BYTES];
    let count = nix::sys::socket::recv(descriptor, &mut bytes, nix::sys::socket::MsgFlags::empty())
        .map_err(|error| format!("receive listener publication: {error}"))?;
    if count == 0 { return Err("listener publication channel closed".into()); }
    bytes.truncate(count);
    Signal::<T>::from(bytes).restore().map_err(|error| format!("invalid listener publication: {error:?}"))
}

fn receive_signal_bounded<T: Restorable>(descriptor: RawFd, timeout: Duration) -> Result<T, String> {
    let deadline = std::time::Instant::now().checked_add(timeout)
        .ok_or_else(|| "listener publication deadline overflow".to_owned())?;
    loop {
        let mut bytes = vec![0_u8; MAXIMUM_PACKET_BYTES];
        match nix::sys::socket::recv(descriptor, &mut bytes, nix::sys::socket::MsgFlags::MSG_DONTWAIT) {
            Ok(0) => return Err("listener publication channel closed".into()),
            Ok(count) => {
                bytes.truncate(count);
                return Signal::<T>::from(bytes).restore()
                    .map_err(|error| format!("invalid listener publication: {error:?}"));
            }
            Err(nix::errno::Errno::EAGAIN) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(nix::errno::Errno::EAGAIN) => return Err("listener publication acknowledgement timed out".into()),
            Err(error) => return Err(format!("receive listener publication: {error}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport_types::{DeviceId, InodeId};
    use std::os::fd::{AsRawFd, IntoRawFd};

    fn channel_for_owned(descriptor: OwnedFd) -> PublicationChannel {
        PublicationChannel(PublicationDescriptor(i64::from(descriptor.into_raw_fd())))
    }

    fn identity() -> SocketIdentity {
        SocketIdentity { device_id: DeviceId(7), inode_id: InodeId(11) }
    }

    #[test]
    fn inherited_channel_refuses_descriptors_outside_inherited_range() {
        for descriptor in [-1, 0, 1, 2, i64::from(i32::MAX) + 1] {
            let error = InheritedChannel::validate(
                &PublicationChannel(PublicationDescriptor(descriptor)),
                Duration::from_millis(10),
            ).err().expect("descriptor must be refused");
            assert!(error.contains("outside the inherited range"), "{error}");
        }
    }

    #[test]
    fn inherited_channel_refuses_closed_descriptor() {
        let (_, child) = pair().expect("publication pair");
        let channel = child.prepare_for_spawn().expect("child descriptor");
        drop(child);
        let error = InheritedChannel::validate(&channel, Duration::from_millis(10))
            .err().expect("closed descriptor must be refused");
        assert!(error.contains("not a Unix socket"), "{error}");
    }

    #[test]
    fn inherited_channel_refuses_unix_stream_socket() {
        let (left, right) = nix::sys::socket::socketpair(
            nix::sys::socket::AddressFamily::Unix,
            nix::sys::socket::SockType::Stream,
            None,
            nix::sys::socket::SockFlag::SOCK_CLOEXEC,
        ).expect("Unix stream pair");
        drop(left);
        let error = InheritedChannel::validate(
            &channel_for_owned(right),
            Duration::from_millis(10),
        ).err().expect("stream socket must be refused");
        assert!(error.contains("not a Unix sequenced-packet socket"), "{error}");
    }

    #[test]
    fn publication_round_trip_requires_matching_acknowledgement() {
        let (parent, child) = pair().expect("publication pair");
        let channel = child.prepare_for_spawn().expect("child descriptor");
        std::mem::forget(child);
        let inherited = InheritedChannel::validate(&channel, Duration::from_secs(1))
            .expect("validated inherited channel");
        let responder = std::thread::spawn(move || {
            let publication = parent.receive().expect("publication");
            assert_eq!(publication.listener, Listener::MetaNexus);
            assert_eq!(publication.socket_identity, identity());
            parent.acknowledge(Listener::MetaNexus).expect("acknowledgement");
        });
        inherited.publish(Listener::MetaNexus, identity()).expect("published listener");
        responder.join().expect("responder");
    }

    #[test]
    fn publication_refuses_wrong_acknowledgement() {
        let (parent, child) = pair().expect("publication pair");
        let channel = child.prepare_for_spawn().expect("child descriptor");
        std::mem::forget(child);
        let inherited = InheritedChannel::validate(&channel, Duration::from_secs(1))
            .expect("validated inherited channel");
        let responder = std::thread::spawn(move || {
            parent.receive().expect("publication");
            parent.acknowledge(Listener::PublicNexus).expect("wrong acknowledgement");
        });
        let error = inherited.publish(Listener::MetaNexus, identity())
            .expect_err("wrong acknowledgement must be refused");
        assert!(error.contains("another listener"), "{error}");
        responder.join().expect("responder");
    }

    #[test]
    fn publication_fails_closed_when_parent_disappears_before_acknowledgement() {
        let (parent, child) = pair().expect("publication pair");
        let channel = child.prepare_for_spawn().expect("child descriptor");
        std::mem::forget(child);
        let inherited = InheritedChannel::validate(&channel, Duration::from_millis(20))
            .expect("validated inherited channel");
        drop(parent);
        let error = inherited.publish(Listener::MetaNexus, identity())
            .expect_err("closed parent must fail publication");
        assert!(error.contains("send listener publication") || error.contains("channel closed"), "{error}");
    }
}
