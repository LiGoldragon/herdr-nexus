//! A bootstrap the daemon refuses must leave the filesystem as it was: the admission and the
//! place name are validated before the place is prepared, and preparation precedes any socket.
//! The bootstrap is raw archived bytes on stdin and is not an authorization.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use herdr_nexus_wire::transport_types::{
    Admission, CoreBootstrap, FrameCapacityBytes, FrameDeadlineMilliseconds, RetryDelayMilliseconds,
};
use shared_translator::{
    FrameCapacity, FrameWriting, Place, PlaceName, Runtime, Signalizable, State,
};
use shared_translator::{PeerIdentity, ProcessId, UserId};

struct Root(PathBuf);

impl Root {
    fn new(label: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("herdr-bootstrap-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("test root");
        Self(path)
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn place(root: &Path, name: &str) -> Place {
    Place {
        place_name: PlaceName(name.into()),
        runtime: Runtime(root.join("run/nexus/alpha").to_string_lossy().into_owned()),
        state: State(
            root.join("state/nexus/alpha")
                .to_string_lossy()
                .into_owned(),
        ),
    }
}

fn admission(deadline: i64) -> Admission {
    Admission {
        frame_deadline_milliseconds: FrameDeadlineMilliseconds(deadline),
        frame_capacity_bytes: FrameCapacityBytes(8_388_608),
        retry_delay_milliseconds: RetryDelayMilliseconds(25),
    }
}

/// Feed one bootstrap to the daemon and return whether it exited successfully.
fn refuse(bootstrap: CoreBootstrap) -> bool {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-nexus-daemon"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the daemon");
    let mut input = child.stdin.take().expect("daemon stdin");
    input
        .write_frame(
            &bootstrap.signalize().expect("archive the bootstrap"),
            FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES),
        )
        .expect("write the bootstrap frame");
    input.flush().expect("flush");
    drop(input);
    child.wait().expect("daemon status").success()
}

fn legacy_state(root: &Path) -> PathBuf {
    let legacy = root.join("state/metaflow/alpha");
    std::fs::create_dir_all(&legacy).expect("legacy state");
    std::fs::write(legacy.join("record"), b"kept").expect("record");
    legacy
}

fn bootstrap(place: Place, admission: Admission) -> CoreBootstrap {
    CoreBootstrap {
        place,
        admission,
        peer_identity: PeerIdentity {
            user_id: UserId(0),
            process_id: ProcessId(1),
        },
        instance_id: signal_herdr_nexus::InstanceId("instance".into()),
    }
}

fn untouched(root: &Path, legacy: &Path) {
    assert!(
        legacy.join("record").is_file(),
        "the legacy state is still in place"
    );
    assert!(
        !root.join("state/nexus").exists(),
        "no current state or lock directory was created"
    );
    assert!(
        !root.join("run").exists(),
        "no runtime directory was created"
    );
}

#[test]
fn an_invalid_admission_is_refused_before_the_place_is_prepared() {
    let root = Root::new("admission");
    let legacy = legacy_state(&root.0);
    assert!(!refuse(bootstrap(place(&root.0, "alpha"), admission(0))));
    untouched(&root.0, &legacy);
}

#[test]
fn an_invalid_place_name_is_refused_before_the_place_is_prepared() {
    let root = Root::new("name");
    let legacy = legacy_state(&root.0);
    assert!(!refuse(bootstrap(
        place(&root.0, "../escape"),
        admission(70_000)
    )));
    untouched(&root.0, &legacy);
}
