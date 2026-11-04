use herdr_nexus_translator::runner::{run, RunError};
use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn shell(
    script: &str,
    timeout: Duration,
    limit: usize,
) -> Result<herdr_nexus_translator::runner::Captured, RunError> {
    run(
        Path::new("/bin/sh"),
        &[OsString::from("-c"), OsString::from(script)],
        Instant::now().checked_add(timeout).expect("test deadline"),
        limit,
    )
}

#[test]
fn captures_both_streams_within_one_joint_limit() {
    let captured =
        shell("printf out; printf err >&2", Duration::from_secs(2), 6).expect("bounded capture");
    assert!(captured.status.success());
    assert_eq!(captured.stdout, b"out");
    assert_eq!(captured.stderr, b"err");
}

#[test]
fn rapid_child_exit_still_drains_stderr() {
    for _ in 0..64 {
        let captured = shell("printf refusal >&2; exit 7", Duration::from_secs(2), 64)
            .expect("rapid child capture");
        assert_eq!(captured.status.code(), Some(7));
        assert_eq!(captured.stderr, b"refusal");
    }
}

#[test]
fn crossing_joint_limit_kills_and_reaps_group() {
    let started = Instant::now();
    let result = shell(
        "printf 12345; printf 67890 >&2; sleep 30",
        Duration::from_secs(5),
        9,
    );
    assert!(matches!(result, Err(RunError::OutputLimit)));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn deadline_kills_group_with_background_child_holding_pipes() {
    let started = Instant::now();
    let result = shell("sleep 30 & wait", Duration::from_millis(50), 1024);
    assert!(matches!(result, Err(RunError::Deadline)));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn spawn_failure_is_typed() {
    let result = run(
        Path::new("/definitely/not/a/program"),
        &[],
        Instant::now() + Duration::from_secs(1),
        1,
    );
    assert!(matches!(result, Err(RunError::Spawn(_))));
}

#[test]
fn termination_leaves_a_process_outside_the_owned_group_alive() {
    struct Sentinel(Child);
    impl Drop for Sentinel {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let mut command = Command::new("sleep");
    command.arg("30").process_group(0);
    let mut sentinel = Sentinel(command.spawn().expect("start outside sentinel"));
    let result = shell("sleep 30 & wait", Duration::from_millis(50), 1024);

    assert!(matches!(result, Err(RunError::Deadline)));
    assert!(sentinel.0.try_wait().expect("inspect sentinel").is_none());
}
