//! Ownership and lifecycle of the Herdr backend and paired Nexus processes.

use crate::generated::backend_deployment::*;
use crate::runner::{self, CommandEnvironment};
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, Mode, OFlags};
use rustix::process::{Pid as RustixPid, PidfdFlags};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const REMOVED_HERDR_ENVIRONMENT: [&str; 4] = [
    "HERDR_CONFIG_PATH",
    "HERDR_SOCKET_PATH",
    "HERDR_CLIENT_SOCKET_PATH",
    "HERDR_SESSION",
];

/// Restore the signal mask inherited before the pair blocked its owned stop
/// signals. This is the only pre-exec boundary.
pub fn restore_child_signal_mask(command: &mut Command, inherited: nix::sys::signal::SigSet) {
    // SAFETY: the closure executes after fork and before exec, when the child
    // is single-threaded. nix::pthread_sigmask directly invokes the
    // async-signal-safe pthread_sigmask primitive; the captured SigSet owns
    // its data and the closure allocates, logs, and locks nothing.
    unsafe {
        command.pre_exec(move || {
            nix::sys::signal::pthread_sigmask(
                nix::sys::signal::SigmaskHow::SIG_SETMASK,
                Some(&inherited),
                None,
            )
            .map_err(|error| std::io::Error::from_raw_os_error(error as i32))
        });
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedDeployment {
    pub executable: PathBuf,
    pub configuration_root: Vec<String>,
    pub state_root: Vec<String>,
    pub claim_name: String,
    pub readiness_timeout: Duration,
    pub probe_timeout: Duration,
    pub stop_timeout: Duration,
    pub poll_interval: Duration,
    pub output_limit: usize,
}

impl TryFrom<BackendDeployment> for ValidatedDeployment {
    type Error = String;

    fn try_from(value: BackendDeployment) -> Result<Self, Self::Error> {
        let executable = PathBuf::from(value.backend_executable.0);
        if !executable.is_absolute() {
            return Err("backend executable must be absolute".into());
        }
        let executable = fs::canonicalize(&executable)
            .map_err(|error| format!("backend executable is unavailable: {error}"))?;
        if !fs::metadata(&executable)
            .map_err(|error| format!("backend executable metadata failed: {error}"))?
            .is_file()
        {
            return Err("backend executable is not a file".into());
        }
        let configuration_root = components(value.configuration_root.0, "configuration root")?;
        let state_root = components(value.backend_state_root.0, "backend state root")?;
        let claim_name = component(value.claim_name.0.0, "claim name")?;
        Ok(Self {
            executable,
            configuration_root,
            state_root,
            claim_name,
            readiness_timeout: positive_milliseconds(value.readiness_timeout.0.0, "readiness timeout")?,
            probe_timeout: positive_milliseconds(value.probe_timeout.0.0, "probe timeout")?,
            stop_timeout: positive_milliseconds(value.stop_timeout.0.0, "stop timeout")?,
            poll_interval: positive_milliseconds(value.poll_interval.0.0, "poll interval")?,
            output_limit: usize::try_from(value.output_limit.0.0)
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| "output limit must be positive".to_owned())?,
        })
    }
}

fn positive_milliseconds(value: i64, name: &str) -> Result<Duration, String> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .ok_or_else(|| format!("{name} must be positive"))
}

fn components(values: Vec<RootComponent>, name: &str) -> Result<Vec<String>, String> {
    if values.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    values
        .into_iter()
        .map(|value| component(value.0, name))
        .collect()
}

fn component(value: String, name: &str) -> Result<String, String> {
    let mut parts = Path::new(&value).components();
    match (parts.next(), parts.next()) {
        (Some(Component::Normal(part)), None) if part == OsStr::new(&value) => Ok(value),
        _ => Err(format!("{name} contains a non-normal component")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }

    fn stat(stat: &rustix::fs::Stat) -> Self {
        Self {
            device: stat.st_dev,
            inode: stat.st_ino,
        }
    }
}

pub struct AnchoredEnvironment {
    _state_root: OwnedFd,
    _configuration: OwnedFd,
    _state: OwnedFd,
    pub command: CommandEnvironment,
    pub configuration_path: PathBuf,
    pub state_path: PathBuf,
}

impl AnchoredEnvironment {
    pub fn open(place_state: &Path, deployment: &ValidatedDeployment) -> Result<Self, String> {
        let state_root = rustix::fs::open(
            place_state,
            OFlags::RDONLY | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map_err(|error| format!("open Place.State: {error}"))?;
        let configuration = descend(&state_root, &deployment.configuration_root)?;
        let state = descend(&state_root, &deployment.state_root)?;
        // These two descriptors are the authority carried through exec.  They
        // deliberately remain open in the backend and translator descendants.
        rustix::io::fcntl_setfd(&configuration, rustix::io::FdFlags::empty())
            .map_err(|error| format!("inherit configuration root: {error}"))?;
        rustix::io::fcntl_setfd(&state, rustix::io::FdFlags::empty())
            .map_err(|error| format!("inherit backend state root: {error}"))?;
        let configuration_path = PathBuf::from(format!("/proc/self/fd/{}", configuration.as_raw_fd()));
        let state_path = PathBuf::from(format!("/proc/self/fd/{}", state.as_raw_fd()));
        Ok(Self {
            _state_root: state_root,
            _configuration: configuration,
            _state: state,
            command: CommandEnvironment {
                configuration_home: configuration_path.as_os_str().to_owned(),
                state_home: state_path.as_os_str().to_owned(),
            },
            configuration_path,
            state_path,
        })
    }

    pub fn apply(&self, command: &mut Command) {
        command
            .env("XDG_CONFIG_HOME", &self.command.configuration_home)
            .env("XDG_STATE_HOME", &self.command.state_home);
        for name in REMOVED_HERDR_ENVIRONMENT {
            command.env_remove(name);
        }
    }

    pub fn backend_roots(&self) -> Result<crate::transport_types::BackendRoots, String> {
        use crate::transport_types::*;
        fn inherited(fd: &OwnedFd) -> Result<InheritedDirectory, String> {
            let stat = rustix::fs::fstat(fd)
                .map_err(|error| format!("inspect inherited backend root: {error}"))?;
            let descriptor = i64::from(fd.as_raw_fd());
            let device = integer_identity(stat.st_dev, "backend root device id")?;
            let inode = integer_identity(stat.st_ino, "backend root inode id")?;
            Ok(InheritedDirectory {
                directory_descriptor: DirectoryDescriptor(descriptor),
                directory_identity: DirectoryIdentity {
                    device_id: DeviceId(device),
                    inode_id: InodeId(inode),
                },
            })
        }
        Ok(BackendRoots {
            configuration_home: ConfigurationHome(inherited(&self._configuration)?),
            backend_state_home: BackendStateHome(inherited(&self._state)?),
        })
    }
}

fn integer_identity(value: u64, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{name} exceeds nonnegative Integer"))
}

fn descend(root: &OwnedFd, components: &[String]) -> Result<OwnedFd, String> {
    let mut current = rustix::io::fcntl_dupfd_cloexec(root, 3)
        .map_err(|error| format!("duplicate Place.State: {error}"))?;
    for component in components {
        match rustix::fs::mkdirat(&current, component.as_str(), Mode::from_raw_mode(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(format!("create confined root component {component}: {error}")),
        }
        current = rustix::fs::openat(
            &current,
            component.as_str(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| format!("open confined root component {component}: {error}"))?;
    }
    Ok(current)
}

pub struct RuntimeClaim {
    runtime: OwnedFd,
    claim: OwnedFd,
    name: String,
    identity: FileIdentity,
}

impl RuntimeClaim {
    pub fn acquire(runtime: &Path, name: &str) -> Result<Self, String> {
        let runtime = rustix::fs::open(
            runtime,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| format!("open Place.Runtime: {error}"))?;
        let claim = rustix::fs::openat(
            &runtime,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|error| format!("backend ownership claim refused: {error}"))?;
        let identity = FileIdentity::stat(
            &rustix::fs::fstat(&claim).map_err(|error| format!("inspect backend claim: {error}"))?,
        );
        Ok(Self { runtime, claim, name: name.to_owned(), identity })
    }

    pub fn refuse_existing(&self, name: &str) -> Result<(), String> {
        match rustix::fs::statat(&self.runtime, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => Err(format!("pair socket path already exists: {name}")),
            Err(rustix::io::Errno::NOENT) => Ok(()),
            Err(error) => Err(format!("inspect pair socket {name}: {error}")),
        }
    }

    pub fn capture_socket(&self, name: &str) -> Result<Option<OwnedRuntimeSocket>, String> {
        let stat = match rustix::fs::statat(&self.runtime, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(format!("inspect pair socket {name}: {error}")),
        };
        if stat.st_mode & libc::S_IFMT != libc::S_IFSOCK {
            return Err(format!("pair path is not a socket: {name}"));
        }
        let runtime = rustix::io::fcntl_dupfd_cloexec(&self.runtime, 3)
            .map_err(|error| format!("retain Place.Runtime for socket ownership: {error}"))?;
        Ok(Some(OwnedRuntimeSocket {
            runtime,
            name: name.to_owned(),
            identity: FileIdentity::stat(&stat),
        }))
    }

    pub fn capture_published_socket(
        &self,
        name: &str,
        expected: &herdr_nexus_wire::transport_types::SocketIdentity,
    ) -> Result<OwnedRuntimeSocket, String> {
        if expected.device_id.0 < 0 || expected.inode_id.0 < 0 {
            return Err(format!("listener publication has negative identity: {name}"));
        }
        let captured = self
            .capture_socket(name)?
            .ok_or_else(|| format!("published listener path is absent: {name}"))?;
        let device = u64::try_from(expected.device_id.0)
            .map_err(|_| format!("listener device is not representable: {name}"))?;
        let inode = u64::try_from(expected.inode_id.0)
            .map_err(|_| format!("listener inode is not representable: {name}"))?;
        if captured.identity.device != device || captured.identity.inode != inode {
            return Err(format!("published listener identity differs at path: {name}"));
        }
        Ok(captured)
    }

    fn still_owned(&self) -> bool {
        rustix::fs::statat(&self.runtime, self.name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .map(|stat| FileIdentity::stat(&stat) == self.identity)
            .unwrap_or(false)
            && rustix::fs::fstat(&self.claim)
                .map(|stat| FileIdentity::stat(&stat) == self.identity)
                .unwrap_or(false)
    }
}

pub struct OwnedRuntimeSocket {
    runtime: OwnedFd,
    name: String,
    identity: FileIdentity,
}

impl OwnedRuntimeSocket {
    pub fn remove_if_owned(&self) {
        let owned = rustix::fs::statat(&self.runtime, self.name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .map(|stat| {
                stat.st_mode & libc::S_IFMT == libc::S_IFSOCK
                    && FileIdentity::stat(&stat) == self.identity
            })
            .unwrap_or(false);
        if owned {
            let _ = rustix::fs::unlinkat(&self.runtime, self.name.as_str(), AtFlags::empty());
        }
    }
}

impl Drop for RuntimeClaim {
    fn drop(&mut self) {
        if self.still_owned() {
            let _ = rustix::fs::unlinkat(&self.runtime, self.name.as_str(), AtFlags::empty());
        }
    }
}

#[derive(Debug)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub parent_pid: u32,
    pub user_id: u32,
    pub start_time: u64,
    pub executable: PathBuf,
}

pub struct OwnedChild {
    pub child: Child,
    pidfd: OwnedFd,
    pub identity: ProcessIdentity,
}

impl OwnedChild {
    pub fn spawn(command: &mut Command, expected: &Path) -> Result<Self, String> {
        let child = command.spawn().map_err(|error| format!("spawn owned child: {error}"))?;
        match Self::capture(child, expected) {
            Ok(child) => Ok(child),
            Err((error, mut child)) => {
                let _ = child.kill();
                let _ = child.wait();
                Err(error)
            }
        }
    }

    fn capture(child: Child, expected: &Path) -> Result<Self, (String, Child)> {
        let pid = child.id();
        let result = (|| {
            let executable = fs::read_link(format!("/proc/{pid}/exe"))
                .map_err(|error| format!("read child executable: {error}"))?;
            let expected = fs::canonicalize(expected)
                .map_err(|error| format!("canonicalize expected child executable: {error}"))?;
            if executable != expected {
                return Err(format!(
                    "owned child executable mismatch: expected {}, observed {}",
                    expected.display(), executable.display()
                ));
            }
            let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
                .map_err(|error| format!("read child stat: {error}"))?;
            let (_, fields) = stat
                .rsplit_once(") ")
                .ok_or_else(|| "child stat format".to_owned())?;
            let fields: Vec<&str> = fields.split_whitespace().collect();
            let parent_pid: u32 = fields.get(1).ok_or("child stat parent")?.parse()
                .map_err(|_| "child stat parent".to_owned())?;
            let start_time: u64 = fields.get(19).ok_or("child stat start time")?.parse()
                .map_err(|_| "child stat start time".to_owned())?;
            if parent_pid != std::process::id() {
                return Err("owned child parent mismatch".into());
            }
            let status = fs::read_to_string(format!("/proc/{pid}/status"))
                .map_err(|error| format!("read child status: {error}"))?;
            let user_id = status
                .lines()
                .find_map(|line| line.strip_prefix("Uid:")?.split_whitespace().next())
                .ok_or_else(|| "child status user id".to_owned())?
                .parse::<u32>()
                .map_err(|_| "child status user id".to_owned())?;
            if user_id != nix::unistd::Uid::current().as_raw() {
                return Err("owned child user mismatch".into());
            }
            let rustix_pid = RustixPid::from_raw(pid as i32)
                .ok_or_else(|| "child pid is invalid".to_owned())?;
            let pidfd = rustix::process::pidfd_open(rustix_pid, PidfdFlags::empty())
                .map_err(|error| format!("open child pidfd: {error}"))?;
            Ok((pidfd, ProcessIdentity { pid, parent_pid, user_id, start_time, executable }))
        })();
        match result {
            Ok((pidfd, identity)) => Ok(Self { child, pidfd, identity }),
            Err(error) => Err((error, child)),
        }
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, String> {
        self.child.try_wait().map_err(|error| format!("wait owned child: {error}"))
    }

    pub fn signal(&self, signal: rustix::process::Signal) -> Result<(), String> {
        rustix::process::pidfd_send_signal(&self.pidfd, signal)
            .map_err(|error| format!("signal owned child: {error}"))
    }

    pub fn wait_until(&mut self, deadline: Instant, poll: Duration) -> Result<Option<ExitStatus>, String> {
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(poll.min(deadline.saturating_duration_since(Instant::now())));
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackendSockets {
    pub api_path: PathBuf,
    pub client_path: PathBuf,
    api_identity: FileIdentity,
    client_identity: FileIdentity,
}

impl BackendSockets {
    pub fn expected(environment: &AnchoredEnvironment, session: &str) -> Self {
        let directory = environment.configuration_path.join("herdr").join("sessions").join(session);
        Self {
            api_path: directory.join("herdr.sock"),
            client_path: directory.join("herdr-client.sock"),
            api_identity: FileIdentity { device: 0, inode: 0 },
            client_identity: FileIdentity { device: 0, inode: 0 },
        }
    }

    pub fn refuse_existing(&self) -> Result<(), String> {
        for path in [&self.api_path, &self.client_path] {
            match fs::symlink_metadata(path) {
                Ok(_) => return Err(format!("existing backend socket refused: {}", path.display())),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("inspect backend socket {}: {error}", path.display())),
            }
        }
        Ok(())
    }

    pub fn capture(mut self) -> Result<Self, String> {
        let api = socket_metadata(&self.api_path)?;
        let client = socket_metadata(&self.client_path)?;
        self.api_identity = FileIdentity::metadata(&api);
        self.client_identity = FileIdentity::metadata(&client);
        Ok(self)
    }

    pub fn still_owned(&self) -> bool {
        socket_metadata(&self.api_path)
            .map(|value| FileIdentity::metadata(&value) == self.api_identity)
            .unwrap_or(false)
            && socket_metadata(&self.client_path)
                .map(|value| FileIdentity::metadata(&value) == self.client_identity)
                .unwrap_or(false)
    }
}

fn socket_metadata(path: &Path) -> Result<fs::Metadata, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect backend socket {}: {error}", path.display()))?;
    if !metadata.file_type().is_socket() {
        return Err(format!("backend path is not a socket: {}", path.display()));
    }
    Ok(metadata)
}

pub fn spawn_backend(
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    inherited_signal_mask: nix::sys::signal::SigSet,
) -> Result<OwnedChild, String> {
    let mut command = Command::new(&deployment.executable);
    command
        .args([OsStr::new("--session"), OsStr::new(session), OsStr::new("server")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    environment.apply(&mut command);
    restore_child_signal_mask(&mut command, inherited_signal_mask);
    OwnedChild::spawn(&mut command, &deployment.executable)
}

pub fn await_backend(
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    backend: &mut OwnedChild,
    expected: &BackendSockets,
    mut stop_requested: impl FnMut() -> Result<bool, String>,
) -> Result<bool, String> {
    let deadline = Instant::now()
        .checked_add(deployment.readiness_timeout)
        .ok_or_else(|| "readiness deadline overflow".to_owned())?;
    loop {
        if stop_requested()? {
            return Ok(false);
        }
        if let Some(status) = backend.try_wait()? {
            return Err(format!("backend exited before readiness: {status}"));
        }
        let probe_deadline = Instant::now()
            .checked_add(deployment.probe_timeout)
            .ok_or_else(|| "probe deadline overflow".to_owned())?;
        let arguments = [
            OsString::from("--session"),
            OsString::from(session),
            OsString::from("status"),
            OsString::from("server"),
            OsString::from("--json"),
        ];
        if let Ok(captured) = runner::run_with_environment(
            &deployment.executable,
            &arguments,
            probe_deadline,
            deployment.output_limit,
            &environment.command,
        ) {
            if captured.status.success()
                && ready_json(&captured.stdout, session, &expected.api_path)
                && expected.clone().capture().is_ok()
            {
                return Ok(true);
            }
        }
        if Instant::now() >= deadline {
            return Err("backend readiness timed out".into());
        }
        std::thread::sleep(deployment.poll_interval.min(deadline.saturating_duration_since(Instant::now())));
    }
}

fn ready_json(bytes: &[u8], session: &str, socket: &Path) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    value.get("running").and_then(|value| value.as_bool()) == Some(true)
        && value.get("compatible").and_then(|value| value.as_bool()) == Some(true)
        && value.get("session").and_then(|value| value.as_str()) == Some(session)
        && value.get("socket").and_then(|value| value.as_str()) == socket.to_str()
}

pub fn request_backend_stop(
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: &BackendSockets,
    deadline: Instant,
) -> Result<(), String> {
    if !sockets.still_owned() {
        return Err("backend socket ownership changed before stop".into());
    }
    let arguments = [
        OsString::from("--session"),
        OsString::from(session),
        OsString::from("server"),
        OsString::from("stop"),
    ];
    let captured = runner::run_with_environment(
        &deployment.executable,
        &arguments,
        deadline,
        deployment.output_limit,
        &environment.command,
    )
    .map_err(|error| format!("backend stop request failed: {error}"))?;
    if !captured.status.success() {
        return Err(format!("backend stop request exited {}", captured.status));
    }
    Ok(())
}

pub fn bounded_stop(child: &mut OwnedChild, timeout: Duration, poll: Duration) {
    let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
    if child.wait_until(deadline, poll).ok().flatten().is_some() {
        return;
    }
    let _ = child.signal(rustix::process::Signal::TERM);
    let deadline = Instant::now().checked_add(timeout).unwrap_or_else(Instant::now);
    if child.wait_until(deadline, poll).ok().flatten().is_some() {
        return;
    }
    let _ = child.signal(rustix::process::Signal::KILL);
    let _ = child.child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn deployment() -> BackendDeployment {
        BackendDeployment {
            backend_executable: BackendExecutable("/bin/true".into()),
            configuration_root: ConfigurationRoot(vec![RootComponent("backend".into()), RootComponent("config".into())]),
            backend_state_root: BackendStateRoot(vec![RootComponent("backend".into()), RootComponent("state".into())]),
            claim_name: ClaimName(RootComponent("backend.owner".into())),
            readiness_timeout: ReadinessTimeout(Milliseconds(30_000)),
            probe_timeout: ProbeTimeout(Milliseconds(1_000)),
            stop_timeout: StopTimeout(Milliseconds(5_000)),
            poll_interval: PollInterval(Milliseconds(25)),
            output_limit: OutputLimit(Bytes(1_048_576)),
        }
    }

    #[test]
    fn deployment_rejects_non_normal_and_non_positive_values() {
        let mut value = deployment();
        value.configuration_root.0[0].0 = "../escape".into();
        assert!(ValidatedDeployment::try_from(value).is_err());
        let mut value = deployment();
        value.poll_interval.0.0 = 0;
        assert!(ValidatedDeployment::try_from(value).is_err());
    }

    #[test]
    fn readiness_requires_all_owned_backend_fields() {
        let socket = Path::new("/proc/self/fd/7/herdr/sessions/main/herdr.sock");
        assert!(ready_json(
            br#"{"running":true,"compatible":true,"session":"main","socket":"/proc/self/fd/7/herdr/sessions/main/herdr.sock"}"#,
            "main",
            socket,
        ));
        for value in [
            br#"{"running":false,"compatible":true,"session":"main","socket":"/proc/self/fd/7/herdr/sessions/main/herdr.sock"}"#.as_slice(),
            br#"{"running":true,"compatible":false,"session":"main","socket":"/proc/self/fd/7/herdr/sessions/main/herdr.sock"}"#,
            br#"{"running":true,"compatible":true,"session":"other","socket":"/proc/self/fd/7/herdr/sessions/main/herdr.sock"}"#,
            br#"{"running":true,"compatible":true,"session":"main","socket":"/tmp/rival.sock"}"#,
            br#"not-json"#,
        ] {
            assert!(!ready_json(value, "main", socket));
        }
    }

    #[test]
    fn retained_root_descriptors_survive_named_path_replacement_at_use() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let fixture = std::env::temp_dir().join(format!(
            "herdr-backend-root-replacement-{}-{nonce}",
            std::process::id()
        ));
        let place_state = fixture.join("place-state");
        let rival = fixture.join("rival");
        fs::create_dir_all(&place_state).expect("place state");
        fs::create_dir_all(rival.join("configuration")).expect("rival configuration");
        fs::create_dir_all(rival.join("state")).expect("rival state");

        let deployment = ValidatedDeployment {
            executable: fs::canonicalize("/bin/sh").expect("native shell"),
            configuration_root: vec!["configuration".into()],
            state_root: vec!["backend-state".into()],
            claim_name: "backend.owner".into(),
            readiness_timeout: Duration::from_secs(1),
            probe_timeout: Duration::from_secs(1),
            stop_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
            output_limit: 1024,
        };
        let environment = AnchoredEnvironment::open(&place_state, &deployment).expect("open roots");

        let retained_configuration = place_state.join("retained-configuration");
        let retained_state = place_state.join("retained-state");
        fs::rename(place_state.join("configuration"), &retained_configuration)
            .expect("retain configuration");
        fs::rename(place_state.join("backend-state"), &retained_state).expect("retain state");
        symlink(rival.join("configuration"), place_state.join("configuration"))
            .expect("replace configuration");
        symlink(rival.join("state"), place_state.join("backend-state"))
            .expect("replace state");

        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "printf retained >\"$XDG_CONFIG_HOME/marker\"; printf retained >\"$XDG_STATE_HOME/marker\"",
        ]);
        environment.apply(&mut command);
        let mut child = OwnedChild::spawn(&mut command, &deployment.executable).expect("owned child");
        let status = child.child.wait().expect("child wait");
        assert!(status.success());
        assert_eq!(fs::read_to_string(retained_configuration.join("marker")).unwrap(), "retained");
        assert_eq!(fs::read_to_string(retained_state.join("marker")).unwrap(), "retained");
        assert!(!rival.join("configuration/marker").exists());
        assert!(!rival.join("state/marker").exists());

        fs::remove_dir_all(&fixture).expect("fixture cleanup");
    }

    #[test]
    fn directory_identity_integer_boundary_is_checked_without_truncation() {
        assert_eq!(integer_identity(i64::MAX as u64, "identity").unwrap(), i64::MAX);
        assert_eq!(
            integer_identity(i64::MAX as u64 + 1, "identity").unwrap_err(),
            "identity exceeds nonnegative Integer"
        );
    }

    #[test]
    fn captured_runtime_socket_is_removed_only_while_its_identity_matches() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let runtime = std::env::temp_dir().join(format!(
            "herdr-pair-runtime-socket-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&runtime).expect("runtime");
        let claim = RuntimeClaim::acquire(&runtime, "backend.owner").expect("claim");
        let socket_path = runtime.join("owned.sock");

        let listener = UnixListener::bind(&socket_path).expect("owned socket");
        let captured = claim
            .capture_socket("owned.sock")
            .expect("capture")
            .expect("present");
        captured.remove_if_owned();
        assert!(!socket_path.exists());
        drop(listener);

        let original = UnixListener::bind(&socket_path).expect("original socket");
        let captured = claim
            .capture_socket("owned.sock")
            .expect("capture original")
            .expect("original present");
        drop(original);
        fs::remove_file(&socket_path).expect("unlink original");
        let rival = UnixListener::bind(&socket_path).expect("replacement socket");
        captured.remove_if_owned();
        assert!(socket_path.exists(), "replacement socket is preserved");
        drop(rival);

        drop(claim);
        fs::remove_file(&socket_path).expect("remove replacement");
        fs::remove_dir(&runtime).expect("remove runtime");
    }

    #[test]
    fn only_incrementally_captured_runtime_sockets_are_removed() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let runtime = std::env::temp_dir().join(format!(
            "herdr-pair-partial-sockets-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&runtime).expect("runtime");
        let claim = RuntimeClaim::acquire(&runtime, "backend.owner").expect("claim");
        let first_path = runtime.join("first.sock");
        let later_path = runtime.join("later.sock");
        let first = UnixListener::bind(&first_path).expect("first socket");
        let captured = claim
            .capture_socket("first.sock")
            .expect("capture first")
            .expect("first present");
        assert!(claim.capture_socket("later.sock").expect("inspect absent").is_none());
        let later = UnixListener::bind(&later_path).expect("later unrecorded socket");

        captured.remove_if_owned();
        assert!(!first_path.exists(), "captured socket is removed");
        assert!(later_path.exists(), "unrecorded socket is preserved");
        drop(first);
        drop(later);
        drop(claim);
        fs::remove_file(&later_path).expect("remove unrecorded fixture socket");
        fs::remove_dir(&runtime).expect("remove runtime");
    }
}
