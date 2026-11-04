//! Bounded execution of one owned external client process group.

use nix::fcntl::{fcntl, FcntlArg, OFlag};
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use std::ffi::OsString;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Captured {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
pub enum RunError {
    Spawn(std::io::Error),
    Pipe(std::io::Error),
    Wait(std::io::Error),
    Deadline,
    OutputLimit,
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "bounded external client failed: {self:?}")
    }
}

impl std::error::Error for RunError {}

/// Runs one executable and its arguments as a new owned process group.
///
/// Output is retained only while the joint stdout/stderr byte count remains
/// within `output_limit`. Crossing the limit or reaching the deadline kills
/// and reaps the complete owned group before returning.
pub fn run(
    executable: &Path,
    arguments: &[OsString],
    deadline: Instant,
    output_limit: usize,
) -> Result<Captured, RunError> {
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().map_err(RunError::Spawn)?;
    let Some(mut stdout) = child.stdout.take() else {
        terminate(&mut child);
        return Err(RunError::Pipe(std::io::Error::other(
            "stdout pipe was not created",
        )));
    };
    let Some(mut stderr) = child.stderr.take() else {
        terminate(&mut child);
        return Err(RunError::Pipe(std::io::Error::other(
            "stderr pipe was not created",
        )));
    };
    if let Err(error) = nonblocking(&stdout).and_then(|()| nonblocking(&stderr)) {
        terminate(&mut child);
        return Err(error);
    }

    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut out_open = true;
    let mut err_open = true;
    let mut exited = None;
    let mut buffer = [0_u8; 8192];
    loop {
        let mut progressed = false;
        if out_open {
            match stdout.read(&mut buffer) {
                Ok(0) => out_open = false,
                Ok(length) => {
                    progressed = true;
                    if !retain(&mut out, &buffer[..length], err.len(), output_limit) {
                        terminate(&mut child);
                        return Err(RunError::OutputLimit);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    terminate(&mut child);
                    return Err(RunError::Pipe(error));
                }
            }
        }
        if err_open {
            match stderr.read(&mut buffer) {
                Ok(0) => err_open = false,
                Ok(length) => {
                    progressed = true;
                    if !retain(&mut err, &buffer[..length], out.len(), output_limit) {
                        terminate(&mut child);
                        return Err(RunError::OutputLimit);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    terminate(&mut child);
                    return Err(RunError::Pipe(error));
                }
            }
        }
        if exited.is_none() {
            match child.try_wait() {
                Ok(status) => exited = status,
                Err(error) => {
                    terminate(&mut child);
                    return Err(RunError::Wait(error));
                }
            }
        }
        if let Some(status) = exited {
            if !out_open && !err_open {
                return Ok(Captured {
                    status,
                    stdout: out,
                    stderr: err,
                });
            }
        }
        if Instant::now() >= deadline {
            terminate(&mut child);
            return Err(RunError::Deadline);
        }
        if !progressed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            wait_for_output(&stdout, &stderr, out_open, err_open, remaining)
                .map_err(RunError::Pipe)?;
        }
    }
}

fn wait_for_output(
    stdout: &impl std::os::fd::AsRawFd,
    stderr: &impl std::os::fd::AsRawFd,
    stdout_open: bool,
    stderr_open: bool,
    remaining: Duration,
) -> std::io::Result<()> {
    let mut descriptors = [
        libc::pollfd {
            fd: stdout.as_raw_fd(),
            events: if stdout_open { libc::POLLIN | libc::POLLHUP } else { 0 },
            revents: 0,
        },
        libc::pollfd {
            fd: stderr.as_raw_fd(),
            events: if stderr_open { libc::POLLIN | libc::POLLHUP } else { 0 },
            revents: 0,
        },
    ];
    let milliseconds = remaining.as_millis().min(i32::MAX as u128) as i32;
    // SAFETY: `descriptors` is a valid mutable array for the duration of this call.
    let result = unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, milliseconds) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        // A child exit may interrupt poll before its pipe hangup is observed.
        // Returning to the outer loop preserves the same absolute deadline and
        // lets it drain both pipes and reap the child.
        if error.kind() == std::io::ErrorKind::Interrupted {
            Ok(())
        } else {
            Err(error)
        }
    } else {
        Ok(())
    }
}

fn nonblocking(pipe: &impl std::os::fd::AsFd) -> Result<(), RunError> {
    let current = fcntl(pipe, FcntlArg::F_GETFL).map_err(nix_pipe_error)?;
    let flags = OFlag::from_bits_truncate(current);
    fcntl(pipe, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).map_err(nix_pipe_error)?;
    Ok(())
}

fn nix_pipe_error(error: nix::errno::Errno) -> RunError {
    RunError::Pipe(std::io::Error::from_raw_os_error(error as i32))
}

fn retain(target: &mut Vec<u8>, bytes: &[u8], other: usize, limit: usize) -> bool {
    let Some(total) = target
        .len()
        .checked_add(other)
        .and_then(|held| held.checked_add(bytes.len()))
    else {
        return false;
    };
    if total > limit {
        return false;
    }
    target.extend_from_slice(bytes);
    true
}

fn terminate(child: &mut Child) {
    let _ = killpg(Pid::from_raw(child.id() as i32), Signal::SIGKILL);
    let _ = child.wait();
}
