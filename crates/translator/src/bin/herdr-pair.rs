use herdr_nexus_translator::{maximum_text_budget, registry, transport_types::*};
use meta_signal_herdr_nexus::{BudgetValue, DepthLimit, NodeLimit};
use nix::sys::signal::{kill, Signal};
use nix::sys::wait::waitpid;
use nix::unistd::{Pid, Uid};
use shared_translator::{FrameCapacity, FrameWriting, Signalizable};
use shared_translator::{PeerIdentity, ProcessId, UserId};
use std::io::Write;
use std::process::{Child, Command, Stdio};

fn fresh_instance_id() -> Result<signal_herdr_nexus::InstanceId, Box<dyn std::error::Error>> {
    let mut bytes = [0_u8; 16];
    let mut filled = 0;
    while filled < bytes.len() {
        let count = rustix::rand::getrandom(
            &mut bytes[filled..],
            rustix::rand::GetRandomFlags::empty(),
        )?;
        if count == 0 {
            return Err("kernel randomness returned no bytes".into());
        }
        filled += count;
    }
    let mut identifier = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut identifier, "{byte:02x}")?;
    }
    Ok(signal_herdr_nexus::InstanceId(identifier))
}

fn integer(value: Option<String>, name: &str) -> Result<i64, Box<dyn std::error::Error>> {
    value.ok_or_else(|| name.to_owned())?.parse().map_err(Into::into)
}

fn stop(child: &mut Child) {
    let pid = Pid::from_raw(i32::try_from(child.id()).expect("child pid fits i32"));
    let _ = kill(pid, Signal::SIGTERM);
    let _ = waitpid(pid, None);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let registry_path = arguments.next().ok_or("REGISTRY_PATH")?;
    let name = arguments.next().ok_or("NAME")?;
    let core_executable = arguments.next().ok_or("CORE_EXECUTABLE")?;
    let translator_executable = arguments.next().ok_or("TRANSLATOR_EXECUTABLE")?;
    let admission = Admission {
        frame_deadline_milliseconds: FrameDeadlineMilliseconds(integer(
            arguments.next(),
            "FRAME_DEADLINE_MILLISECONDS",
        )?),
        frame_capacity_bytes: FrameCapacityBytes(integer(
            arguments.next(),
            "FRAME_CAPACITY_BYTES",
        )?),
        retry_delay_milliseconds: RetryDelayMilliseconds(integer(
            arguments.next(),
            "RETRY_DELAY_MILLISECONDS",
        )?),
    };
    herdr_nexus_wire::ValidAdmission::try_from(&admission)
        .map_err(|message| format!("invalid admission: {message}"))?;
    let budget_value = BudgetValue {
        node_limit: NodeLimit(integer(arguments.next(), "TEXT_NODE_LIMIT")?),
        depth_limit: DepthLimit(integer(arguments.next(), "TEXT_DEPTH_LIMIT")?),
    };
    if arguments.next().is_some() {
        return Err("unexpected launcher argument".into());
    }
    let registry_text = std::fs::read_to_string(registry_path)?;
    let registry = registry::decode(&registry_text, &mut maximum_text_budget())
        .map_err(|error| format!("{error:?}"))?;
    let place = registry::select(&registry, &name)
        .map_err(|error| format!("{error:?}"))?
        .clone();
    if !std::path::Path::new(&place.directory.0).is_dir() {
        return Err(format!("metaflow directory does not exist: {}", place.directory.0).into());
    }

    let mut core = Command::new(core_executable)
        .stdin(Stdio::piped())
        .spawn()?;
    let mut translator = match Command::new(translator_executable)
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            stop(&mut core);
            return Err(error.into());
        }
    };
    let user_id = UserId(i64::from(Uid::current().as_raw()));
    let core_pid = ProcessId(i64::from(core.id()));
    let translator_pid = ProcessId(i64::from(translator.id()));
    let instance_id = fresh_instance_id()?;
    let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let bootstrapped = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut core_input = core.stdin.take().ok_or("core bootstrap pipe")?;
        core_input.write_frame(
            &CoreBootstrap {
                place: place.clone(),
                admission: admission.clone(),
                peer_identity: PeerIdentity {
                    user_id: user_id.clone(),
                    process_id: translator_pid,
                },
                instance_id,
            }
            .signalize()?,
            capacity,
        )?;
        core_input.flush()?;
        drop(core_input);

        let mut translator_input = translator.stdin.take().ok_or("translator bootstrap pipe")?;
        translator_input.write_frame(
            &TranslatorBootstrap {
                place,
                admission,
                budget_value,
                peer_identity: PeerIdentity {
                    user_id,
                    process_id: core_pid,
                },
            }
            .signalize()?,
            capacity,
        )?;
        translator_input.flush()?;
        drop(translator_input);
        Ok(())
    })();
    if let Err(error) = bootstrapped {
        stop(&mut core);
        stop(&mut translator);
        return Err(error);
    }

    let core_pid = Pid::from_raw(i32::try_from(core.id()).expect("child pid fits i32"));
    let translator_pid =
        Pid::from_raw(i32::try_from(translator.id()).expect("child pid fits i32"));
    let ended = waitpid(Pid::from_raw(-1), None)?;
    let survivor = if ended.pid() == Some(core_pid) {
        translator_pid
    } else {
        core_pid
    };
    let _ = kill(survivor, Signal::SIGTERM);
    let _ = waitpid(survivor, None);
    Err(format!("paired Herdr process ended: {ended:?}").into())
}
