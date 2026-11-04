use herdr_nexus_translator::backend_pair::{
    await_backend, bounded_stop, request_backend_stop, spawn_backend, AnchoredEnvironment,
    BackendSockets, OwnedChild, OwnedRuntimeSocket, RuntimeClaim, ValidatedDeployment,
};
use herdr_nexus_translator::generated::backend_deployment::BackendDeployment;
use herdr_nexus_translator::{maximum_text_budget, transport_types::*};
use meta_signal_herdr_nexus::{BudgetValue, DepthLimit, NodeLimit, PathValue};
use nix::sys::signal::{SigSet, SigmaskHow, Signal};
use nix::sys::signalfd::{SfdFlags, SignalFd};
use nix::unistd::Uid;
use shared_translator::{FrameCapacity, FrameWriting, Signalizable};
use shared_translator::{PeerIdentity, ProcessId, UserId};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

struct PairSockets {
    sockets: Vec<(Listener, String, Option<OwnedRuntimeSocket>)>,
}

impl PairSockets {
    fn expected() -> Self {
        let names = herdr_nexus_wire::socket_names();
        Self {
            sockets: vec![
                (Listener::PublicNexus, names.public_nexus_socket.0, None),
                (Listener::MetaNexus, names.meta_nexus_socket.0, None),
                (Listener::PublicTranslator, names.public_translator_socket.0, None),
                (Listener::MetaTranslator, names.meta_translator_socket.0, None),
            ],
        }
    }

    fn refuse_existing(&self, claim: &RuntimeClaim) -> Result<(), String> {
        for (_, name, _) in &self.sockets {
            claim.refuse_existing(name)?;
        }
        Ok(())
    }

    fn commit_publication(
        &mut self,
        publication: &ListenerPublication,
        expected_listener: Listener,
        claim: &RuntimeClaim,
        mut all_children_live: impl FnMut() -> Result<bool, String>,
    ) -> Result<(), String> {
        if publication.listener != expected_listener {
            return Err("listener publication names an unexpected listener".into());
        }
        let index = self.sockets.iter().position(|(listener, _, _)| *listener == expected_listener)
            .ok_or_else(|| "listener publication has no fixed path".to_owned())?;
        if self.sockets[index].2.is_some() {
            return Err("duplicate listener publication".into());
        }
        if !all_children_live()? {
            return Err("paired child ended before admission socket ownership was recorded".to_owned());
        }
        let staged = claim.capture_published_socket(
            &self.sockets[index].1,
            &publication.socket_identity,
        )?;
        if !all_children_live()? {
            return Err("paired child ended while admission socket ownership was recorded".to_owned());
        }
        self.sockets[index].2 = Some(staged);
        Ok(())
    }

    fn accept_publication(
        &mut self,
        channel: &herdr_nexus_wire::publication::ParentChannel,
        expected_listener: Listener,
        core: &mut OwnedChild,
        translator: &mut OwnedChild,
        backend: &mut OwnedChild,
        claim: &RuntimeClaim,
    ) -> Result<(), String> {
        if core.try_wait()?.is_some() || translator.try_wait()?.is_some() || backend.try_wait()?.is_some() {
            return Err("paired child ended before listener publication".into());
        }
        let publication = channel.receive()?;
        self.commit_publication(&publication, expected_listener.clone(), claim, || {
            Ok(core.try_wait()?.is_none()
                && translator.try_wait()?.is_none()
                && backend.try_wait()?.is_none())
        })?;
        channel.acknowledge(expected_listener)
    }

    fn remove_owned(&self) {
        for (_, _, socket) in &self.sockets {
            if let Some(socket) = socket {
                socket.remove_if_owned();
            }
        }
    }
}

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

fn terminate_nexus(child: &mut OwnedChild, deployment: &ValidatedDeployment) {
    let _ = child.signal(rustix::process::Signal::TERM);
    let deadline = std::time::Instant::now()
        .checked_add(deployment.stop_timeout)
        .unwrap_or_else(std::time::Instant::now);
    if child
        .wait_until(deadline, deployment.poll_interval)
        .ok()
        .flatten()
        .is_none()
    {
        let _ = child.signal(rustix::process::Signal::KILL);
        let _ = child.child.wait();
    }
}

fn terminate_backend(
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: Option<&BackendSockets>,
) {
    if let Some(sockets) = sockets {
        let _ = request_backend_stop(deployment, environment, session, sockets);
    }
    bounded_stop(backend, deployment.stop_timeout, deployment.poll_interval);
}

fn terminate_all(
    core: &mut OwnedChild,
    translator: &mut OwnedChild,
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: &BackendSockets,
) {
    terminate_nexus(core, deployment);
    terminate_nexus(translator, deployment);
    terminate_backend(backend, deployment, environment, session, Some(sockets));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let place_text = arguments.next().ok_or("PLACE")?;
    let deployment_text = arguments.next().ok_or("BACKEND_DEPLOYMENT")?;
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
    let place = shared_translator::decode::<shared_translator::Place>(
        &place_text,
        &mut maximum_text_budget(),
    )
    .map_err(|error| format!("{error:?}"))?;
    let deployment = shared_translator::decode::<BackendDeployment>(
        &deployment_text,
        &mut maximum_text_budget(),
    )
    .map_err(|error| format!("{error:?}"))?;
    let deployment = ValidatedDeployment::try_from(deployment)?;
    shared_translator::placement::validate_metaflow_name(&place.metaflow_name.0)?;
    let instance_id = fresh_instance_id()?;
    let runtime = shared_translator::placement::runtime_path(&place);
    let state = shared_translator::placement::state_path(&place);
    let claim = RuntimeClaim::acquire(&runtime, &deployment.claim_name)?;
    let mut pair_sockets = PairSockets::expected();
    pair_sockets.refuse_existing(&claim)?;
    let environment = AnchoredEnvironment::open(&state, &deployment)?;
    let expected_sockets = BackendSockets::expected(&environment, &place.metaflow_name.0);
    expected_sockets.refuse_existing()?;

    let mut backend = spawn_backend(&deployment, &environment, &place.metaflow_name.0)?;
    if let Err(error) = await_backend(
        &deployment,
        &environment,
        &place.metaflow_name.0,
        &mut backend,
        &expected_sockets,
    ) {
        terminate_backend(
            &mut backend,
            &deployment,
            &environment,
            &place.metaflow_name.0,
            None,
        );
        return Err(error.into());
    }
    let sockets = match expected_sockets.capture() {
        Ok(sockets) => sockets,
        Err(error) => {
            terminate_backend(
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                None,
            );
            return Err(error.into());
        }
    };
    let (core_publication, core_child_publication) =
        herdr_nexus_wire::publication::pair()?;
    core_publication.set_deadline(deployment.readiness_timeout)?;
    let core_publication_channel = core_child_publication.prepare_for_spawn()?;
    let mut core_command = Command::new(&core_executable);
    core_command.stdin(Stdio::piped());
    let mut core = match OwnedChild::spawn(&mut core_command, Path::new(&core_executable)) {
        Ok(child) => child,
        Err(error) => {
            terminate_backend(&mut backend, &deployment, &environment, &place.metaflow_name.0, Some(&sockets));
            return Err(error.into());
        }
    };
    drop(core_child_publication);
    let (translator_publication, translator_child_publication) =
        herdr_nexus_wire::publication::pair()?;
    translator_publication.set_deadline(deployment.readiness_timeout)?;
    let translator_publication_channel = translator_child_publication.prepare_for_spawn()?;
    let mut translator_command = Command::new(&translator_executable);
    translator_command.stdin(Stdio::piped());
    environment.apply(&mut translator_command);
    let mut translator = match OwnedChild::spawn(&mut translator_command, Path::new(&translator_executable)) {
        Ok(child) => child,
        Err(error) => {
            terminate_nexus(&mut core, &deployment);
            terminate_backend(&mut backend, &deployment, &environment, &place.metaflow_name.0, Some(&sockets));
            pair_sockets.remove_owned();
            return Err(error.into());
        }
    };
    drop(translator_child_publication);
    let user_id = UserId(i64::from(Uid::current().as_raw()));
    let core_pid = ProcessId(i64::from(core.child.id()));
    let translator_pid = ProcessId(i64::from(translator.child.id()));
    let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let bootstrapped = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut core_input = core.child.stdin.take().ok_or("core bootstrap pipe")?;
        core_input.write_frame(
            &CoreBootstrap {
                place: place.clone(),
                admission: admission.clone(),
                peer_identity: PeerIdentity {
                    user_id: user_id.clone(),
                    process_id: translator_pid,
                },
                instance_id,
                publication_channel: core_publication_channel,
            }
            .signalize()?,
            capacity,
        )?;
        core_input.flush()?;
        drop(core_input);

        let mut translator_input = translator.child.stdin.take().ok_or("translator bootstrap pipe")?;
        translator_input.write_frame(
            &TranslatorBootstrap {
                place: place.clone(),
                admission,
                budget_value,
                peer_identity: PeerIdentity {
                    user_id,
                    process_id: core_pid,
                },
                expected_herdr_executable: ExpectedHerdrExecutable(PathValue(
                    deployment.executable.to_string_lossy().into_owned(),
                )),
                backend_roots: environment.backend_roots()?,
                publication_channel: translator_publication_channel,
            }
            .signalize()?,
            capacity,
        )?;
        translator_input.flush()?;
        drop(translator_input);
        Ok(())
    })();
    if let Err(error) = bootstrapped {
        terminate_all(
            &mut core,
            &mut translator,
            &mut backend,
            &deployment,
            &environment,
            &place.metaflow_name.0,
            &sockets,
        );
        pair_sockets.remove_owned();
        return Err(error);
    }

    let initial_publications = (|| -> Result<(), String> {
        pair_sockets.accept_publication(
            &core_publication,
            Listener::MetaNexus,
            &mut core,
            &mut translator,
            &mut backend,
            &claim,
        )?;
        pair_sockets.accept_publication(
            &translator_publication,
            Listener::PublicTranslator,
            &mut core,
            &mut translator,
            &mut backend,
            &claim,
        )?;
        pair_sockets.accept_publication(
            &translator_publication,
            Listener::MetaTranslator,
            &mut core,
            &mut translator,
            &mut backend,
            &claim,
        )
    })();
    if let Err(error) = initial_publications {
        terminate_all(
            &mut core,
            &mut translator,
            &mut backend,
            &deployment,
            &environment,
            &place.metaflow_name.0,
            &sockets,
        );
        pair_sockets.remove_owned();
        return Err(error.into());
    }

    let mut mask = SigSet::empty();
    mask.add(Signal::SIGTERM);
    mask.add(Signal::SIGINT);
    let signal_fd = match nix::sys::signal::pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&mask), None)
        .and_then(|()| SignalFd::with_flags(&mask, SfdFlags::SFD_NONBLOCK | SfdFlags::SFD_CLOEXEC))
    {
        Ok(signal_fd) => signal_fd,
        Err(error) => {
            terminate_all(
                &mut core,
                &mut translator,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            );
            pair_sockets.remove_owned();
            return Err(error.into());
        }
    };
    loop {
        match core_publication.try_receive() {
            Ok(Some(publication)) => {
                let committed = pair_sockets.commit_publication(
                    &publication,
                    Listener::PublicNexus,
                    &claim,
                    || Ok(core.try_wait()?.is_none()
                        && translator.try_wait()?.is_none()
                        && backend.try_wait()?.is_none()),
                ).and_then(|()| core_publication.acknowledge(Listener::PublicNexus));
                if let Err(error) = committed {
                    terminate_all(&mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                    pair_sockets.remove_owned();
                    return Err(error.into());
                }
            }
            Ok(None) => {}
            Err(error) => {
                terminate_all(&mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        }
        match translator_publication.try_receive() {
            Ok(Some(_)) => {
                terminate_all(&mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                pair_sockets.remove_owned();
                return Err("unexpected duplicate translator listener publication".into());
            }
            Ok(None) => {}
            Err(error) => {
                terminate_all(&mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        }
        let statuses = (|| -> Result<_, String> {
            Ok((core.try_wait()?, translator.try_wait()?, backend.try_wait()?))
        })();
        let (core_status, translator_status, backend_status) = match statuses {
            Ok(statuses) => statuses,
            Err(error) => {
                terminate_all(&mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        };
        if core_status.is_some() || translator_status.is_some() || backend_status.is_some() {
            if core_status.is_none() {
                terminate_nexus(&mut core, &deployment);
            }
            if translator_status.is_none() {
                terminate_nexus(&mut translator, &deployment);
            }
            if backend_status.is_none() {
                terminate_backend(
                    &mut backend,
                    &deployment,
                    &environment,
                    &place.metaflow_name.0,
                    Some(&sockets),
                );
            }
            pair_sockets.remove_owned();
            return Err(format!(
                "paired Herdr process ended: core={core_status:?} translator={translator_status:?} backend={backend_status:?}"
            )
            .into());
        }
        let received_signal = match signal_fd.read_signal() {
            Ok(signal) => signal,
            Err(error) => {
                terminate_all(&mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        };
        if received_signal.is_some() {
            terminate_all(
                &mut core,
                &mut translator,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            );
            pair_sockets.remove_owned();
            return Ok(());
        }
        std::thread::sleep(deployment.poll_interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::net::UnixListener;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn failed_post_capture_witness_does_not_expand_cleanup_authority() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let runtime = std::env::temp_dir().join(format!(
            "herdr-pair-capture-transaction-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&runtime).expect("runtime");
        let claim = RuntimeClaim::acquire(&runtime, "backend.owner").expect("claim");
        let prior_path = runtime.join("prior.sock");
        let staged_path = runtime.join("staged.sock");
        let prior_listener = UnixListener::bind(&prior_path).expect("prior socket");
        let staged_listener = UnixListener::bind(&staged_path).expect("staged socket");
        let prior = claim
            .capture_socket("prior.sock")
            .expect("capture prior")
            .expect("prior present");
        let mut sockets = PairSockets {
            sockets: vec![
                (Listener::MetaNexus, "prior.sock".into(), Some(prior)),
                (Listener::PublicNexus, "staged.sock".into(), None),
            ],
        };
        let metadata = fs::metadata(&staged_path).expect("staged identity");
        let publication = ListenerPublication {
            listener: Listener::PublicNexus,
            socket_identity: SocketIdentity {
                device_id: DeviceId(i64::try_from(metadata.dev()).expect("device")),
                inode_id: InodeId(i64::try_from(metadata.ino()).expect("inode")),
            },
        };
        let wrong_listener = ListenerPublication {
            listener: Listener::MetaNexus,
            socket_identity: publication.socket_identity.clone(),
        };
        assert!(sockets
            .commit_publication(
                &wrong_listener,
                Listener::PublicNexus,
                &claim,
                || Ok(true),
            )
            .expect_err("wrong child listener must be refused")
            .contains("unexpected listener"));
        let duplicate = ListenerPublication {
            listener: Listener::MetaNexus,
            socket_identity: SocketIdentity { device_id: DeviceId(0), inode_id: InodeId(0) },
        };
        assert!(sockets
            .commit_publication(&duplicate, Listener::MetaNexus, &claim, || Ok(true))
            .expect_err("duplicate publication must be refused")
            .contains("duplicate listener"));
        let mut witness = [true, false].into_iter();

        assert!(sockets
            .commit_publication(
                &publication,
                Listener::PublicNexus,
                &claim,
                || Ok(witness.next().expect("two witnesses")),
            )
            .is_err());
        sockets.remove_owned();
        assert!(!prior_path.exists(), "previously proven socket is removed");
        assert!(staged_path.exists(), "uncommitted observation is preserved");

        drop(prior_listener);
        drop(staged_listener);
        drop(claim);
        fs::remove_file(&staged_path).expect("remove staged fixture socket");
        fs::remove_dir(&runtime).expect("remove runtime");
    }
}
