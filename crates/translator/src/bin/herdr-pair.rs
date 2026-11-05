use herdr_nexus_translator::backend_pair::{
    await_backend, request_backend_stop, restore_child_signal_mask, spawn_backend,
    AnchoredEnvironment, BackendSockets, OwnedChild, OwnedRuntimeSocket, RuntimeClaim,
    ValidatedDeployment,
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
use std::time::{Duration, Instant};

struct PairSockets {
    sockets: Vec<(Listener, String, Option<OwnedRuntimeSocket>)>,
}

fn stop_requested(signal_fd: &SignalFd) -> Result<bool, String> {
    signal_fd
        .read_signal()
        .map(|signal| signal.is_some())
        .map_err(|error| format!("read owned stop signal: {error}"))
}

fn acquire_owned_stop_signals() -> Result<(SigSet, SignalFd), Box<dyn std::error::Error>> {
    let mut owned = SigSet::empty();
    owned.add(Signal::SIGTERM);
    owned.add(Signal::SIGINT);
    let mut inherited = SigSet::empty();
    nix::sys::signal::pthread_sigmask(
        SigmaskHow::SIG_BLOCK,
        Some(&owned),
        Some(&mut inherited),
    )?;
    let signal_fd = SignalFd::with_flags(
        &owned,
        SfdFlags::SFD_NONBLOCK | SfdFlags::SFD_CLOEXEC,
    )?;
    Ok((inherited, signal_fd))
}

fn lifetime_start_allowed(signal_fd: &SignalFd) -> Result<bool, String> {
    stop_requested(signal_fd).map(|stop| !stop)
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
        signal_fd: &SignalFd,
        timeout: std::time::Duration,
        poll_interval: std::time::Duration,
    ) -> Result<bool, String> {
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "listener publication deadline overflow".to_owned())?;
        loop {
            if stop_requested(signal_fd)? {
                return Ok(false);
            }
            if core.try_wait()?.is_some() || translator.try_wait()?.is_some() || backend.try_wait()?.is_some() {
                return Err("paired child ended before listener publication".into());
            }
            if let Some(publication) = channel.try_receive()? {
                if stop_requested(signal_fd)? {
                    return Ok(false);
                }
                self.commit_publication(&publication, expected_listener.clone(), claim, || {
                    Ok(core.try_wait()?.is_none()
                        && translator.try_wait()?.is_none()
                        && backend.try_wait()?.is_none())
                })?;
                if stop_requested(signal_fd)? {
                    return Ok(false);
                }
                channel.acknowledge(expected_listener)?;
                return Ok(true);
            }
            if std::time::Instant::now() >= deadline {
                return Err("listener publication timed out".into());
            }
            std::thread::sleep(poll_interval.min(deadline.saturating_duration_since(std::time::Instant::now())));
        }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TeardownBudget {
    grace: Duration,
    nexus: Duration,
    global: Duration,
}

impl TeardownBudget {
    fn from_total(total: Duration) -> Self {
        fn fraction(total: Duration, numerator: u128) -> Duration {
            let nanos = total.as_nanos().saturating_mul(numerator) / 3;
            Duration::new(
                u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX),
                u32::try_from(nanos % 1_000_000_000).expect("subsecond nanoseconds"),
            )
        }
        Self {
            grace: fraction(total, 1),
            nexus: fraction(total, 2),
            global: total,
        }
    }
}

#[derive(Clone, Copy)]
struct TeardownDeadlines {
    grace: Instant,
    nexus: Instant,
    global: Instant,
}

impl TeardownDeadlines {
    fn new(start: Instant, total: Duration) -> Result<Self, String> {
        let budget = TeardownBudget::from_total(total);
        Ok(Self {
            grace: start.checked_add(budget.grace).ok_or_else(|| "grace deadline overflow".to_owned())?,
            nexus: start.checked_add(budget.nexus).ok_or_else(|| "Nexus deadline overflow".to_owned())?,
            global: start.checked_add(budget.global).ok_or_else(|| "global teardown deadline overflow".to_owned())?,
        })
    }
}

fn remember_first(first: &mut Option<String>, result: Result<(), String>) {
    if let Err(error) = result {
        if first.is_none() {
            *first = Some(error);
        }
    }
}

fn exited(child: &mut OwnedChild, name: &str, first: &mut Option<String>) -> bool {
    match child.try_wait() {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(error) => {
            remember_first(first, Err(format!("observe {name}: {error}")));
            false
        }
    }
}

fn wait_pair_until(
    core: &mut OwnedChild,
    translator: Option<&mut OwnedChild>,
    deadline: Instant,
    poll: Duration,
    first: &mut Option<String>,
) -> (bool, bool) {
    let mut translator = translator;
    loop {
        let core_done = exited(core, "core", first);
        let translator_done = translator
            .as_deref_mut()
            .map(|child| exited(child, "translator", first))
            .unwrap_or(true);
        if core_done && translator_done || Instant::now() >= deadline {
            return (core_done, translator_done);
        }
        std::thread::sleep(poll.min(deadline.saturating_duration_since(Instant::now())));
    }
}

fn escalation_at(deadline: Instant, poll: Duration) -> Instant {
    escalation_at_from(Instant::now(), deadline, poll)
}

fn escalation_at_from(now: Instant, deadline: Instant, poll: Duration) -> Instant {
    let remaining = deadline.saturating_duration_since(now);
    deadline.checked_sub(poll.min(remaining)).unwrap_or(deadline)
}

fn kill_if_live(child: &mut OwnedChild, name: &str, first: &mut Option<String>) {
    if !exited(child, name, first) {
        remember_first(first, child.signal(rustix::process::Signal::KILL).map_err(|error| format!("kill {name}: {error}")));
    }
}

fn terminate_backend_only(
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: Option<&BackendSockets>,
) -> Result<(), String> {
    let deadlines = TeardownDeadlines::new(Instant::now(), deployment.stop_timeout)?;
    let mut first = None;
    let force_at = escalation_at(deadlines.global, deployment.poll_interval);
    if let Some(sockets) = sockets {
        remember_first(&mut first, request_backend_stop(deployment, environment, session, sockets, force_at));
    } else if !exited(backend, "backend", &mut first) {
        remember_first(
            &mut first,
            backend
                .signal(rustix::process::Signal::TERM)
                .map_err(|error| format!("terminate backend: {error}")),
        );
    }
    if !exited(backend, "backend", &mut first) {
        match backend.wait_until(force_at, deployment.poll_interval) {
            Ok(Some(_)) => {}
            Ok(None) => kill_if_live(backend, "backend", &mut first),
            Err(error) => remember_first(&mut first, Err(format!("observe backend stop: {error}"))),
        }
    }
    if !exited(backend, "backend", &mut first) {
        match backend.wait_until(deadlines.global, deployment.poll_interval) {
            Ok(Some(_)) => {}
            Ok(None) => remember_first(&mut first, Err("backend reap unresolved at global teardown deadline".into())),
            Err(error) => remember_first(&mut first, Err(format!("reap backend: {error}"))),
        }
    }
    first.map_or(Ok(()), Err)
}

fn terminate_backend(
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: Option<&BackendSockets>,
) {
    if let Err(error) = terminate_backend_only(backend, deployment, environment, session, sockets) {
        eprintln!("backend teardown failed: {error}");
    }
}

fn report_cleanup_after_prior(result: Result<(), String>) {
    if let Err(error) = result {
        eprintln!("cleanup after prior failure also failed: {error}");
    }
}

fn terminate_core_and_backend(
    core_publication: &mut Option<herdr_nexus_wire::publication::ParentChannel>,
    core: &mut OwnedChild,
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: &BackendSockets,
) -> Result<(), String> {
    let deadlines = TeardownDeadlines::new(Instant::now(), deployment.stop_timeout);
    drop(core_publication.take());
    terminate_owned(core, None, backend, deployment, environment, session, sockets, deadlines?)
}

fn terminate_all(
    core_publication: &mut Option<herdr_nexus_wire::publication::ParentChannel>,
    translator_publication: &mut Option<herdr_nexus_wire::publication::ParentChannel>,
    core: &mut OwnedChild,
    translator: &mut OwnedChild,
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: &BackendSockets,
) -> Result<(), String> {
    let deadlines = TeardownDeadlines::new(Instant::now(), deployment.stop_timeout);
    // Closing the sole parent endpoints is the graceful stop for a child
    // blocked before listener acknowledgement. Move them out so no later
    // path can accidentally keep the publication capability alive.
    drop(core_publication.take());
    drop(translator_publication.take());
    terminate_owned(core, Some(translator), backend, deployment, environment, session, sockets, deadlines?)
}

fn terminate_owned(
    core: &mut OwnedChild,
    mut translator: Option<&mut OwnedChild>,
    backend: &mut OwnedChild,
    deployment: &ValidatedDeployment,
    environment: &AnchoredEnvironment,
    session: &str,
    sockets: &BackendSockets,
    deadlines: TeardownDeadlines,
) -> Result<(), String> {
    let mut first = None;

    // Publication channels are already closed. Give both children the same
    // EOF-unwind window, which is empty when the first third is empty.
    let (mut core_done, mut translator_done) = wait_pair_until(
        core,
        translator.as_deref_mut(),
        deadlines.grace,
        deployment.poll_interval,
        &mut first,
    );

    // Signal every remaining Nexus before waiting for either one.
    if !core_done {
        remember_first(&mut first, core.signal(rustix::process::Signal::TERM).map_err(|error| format!("terminate core: {error}")));
    }
    if !translator_done {
        if let Some(child) = translator.as_deref_mut() {
            remember_first(&mut first, child.signal(rustix::process::Signal::TERM).map_err(|error| format!("terminate translator: {error}")));
        }
    }
    let force_at = escalation_at(deadlines.nexus, deployment.poll_interval);
    (core_done, translator_done) = wait_pair_until(
        core,
        translator.as_deref_mut(),
        force_at,
        deployment.poll_interval,
        &mut first,
    );
    if !core_done {
        kill_if_live(core, "core", &mut first);
    }
    if !translator_done {
        if let Some(child) = translator.as_deref_mut() {
            kill_if_live(child, "translator", &mut first);
        }
    }
    (core_done, translator_done) = wait_pair_until(
        core,
        translator.as_deref_mut(),
        deadlines.nexus,
        deployment.poll_interval,
        &mut first,
    );

    // As soon as both Nexuses are reaped, the backend receives the remainder
    // of the one global budget. If either Nexus runs past D2, keep observing
    // it without resetting a deadline; orderly backend stop may begin only
    // after both observations succeed.
    while !(core_done && translator_done) && Instant::now() < deadlines.global {
        (core_done, translator_done) = wait_pair_until(
            core,
            translator.as_deref_mut(),
            (Instant::now() + deployment.poll_interval).min(deadlines.global),
            deployment.poll_interval,
            &mut first,
        );
    }
    if core_done && translator_done && !exited(backend, "backend", &mut first) {
        let backend_force_at = escalation_at(deadlines.global, deployment.poll_interval);
        if Instant::now() < backend_force_at {
            remember_first(
                &mut first,
                request_backend_stop(deployment, environment, session, sockets, backend_force_at),
            );
        }
        match backend.wait_until(backend_force_at, deployment.poll_interval) {
            Ok(Some(_)) => {}
            Ok(None) => kill_if_live(backend, "backend", &mut first),
            Err(error) => remember_first(&mut first, Err(format!("observe backend stop: {error}"))),
        }
        if !exited(backend, "backend", &mut first) {
            match backend.wait_until(deadlines.global, deployment.poll_interval) {
                Ok(Some(_)) => {}
                Ok(None) => {}
                Err(error) => remember_first(&mut first, Err(format!("reap backend: {error}"))),
            }
        }
    }

    // At global expiry cancel every remaining owned child. Final observation
    // is nonblocking, so D3 is neither reset nor extended.
    kill_if_live(core, "core", &mut first);
    if let Some(child) = translator.as_deref_mut() {
        kill_if_live(child, "translator", &mut first);
    }
    kill_if_live(backend, "backend", &mut first);
    for (child, name) in [(&mut *core, "core"), (&mut *backend, "backend")] {
        if !exited(child, name, &mut first) {
            remember_first(&mut first, Err(format!("{name} reap unresolved at global teardown deadline")));
        }
    }
    if let Some(child) = translator.as_deref_mut() {
        if !exited(child, "translator", &mut first) {
            remember_first(&mut first, Err("translator reap unresolved at global teardown deadline".into()));
        }
    }
    first.map_or(Ok(()), Err)
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
    let (inherited_signal_mask, signal_fd) = acquire_owned_stop_signals()?;
    if stop_requested(&signal_fd)? {
        return Ok(());
    }
    let instance_id = fresh_instance_id()?;
    let runtime = shared_translator::placement::runtime_path(&place);
    let state = shared_translator::placement::state_path(&place);
    let claim = RuntimeClaim::acquire(&runtime, &deployment.claim_name)?;
    if stop_requested(&signal_fd)? {
        return Ok(());
    }
    let mut pair_sockets = PairSockets::expected();
    pair_sockets.refuse_existing(&claim)?;
    let environment = AnchoredEnvironment::open(&state, &deployment)?;
    if stop_requested(&signal_fd)? {
        return Ok(());
    }
    let expected_sockets = BackendSockets::expected(&environment, &place.metaflow_name.0);
    expected_sockets.refuse_existing()?;

    let mut backend = spawn_backend(
        &deployment,
        &environment,
        &place.metaflow_name.0,
        inherited_signal_mask.clone(),
    )?;
    let backend_ready = await_backend(
        &deployment,
        &environment,
        &place.metaflow_name.0,
        &mut backend,
        &expected_sockets,
        || stop_requested(&signal_fd),
    );
    let backend_ready = match backend_ready {
        Ok(ready) => ready,
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
    if !backend_ready {
        let teardown = terminate_backend_only(
            &mut backend,
            &deployment,
            &environment,
            &place.metaflow_name.0,
            None,
        );
        return teardown.map_err(Into::into);
    }
    match stop_requested(&signal_fd) {
        Ok(false) => {}
        stop => {
            let teardown = terminate_backend_only(
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                None,
            );
            return match stop {
                Ok(true) => teardown.map_err(Into::into),
                Err(error) => {
                    report_cleanup_after_prior(teardown);
                    Err(error.into())
                }
                Ok(false) => unreachable!(),
            };
        }
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
    let core_channels = (|| -> Result<_, String> {
        let (parent, child) = herdr_nexus_wire::publication::pair()?;
        parent.set_deadline(deployment.readiness_timeout)?;
        let channel = child.prepare_for_spawn()?;
        Ok((parent, child, channel))
    })();
    let (core_publication, core_child_publication, core_publication_channel) =
        match core_channels {
            Ok(channels) => channels,
            Err(error) => {
                terminate_backend(&mut backend, &deployment, &environment, &place.metaflow_name.0, Some(&sockets));
                return Err(error.into());
            }
        };
    let mut core_publication = Some(core_publication);
    let mut core_command = Command::new(&core_executable);
    core_command.stdin(Stdio::piped());
    restore_child_signal_mask(&mut core_command, inherited_signal_mask.clone());
    let mut core = match OwnedChild::spawn(&mut core_command, Path::new(&core_executable)) {
        Ok(child) => child,
        Err(error) => {
            drop(core_child_publication);
            drop(core_publication.take());
            terminate_backend(&mut backend, &deployment, &environment, &place.metaflow_name.0, Some(&sockets));
            return Err(error.into());
        }
    };
    drop(core_child_publication);
    match stop_requested(&signal_fd) {
        Ok(false) => {}
        stop => {
            let teardown = terminate_core_and_backend(
                &mut core_publication,
                &mut core,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            );
            pair_sockets.remove_owned();
            return match stop {
                Ok(true) => teardown.map_err(Into::into),
                Err(error) => {
                    report_cleanup_after_prior(teardown);
                    Err(error.into())
                }
                Ok(false) => unreachable!(),
            };
        }
    }
    let translator_channels = (|| -> Result<_, String> {
        let (parent, child) = herdr_nexus_wire::publication::pair()?;
        parent.set_deadline(deployment.readiness_timeout)?;
        let channel = child.prepare_for_spawn()?;
        Ok((parent, child, channel))
    })();
    let (translator_publication, translator_child_publication, translator_publication_channel) =
        match translator_channels {
            Ok(channels) => channels,
            Err(error) => {
                report_cleanup_after_prior(terminate_core_and_backend(
                    &mut core_publication,
                    &mut core,
                    &mut backend,
                    &deployment,
                    &environment,
                    &place.metaflow_name.0,
                    &sockets,
                ));
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        };
    let mut translator_publication = Some(translator_publication);
    let mut translator_command = Command::new(&translator_executable);
    translator_command.stdin(Stdio::piped());
    environment.apply(&mut translator_command);
    restore_child_signal_mask(&mut translator_command, inherited_signal_mask);
    let mut translator = match OwnedChild::spawn(&mut translator_command, Path::new(&translator_executable)) {
        Ok(child) => child,
        Err(error) => {
            drop(translator_child_publication);
            drop(translator_publication.take());
            report_cleanup_after_prior(terminate_core_and_backend(
                &mut core_publication,
                &mut core,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            ));
            pair_sockets.remove_owned();
            return Err(error.into());
        }
    };
    drop(translator_child_publication);
    match stop_requested(&signal_fd) {
        Ok(false) => {}
        stop => {
            let teardown = terminate_all(
                &mut core_publication,
                &mut translator_publication,
                &mut core,
                &mut translator,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            );
            pair_sockets.remove_owned();
            return match stop {
                Ok(true) => teardown.map_err(Into::into),
                Err(error) => {
                    report_cleanup_after_prior(teardown);
                    Err(error.into())
                }
                Ok(false) => unreachable!(),
            };
        }
    }
    let user_id = UserId(i64::from(Uid::current().as_raw()));
    let core_pid = ProcessId(i64::from(core.child.id()));
    let translator_pid = ProcessId(i64::from(translator.child.id()));
    let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let mut bootstrap_stop = false;
    let bootstrapped = (|| -> Result<(), Box<dyn std::error::Error>> {
        if stop_requested(&signal_fd)? {
            bootstrap_stop = true;
            return Err("pair stop requested during bootstrap".into());
        }
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

        if stop_requested(&signal_fd)? {
            bootstrap_stop = true;
            return Err("pair stop requested during bootstrap".into());
        }

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
        if stop_requested(&signal_fd)? {
            bootstrap_stop = true;
            return Err("pair stop requested during bootstrap".into());
        }
        Ok(())
    })();
    if let Err(error) = bootstrapped {
        let teardown = terminate_all(
            &mut core_publication,
            &mut translator_publication,
            &mut core,
            &mut translator,
            &mut backend,
            &deployment,
            &environment,
            &place.metaflow_name.0,
            &sockets,
        );
        pair_sockets.remove_owned();
        if bootstrap_stop {
            return teardown.map_err(Into::into);
        }
        report_cleanup_after_prior(teardown);
        return Err(error);
    }

    let initial_publications = (|| -> Result<bool, String> {
        if !pair_sockets.accept_publication(
            core_publication.as_ref().expect("core publication channel"),
            Listener::MetaNexus,
            &mut core,
            &mut translator,
            &mut backend,
            &claim,
            &signal_fd,
            deployment.readiness_timeout,
            deployment.poll_interval,
        )? { return Ok(false); }
        if !pair_sockets.accept_publication(
            translator_publication.as_ref().expect("translator publication channel"),
            Listener::PublicTranslator,
            &mut core,
            &mut translator,
            &mut backend,
            &claim,
            &signal_fd,
            deployment.readiness_timeout,
            deployment.poll_interval,
        )? { return Ok(false); }
        if !pair_sockets.accept_publication(
            translator_publication.as_ref().expect("translator publication channel"),
            Listener::MetaTranslator,
            &mut core,
            &mut translator,
            &mut backend,
            &claim,
            &signal_fd,
            deployment.readiness_timeout,
            deployment.poll_interval,
        )? { return Ok(false); }
        Ok(true)
    })();
    let initial_publications = match initial_publications {
        Ok(published) => published,
        Err(error) => {
            report_cleanup_after_prior(terminate_all(
                &mut core_publication,
                &mut translator_publication,
                &mut core,
                &mut translator,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            ));
            pair_sockets.remove_owned();
            return Err(error.into());
        }
    };
    if !initial_publications {
        let teardown = terminate_all(
            &mut core_publication,
            &mut translator_publication,
            &mut core,
            &mut translator,
            &mut backend,
            &deployment,
            &environment,
            &place.metaflow_name.0,
            &sockets,
        );
        pair_sockets.remove_owned();
        return teardown.map_err(Into::into);
    }
    match lifetime_start_allowed(&signal_fd) {
        Ok(true) => {}
        allowed => {
            let teardown = terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
            pair_sockets.remove_owned();
            return match allowed {
                Ok(false) => teardown.map_err(Into::into),
                Err(error) => {
                    report_cleanup_after_prior(teardown);
                    Err(error.into())
                }
                Ok(true) => unreachable!(),
            };
        }
    }
    loop {
        match stop_requested(&signal_fd) {
            Ok(true) => {
                let teardown = terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                pair_sockets.remove_owned();
                return teardown.map_err(Into::into);
            }
            Ok(false) => {}
            Err(error) => {
                report_cleanup_after_prior(terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets));
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        }
        match core_publication.as_ref().expect("core publication channel").try_receive() {
            Ok(Some(publication)) => {
                let committed = (|| -> Result<bool, String> {
                    if stop_requested(&signal_fd)? { return Ok(false); }
                    pair_sockets.commit_publication(
                        &publication,
                        Listener::PublicNexus,
                        &claim,
                        || Ok(core.try_wait()?.is_none()
                            && translator.try_wait()?.is_none()
                            && backend.try_wait()?.is_none()),
                    )?;
                    if stop_requested(&signal_fd)? { return Ok(false); }
                    core_publication.as_ref().expect("core publication channel").acknowledge(Listener::PublicNexus)?;
                    Ok(true)
                })();
                match committed {
                    Ok(true) => {}
                    Ok(false) => {
                        let teardown = terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets);
                        pair_sockets.remove_owned();
                        return teardown.map_err(Into::into);
                    }
                    Err(error) => {
                        report_cleanup_after_prior(terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets));
                        pair_sockets.remove_owned();
                        return Err(error.into());
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                report_cleanup_after_prior(terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets));
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        }
        match translator_publication.as_ref().expect("translator publication channel").try_receive() {
            Ok(Some(_)) => {
                report_cleanup_after_prior(terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets));
                pair_sockets.remove_owned();
                return Err("unexpected duplicate translator listener publication".into());
            }
            Ok(None) => {}
            Err(error) => {
                report_cleanup_after_prior(terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets));
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
                report_cleanup_after_prior(terminate_all(&mut core_publication, &mut translator_publication, &mut core, &mut translator, &mut backend, &deployment, &environment, &place.metaflow_name.0, &sockets));
                pair_sockets.remove_owned();
                return Err(error.into());
            }
        };
        if core_status.is_some() || translator_status.is_some() || backend_status.is_some() {
            report_cleanup_after_prior(terminate_all(
                &mut core_publication,
                &mut translator_publication,
                &mut core,
                &mut translator,
                &mut backend,
                &deployment,
                &environment,
                &place.metaflow_name.0,
                &sockets,
            ));
            pair_sockets.remove_owned();
            return Err(format!(
                "paired Herdr process ended: core={core_status:?} translator={translator_status:?} backend={backend_status:?}"
            )
            .into());
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
    fn teardown_budget_uses_absolute_floor_thirds() {
        let budget = TeardownBudget::from_total(Duration::ZERO);
        assert_eq!(budget.grace, Duration::ZERO);
        assert_eq!(budget.nexus, Duration::ZERO);
        assert_eq!(budget.global, Duration::ZERO);

        let budget = TeardownBudget::from_total(Duration::from_nanos(5));
        assert_eq!(budget.grace, Duration::from_nanos(1));
        assert_eq!(budget.nexus, Duration::from_nanos(3));
        assert_eq!(budget.global, Duration::from_nanos(5));

        let budget = TeardownBudget::from_total(Duration::from_nanos(2));
        assert_eq!(budget.grace, Duration::ZERO);
        assert_eq!(budget.nexus, Duration::from_nanos(1));
        assert_eq!(budget.global, Duration::from_nanos(2));

        let budget = TeardownBudget::from_total(Duration::from_nanos(8));
        assert_eq!(budget.grace, Duration::from_nanos(2));
        assert_eq!(budget.nexus, Duration::from_nanos(5));
        assert_eq!(budget.global, Duration::from_nanos(8));

        let budget = TeardownBudget::from_total(Duration::MAX);
        assert!(budget.grace <= budget.nexus);
        assert!(budget.nexus <= budget.global);
    }

    #[test]
    fn teardown_deadlines_share_one_start_and_global_budget() {
        let start = Instant::now();
        let deadlines = TeardownDeadlines::new(start, Duration::from_millis(5_000))
            .expect("deadlines");
        assert_eq!(deadlines.grace.duration_since(start), Duration::from_nanos(1_666_666_666));
        assert_eq!(deadlines.nexus.duration_since(start), Duration::from_nanos(3_333_333_333));
        assert_eq!(deadlines.global.duration_since(start), Duration::from_millis(5_000));
        assert!(TeardownDeadlines::new(start, Duration::MAX).is_err());
    }

    #[test]
    fn escalation_reserves_one_poll_or_the_whole_remaining_phase() {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(100);
        assert_eq!(
            escalation_at_from(start, deadline, Duration::from_millis(25)),
            start + Duration::from_millis(75),
        );
        let late = start + Duration::from_millis(90);
        assert_eq!(
            escalation_at_from(late, deadline, Duration::from_millis(25)),
            late,
        );
        assert_eq!(escalation_at_from(deadline, deadline, Duration::from_millis(25)), deadline);
        assert_eq!(
            escalation_at_from(deadline + Duration::from_millis(1), deadline, Duration::from_millis(25)),
            deadline,
        );
    }

    #[test]
    fn pending_stop_is_observed_before_runtime_or_state_effects() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let absent = std::env::temp_dir().join(format!(
            "herdr-pair-pre-effect-{}-{nonce}",
            std::process::id()
        ));
        let runtime = absent.join("runtime");
        let state = absent.join("state");
        let (inherited, signal_fd) = acquire_owned_stop_signals().expect("signal controller");
        nix::sys::signal::raise(Signal::SIGTERM).expect("queue stop");
        let observed = stop_requested(&signal_fd).expect("read stop");
        nix::sys::signal::pthread_sigmask(
            SigmaskHow::SIG_SETMASK,
            Some(&inherited),
            None,
        )
        .expect("restore test mask");
        assert!(observed);
        assert!(!runtime.exists());
        assert!(!state.exists());
    }

    #[test]
    fn pending_stop_prevents_lifetime_entry_after_initial_publications() {
        let (inherited, signal_fd) = acquire_owned_stop_signals().expect("signal controller");
        nix::sys::signal::raise(Signal::SIGTERM).expect("queue stop on this test thread");
        let allowed = lifetime_start_allowed(&signal_fd).expect("read stop");
        nix::sys::signal::pthread_sigmask(
            SigmaskHow::SIG_SETMASK,
            Some(&inherited),
            None,
        )
        .expect("restore test mask");
        assert!(!allowed);
    }

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
