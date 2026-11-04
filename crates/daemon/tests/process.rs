use meta_signal_herdr_nexus::{
    CallTimeoutMilliseconds, Configuration, ConfigureOutcome, ExternalOutputLimitBytes,
    FrameCapacityBytes, HerdrExecutable, PollingMilliseconds, Query as MetaQuery,
    Response as MetaResponse, StartTimeoutMilliseconds,
};
use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_herdr_nexus::{
    Name, Outcome, Pane, PaneId, Query, Response, Root, Session, Value, Variable, Workspace,
};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn connect_before(deadline: Instant, path: &std::path::Path) -> UnixStream {
    loop {
        match UnixStream::connect(path) {
            Ok(stream) => return stream,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("connect {}: {error}", path.display()),
        }
    }
}

#[test]
fn binary_bootstrap_configures_and_dispatches_through_running_daemon() {
    let directory = std::env::temp_dir().join(format!(
        "herdr-daemon-process-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir(&directory).expect("create test placement");
    let place = shared_translator::placement::Place {
        metaflow_name: shared_translator::placement::MetaflowName("process-proof".into()),
        directory: shared_translator::placement::Directory(directory.display().to_string()),
    };
    let translator_path = directory.join(herdr_nexus_daemon::TRANSLATOR_SOCKET_NAME);
    let translator = UnixListener::bind(&translator_path).expect("bind translator fixture");
    let capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let fixture = std::thread::spawn(move || {
        let (mut stream, _) = translator.accept().expect("accept execute");
        let body = stream.read_frame(capacity).expect("read execute");
        let execute = Signal::<emission_herdr_nexus::Query>::from(Vec::<u8>::from(body))
            .restore()
            .expect("restore execute");
        let session = match execute {
            emission_herdr_nexus::Query::Execute(execute) => execute.herdr.session,
        };
        let response = emission_herdr_nexus::Response::Executed(
            emission_herdr_nexus::ExternalOutcome::Created(Pane {
                session,
                pane_id: PaneId("process:p1".into()),
            }),
        );
        stream
            .write_frame(
                &response.signalize().expect("archive execute response"),
                capacity,
            )
            .expect("write execute response");
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-nexus-daemon"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start daemon");
    let place_signal = place.signalize().expect("archive Place");
    let mut bootstrap = child.stdin.take().expect("bootstrap stdin");
    let bootstrap_pipe = std::fs::read_link(format!(
        "/proc/{}/fd/{}",
        std::process::id(),
        bootstrap.as_raw_fd()
    ))
    .expect("bootstrap pipe identity");
    bootstrap
        .write_frame(&place_signal, capacity)
        .expect("write Place frame");
    drop(bootstrap);

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut meta = connect_before(
        deadline,
        &directory.join(herdr_nexus_daemon::META_SOCKET_NAME),
    );
    let configuration = Configuration {
        herdr_executable: HerdrExecutable("/run/current-system/sw/bin/herdr".into()),
        polling_milliseconds: PollingMilliseconds(750),
        call_timeout_milliseconds: CallTimeoutMilliseconds(30_000),
        start_timeout_milliseconds: StartTimeoutMilliseconds(60_000),
        frame_capacity_bytes: FrameCapacityBytes(signal::MAXIMUM_SIGNAL_BYTES as i64),
        external_output_limit_bytes: ExternalOutputLimitBytes(1024 * 1024),
    };
    meta.write_frame(
        &MetaQuery::Configure(configuration)
            .signalize()
            .expect("archive Configure"),
        capacity,
    )
    .expect("send Configure");
    let meta_response = Signal::<MetaResponse>::from(Vec::<u8>::from(
        meta.read_frame(capacity).expect("read Configured"),
    ))
    .restore()
    .expect("restore Configured");
    assert_eq!(
        meta_response,
        MetaResponse::Configured(ConfigureOutcome::Succeeded)
    );
    assert!(
        std::fs::read_dir(format!("/proc/{}/fd", child.id()))
            .expect("child file descriptors")
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .all(|target| target != bootstrap_pipe),
        "the daemon retained its bootstrap pipe"
    );

    let mut public = connect_before(
        deadline,
        &directory.join(herdr_nexus_daemon::PUBLIC_SOCKET_NAME),
    );
    let workspace = Workspace {
        session: Session("process-proof".into()),
        root: Root(directory.display().to_string()),
        variable_vector: vec![Variable {
            name: Name("FLOW_ID".into()),
            value: Value("af8fe3".into()),
        }],
    };
    public
        .write_frame(
            &Query::Create(workspace)
                .signalize()
                .expect("archive Create"),
            capacity,
        )
        .expect("send Create");
    let response = Signal::<Response>::from(Vec::<u8>::from(
        public.read_frame(capacity).expect("read Created"),
    ))
    .restore()
    .expect("restore Created");
    assert!(matches!(response, Response::Created(Outcome::Created(_))));

    fixture.join().expect("translator fixture");
    child.kill().expect("stop daemon fixture");
    child.wait().expect("reap daemon fixture");
    for name in [
        herdr_nexus_daemon::META_SOCKET_NAME,
        herdr_nexus_daemon::PUBLIC_SOCKET_NAME,
        herdr_nexus_daemon::TRANSLATOR_SOCKET_NAME,
    ] {
        let path = directory.join(name);
        if path.exists() {
            std::fs::remove_file(path).expect("remove owned fixture socket");
        }
    }
    std::fs::remove_dir(directory).expect("remove owned placement");
}
