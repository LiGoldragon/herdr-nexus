use emission_herdr_nexus::{
    AgentPrompt_Data, AgentStart_Data, ExternalOutcome, Herdr, Keys_Data, Verb,
    WorkspaceCreate_Data,
};
use herdr_nexus_translator::server::{execute, Runtime};
use emission_herdr_nexus::Timeout;
use signal_herdr_nexus::{
    AgentKind, AgentName, Command, Executable, Keys, Name, PaneId, Program, Root, Session, State,
    Text, Value, Variable,
};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    directory: PathBuf,
    executable: PathBuf,
    record: PathBuf,
}

impl Fixture {
    fn new(body: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "herdr-translator-fixture-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("create fixture directory");
        let executable = directory.join("herdr-fixture");
        let record = directory.join("arguments");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n{}\n",
            record.display(),
            body
        );
        fs::write(&executable, script).expect("write fixture");
        let mut permissions = fs::metadata(&executable)
            .expect("fixture metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions).expect("make fixture executable");
        Self {
            directory,
            executable,
            record,
        }
    }

    fn runtime(&self) -> Runtime {
        Runtime::new(
            self.executable.clone(),
            std::time::Duration::from_millis(5_000),
            64 * 1024,
        ).expect("valid runtime fixture")
    }

    fn arguments(&self) -> Vec<String> {
        fs::read_to_string(&self.record)
            .expect("read recorded arguments")
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.executable);
        let _ = fs::remove_file(&self.record);
        let _ = fs::remove_dir(&self.directory);
    }
}

fn herdr(verb: Verb) -> Herdr {
    Herdr {
        session: Session("main".into()),
        verb,
    }
}

const SUCCESS_SCRIPT: &str = r#"
case "$3:$4" in
  workspace:create)
    printf '%s\n' '{"id":"cli:workspace:create","result":{"type":"workspace_created","workspace":{},"tab":{},"root_pane":{"pane_id":"w:p","terminal_id":"t","workspace_id":"w","tab_id":"tab","focused":true,"agent_status":"unknown","revision":1}}}'
    ;;
  agent:start)
    printf '%s\n' '{"id":"cli:agent:start","result":{"type":"agent_started","agent":{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"idle","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":2},"argv":["codex"],"executable":"codex"}}'
    ;;
  agent:prompt)
    printf '%s\n' '{"id":"cli:agent:prompt","result":{"type":"agent_prompted","agent":{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"working","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":3}}}'
    ;;
  pane:send-keys)
    exit 0
    ;;
  pane:close)
    printf '%s\n' '{"id":"cli:pane:close","result":{"type":"ok"}}'
    ;;
  pane:list)
    printf '%s\n' '{"id":"cli:pane:list","result":{"type":"pane_list","panes":[{"pane_id":"w:p","terminal_id":"t","workspace_id":"w","tab_id":"tab","focused":true,"agent_status":"idle","revision":4}]}}'
    ;;
  agent:list)
    printf '%s\n' '{"id":"cli:agent:list","result":{"type":"agent_list","agents":[{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"idle","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":4}]}}'
    ;;
  *) exit 19 ;;
esac
"#;

#[test]
fn production_execute_covers_every_external_verb_and_exact_argv() {
    let fixture = Fixture::new(SUCCESS_SCRIPT);
    let runtime = fixture.runtime();

    let created = execute(
        &runtime,
        &herdr(Verb::WorkspaceCreate(WorkspaceCreate_Data {
            root: Root("/work with space".into()),
            variable_vector: vec![Variable {
                name: Name("A".into()),
                value: Value("one two".into()),
            }],
        })),
    );
    assert!(matches!(created, ExternalOutcome::Created(_)));
    assert_eq!(
        fixture.arguments(),
        [
            "--session",
            "main",
            "workspace",
            "create",
            "--cwd",
            "/work with space",
            "--env",
            "A=one two",
        ]
    );

    let started = execute(
        &runtime,
        &herdr(Verb::AgentStart(AgentStart_Data {
            pane_id: PaneId("w:p".into()),
            command: Command {
                agent_name: AgentName("probe".into()),
                agent_kind: AgentKind::Codex,
                executable: Executable::Explicit(Program("/opt/codex".into())),
                string_vector: vec!["--model".into(), "x y".into()],
            },
            timeout: Timeout(5_000),
        })),
    );
    assert_eq!(started, ExternalOutcome::Started(State::Idle));
    assert_eq!(
        fixture.arguments(),
        [
            "--session",
            "main",
            "agent",
            "start",
            "probe",
            "--kind",
            "codex",
            "--pane",
            "w:p",
            "--executable",
            "/opt/codex",
            "--timeout",
            "5000",
            "--",
            "--model",
            "x y",
        ]
    );

    assert_eq!(
        execute(
            &runtime,
            &herdr(Verb::AgentPrompt(AgentPrompt_Data {
                pane_id: PaneId("w:p".into()),
                text: Text("hello there".into()),
            })),
        ),
        ExternalOutcome::Done
    );
    assert_eq!(
        fixture.arguments(),
        ["--session", "main", "agent", "prompt", "w:p", "hello there"]
    );

    assert_eq!(
        execute(
            &runtime,
            &herdr(Verb::Keys(Keys_Data {
                pane_id: PaneId("w:p".into()),
                keys: Keys(vec!["ctrl-c".into(), "x y".into()]),
            })),
        ),
        ExternalOutcome::Done
    );
    assert_eq!(
        fixture.arguments(),
        [
            "--session",
            "main",
            "pane",
            "send-keys",
            "w:p",
            "ctrl-c",
            "x y",
        ]
    );

    assert_eq!(
        execute(
            &runtime,
            &herdr(Verb::PaneClose(PaneId("w:p".into()))),
        ),
        ExternalOutcome::Done
    );

    let ExternalOutcome::Panes(panes) = execute(&runtime, &herdr(Verb::PaneList)) else {
        panic!("pane list expected");
    };
    assert_eq!(panes.len(), 1);
    assert_eq!(panes[0].pane.pane_id.0, "w:p");
    let ExternalOutcome::Agents(agents) = execute(&runtime, &herdr(Verb::AgentList)) else {
        panic!("agent list expected");
    };
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].state, State::Idle);
}

#[test]
fn production_execute_preserves_symbolic_refusal_and_process_status() {
    let fixture = Fixture::new(
        r#"printf '%s\n' '{"id":"cli:pane:close","error":{"code":"agent_executable_not_allowed","message":"not configured"}}' >&2
exit 7"#,
    );
    let outcome = execute(
        &fixture.runtime(),
        &herdr(Verb::PaneClose(PaneId("w:p".into()))),
    );
    let ExternalOutcome::Refused(refusal) = outcome else {
        panic!("typed refusal expected, got {outcome:?}");
    };
    assert_eq!(refusal.error_code.0, "agent_executable_not_allowed");
    assert_eq!(refusal.exit_status_option.expect("exit status").0, 7);
    assert_eq!(refusal.message.0, "not configured");
}

#[test]
fn start_execution_owns_its_timeout_instead_of_generic_call_timeout() {
    let fixture = Fixture::new(
        r#"sleep 0.08
printf '%s\n' '{"id":"cli:agent:start","result":{"type":"agent_started","agent":{"terminal_id":"t","name":"probe","agent":"codex","agent_status":"idle","workspace_id":"w","tab_id":"tab","pane_id":"w:p","focused":true,"interactive_ready":true,"revision":2},"argv":["codex"]}}'"#,
    );
    let runtime = Runtime::new(
        fixture.executable.clone(),
        std::time::Duration::from_millis(10),
        64 * 1024,
    )
    .expect("runtime");
    let command = Command {
        agent_name: AgentName("probe".into()),
        agent_kind: AgentKind::Codex,
        executable: Executable::Configured,
        string_vector: Vec::new(),
    };

    let began = std::time::Instant::now();
    assert_eq!(
        execute(
            &runtime,
            &herdr(Verb::AgentStart(AgentStart_Data {
                pane_id: PaneId("w:p".into()),
                command: command.clone(),
                timeout: Timeout(200),
            })),
        ),
        ExternalOutcome::Started(State::Idle)
    );
    assert!(began.elapsed() >= std::time::Duration::from_millis(70));

    assert_eq!(
        execute(
            &runtime,
            &herdr(Verb::AgentStart(AgentStart_Data {
                pane_id: PaneId("w:p".into()),
                command,
                timeout: Timeout(20),
            })),
        ),
        ExternalOutcome::Uncertain
    );
}
