use herdr_nexus_translator::*;
use std::path::PathBuf;
#[test]
fn explicit_start_preserves_argv() {
 let c=AgentStart{name:AgentName::new("probe").unwrap(),kind:AgentKind::Codex,executable:Executable::Explicit(PathBuf::from("/opt/codex")),pane_id:"wJ0:p1".into(),arguments:vec!["--model".into(),"x y".into()],timeout_ms:Some(42)};
 assert_eq!(c.argv(),["agent","start","probe","--kind","codex","--pane","wJ0:p1","--executable","/opt/codex","--timeout","42","--","--model","x y"]);
}
#[test]
fn name_is_bounded() { assert!(AgentName::new("Probe").is_err()); assert!(AgentName::new("a".repeat(33)).is_err()); assert!(AgentName::new("a_b-2").is_ok()); }

#[test]
fn first_character_must_be_lowercase() { assert!(AgentName::new("1agent").is_err()); }

#[test]
fn refusal_preserves_symbolic_code_and_optional_status() {
 let json=r#"{"id":"cli:agent:start","error":{"code":"agent_executable_not_allowed","message":"not configured","exit_status":null}}"#;
 let e=parse_error(json.as_bytes()).unwrap(); assert_eq!(e.error.code,"agent_executable_not_allowed"); assert_eq!(e.error.message,"not configured");
}

#[test]
fn workspace_result_extracts_owned_root_pane() {
 let json=r#"{"id":"cli:workspace:create","result":{"type":"workspace_created","workspace":{"workspace_id":"w"},"root_pane":{"pane_id":"wJ0:p1"}}}"#;
 let e: ResultEnvelope<WorkspaceCreatedResult>=serde_json::from_str(json).unwrap(); assert_eq!(e.result.root_pane.pane_id,"wJ0:p1");
}

#[test]
fn readiness_is_required_for_start_witness() {
 let json=r#"{"id":"cli:agent:start","result":{"type":"agent_started","agent":{"agent":"codex","agent_status":"idle","interactive_ready":false,"name":"probe","pane_id":"wJ0:p1"},"argv":["codex"],"executable":"codex"}}"#;
 let e=parse_agent_started(json.as_bytes()).unwrap(); assert!(matches!(witness_agent_started(e.result),Err(StartWitnessError::NotReady)));
}

#[test]
fn malformed_or_incomplete_json_is_not_success() {
 assert!(parse_agent_started(br#"{"id":"cli:agent:start","result":{}}"#).is_err());
 assert!(parse_agent_started(br#"not-json"#).is_err());
}
