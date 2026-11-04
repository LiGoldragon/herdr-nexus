use shared_translator::placement::Place;
use emission_herdr_nexus::{ExternalOutcome, Herdr, Verb};
use signal_herdr_nexus::{Pane, PaneId, Refusal, ErrorCode, ExitStatus, Message, State};
use std::{io::{Read}, os::unix::net::{UnixListener,UnixStream}, process::{Command,Stdio}, thread, time::{Duration,Instant}};
use shared_translator::{FrameReading,FrameWriting,Restorable,Signal,Signalizable};

pub fn run(place: &Place) -> Result<(), ServerError> {
 let socket=std::path::Path::new(&place.directory.0).join("herdr-translator.sock");
 let listener=UnixListener::bind(socket).map_err(ServerError::Io)?;
 for stream in listener.incoming() { let Ok(mut stream)=stream else { continue }; let _=serve_one(&mut stream); }
 Ok(())
}
pub fn serve_one(stream:&mut UnixStream)->Result<(),ServerError>{
 let body=stream.read_frame(SharedCapacity::default()).map_err(ServerError::Frame)?;
 let request=Signal::<emission_herdr_nexus::Query>::from(Vec::from(body)).restore().map_err(|e|ServerError::Signal(format!("{e:?}")))?;
 let emission_herdr_nexus::Query::Execute(data)=request;
 let outcome=execute(data.configuration,data.herdr);
 let response=emission_herdr_nexus::Response::Executed(outcome).signalize().map_err(|e|ServerError::Signal(format!("{e:?}")))?;
 stream.write_frame(&response,SharedCapacity::default()).map_err(ServerError::Frame)
}
use shared_translator::FrameCapacity as SharedCapacity;
fn execute(config:meta_signal_herdr_nexus::Configuration, herdr:Herdr)->ExternalOutcome{
 let timeout=Duration::from_millis(config.call_timeout_milliseconds.0.max(1) as u64);
 let (mut argv,pane,kind)=argv_for(&herdr);
 argv.splice(0..0, ["--session".into(), herdr.session.0.clone()]);
 let output=match run_command(&config.herdr_executable.0,&argv,timeout){Ok(v)=>v,Err(RunError::Timeout)=>return ExternalOutcome::Uncertain,Err(RunError::Io(e))=>return refused("process_io",None,e.to_string())};
 if !output.status.success(){return refused("process_refused",Some(output.status.code().unwrap_or(-1) as i64),String::from_utf8_lossy(&output.stderr).into_owned());}
 if kind=="list" {
  let mut agent_argv=vec!["--session".into(),herdr.session.0.clone(),"agent".into(),"list".into()];
  let agents=match run_command(&config.herdr_executable.0,&agent_argv,timeout){Ok(v)=>v,Err(_)=>return ExternalOutcome::Uncertain};
  if !agents.status.success(){return refused("agent_list_failed",Some(agents.status.code().unwrap_or(-1) as i64),String::from_utf8_lossy(&agents.stderr).into_owned());}
  return parse_list_join(&output.stdout,&agents.stdout,herdr.session);
 }
 parse_success(&output.stdout,kind,pane,herdr.session)
}
fn argv_for(h:&Herdr)->(Vec<String>,Option<String>,&'static str){match &h.verb{
 Verb::WorkspaceCreate(d)=>(vec!["workspace".into(),"create".into(),"--cwd".into(),d.root.0.clone()],None,"workspace"),
 Verb::AgentStart(d)=>{let mut a=vec!["agent".into(),"start".into(),d.command.agent_name.0.clone(),"--kind".into(),format!("{:?}",d.command.agent_kind).to_lowercase(),"--pane".into(),d.pane_id.0.clone()]; if let signal_herdr_nexus::Executable::Explicit(p)=&d.command.executable{a.extend(["--executable".into(),p.0.clone()]);} if !d.command.string_vector.is_empty(){a.push("--".into());} a.extend(d.command.string_vector.clone());(a,Some(d.pane_id.0.clone()),"start")},
 Verb::AgentPrompt(d)=>(vec!["agent".into(),"prompt".into(),d.pane_id.0.clone(),d.text.0.clone()],Some(d.pane_id.0.clone()),"prompt"),
 Verb::Keys(d)=>(vec!["pane".into(),"send-keys".into(),d.pane_id.0.clone()],Some(d.pane_id.0.clone()),"keys"),
 Verb::PaneClose(id)=>(vec!["pane".into(),"close".into(),id.0.clone()],Some(id.0.clone()),"close"),
 Verb::PaneList=>(vec!["pane".into(),"list".into()],None,"list")}}

fn parse_list_join(panes:&[u8],agents:&[u8],session:signal_herdr_nexus::Session)->ExternalOutcome {
 let p:serde_json::Value=match serde_json::from_slice(panes){Ok(v)=>v,Err(_)=>return ExternalOutcome::Uncertain}; let a:serde_json::Value=match serde_json::from_slice(agents){Ok(v)=>v,Err(_)=>return ExternalOutcome::Uncertain};
 if p.pointer("/result/type").and_then(|v|v.as_str())!=Some("pane_list") || a.pointer("/result/type").and_then(|v|v.as_str())!=Some("agent_list"){return ExternalOutcome::Uncertain}
 let Some(ps)=p.pointer("/result/panes").and_then(|v|v.as_array()) else{return ExternalOutcome::Uncertain}; let Some(as_)=a.pointer("/result/agents").and_then(|v|v.as_array()) else{return ExternalOutcome::Uncertain};
 let mut out=Vec::new(); for pane in ps { let Some(id)=pane.get("pane_id").and_then(|v|v.as_str()) else{return ExternalOutcome::Uncertain}; let ag=as_.iter().find(|x|x.get("pane_id").and_then(|v|v.as_str())==Some(id)); let Some(ag)=ag else { out.push(emission_herdr_nexus::PaneObservation{pane:Pane{session:session.clone(),pane_id:PaneId(id.into())},state:State::Open}); continue; };; let status=ag.get("agent_status").and_then(|v|v.as_str()).unwrap_or(""); let ready=ag.get("interactive_ready").and_then(|v|v.as_bool()).unwrap_or(false); let state=if status=="working"||status=="blocked"{State::Busy}else if (status=="idle"||status=="done")&&ready{State::Idle}else{State::Open}; out.push(emission_herdr_nexus::PaneObservation{pane:Pane{session:session.clone(),pane_id:PaneId(id.into())},state}); } ExternalOutcome::Listed(out)
}
fn parse_success(bytes:&[u8],kind:&str,pane:Option<String>,session:signal_herdr_nexus::Session)->ExternalOutcome{let value:serde_json::Value=match serde_json::from_slice(bytes){Ok(v)=>v,Err(_)=>return ExternalOutcome::Uncertain}; if kind=="list"{return parse_list(&value,session)} if kind=="start"&&!value.pointer("/result/agent/interactive_ready").and_then(|v|v.as_bool()).unwrap_or(false){return ExternalOutcome::Uncertain} if kind=="workspace"{if let Some(id)=value.pointer("/result/root_pane/pane_id").and_then(|v|v.as_str()){return ExternalOutcome::Created(Pane{session,pane_id:PaneId(id.into())})}} if pane.is_some(){
  let expected=match kind {"prompt"=>"agent_prompted","keys"=>"keys_sent","close"=>"pane_closed",_=>""};
  if value.pointer("/result/type").and_then(|v|v.as_str())==Some(expected){ExternalOutcome::Done}else{ExternalOutcome::Uncertain}
 }else{ExternalOutcome::Uncertain}}
fn parse_list(value:&serde_json::Value,session:signal_herdr_nexus::Session)->ExternalOutcome {
 let items_value=value.pointer("/result/panes").or_else(|| value.pointer("/result"));
 let Some(items)=items_value.and_then(|v|v.as_array()) else { return ExternalOutcome::Uncertain };
 let mut observations=Vec::with_capacity(items.len());
 for item in items {
  let Some(id)=item.get("pane_id").and_then(|v|v.as_str()) else { return ExternalOutcome::Uncertain };
  let state=match item.get("interactive_ready").and_then(|v|v.as_bool()) { Some(true)=>State::Idle, Some(false)=>State::Busy, None=>State::Open };
  observations.push(emission_herdr_nexus::PaneObservation{pane:Pane{session:session.clone(),pane_id:PaneId(id.into())},state});
 }
 ExternalOutcome::Listed(observations)
}
fn refused(code:&str,status:Option<i64>,message:String)->ExternalOutcome{ExternalOutcome::Refused(Refusal{error_code:ErrorCode(code.into()),exit_status_option:status.map(ExitStatus),message:Message(message)})}
struct Output{status:std::process::ExitStatus,stdout:Vec<u8>,stderr:Vec<u8>}
enum RunError{Io(std::io::Error),Timeout}
fn run_command(program:&str,args:&[String],timeout:Duration)->Result<Output,RunError>{let mut child=Command::new(program).args(args).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(RunError::Io)?; let mut out=child.stdout.take().unwrap(); let mut err=child.stderr.take().unwrap(); let a=thread::spawn(move||{let mut b=Vec::new();let _=out.read_to_end(&mut b);b}); let b=thread::spawn(move||{let mut b=Vec::new();let _=err.read_to_end(&mut b);b}); let start=Instant::now(); loop{if let Some(status)=child.try_wait().map_err(RunError::Io)?{return Ok(Output{status,stdout:a.join().unwrap_or_default(),stderr:b.join().unwrap_or_default()})} if start.elapsed()>=timeout{let _=child.kill();let _=child.wait();return Err(RunError::Timeout)} thread::sleep(Duration::from_millis(5));}}
#[derive(Debug)]pub enum ServerError{Io(std::io::Error),Frame(shared_translator::FrameError),Signal(String)}

impl std::fmt::Display for ServerError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") } }
impl std::error::Error for ServerError {}
#[cfg(test)]
mod tests { use super::*; #[test] fn list_object_maps_complete_panes(){let j=br#"{"result":{"type":"pane_list","panes":[{"pane_id":"w:p","interactive_ready":true}]}}"#; match parse_success(j,"list",None,signal_herdr_nexus::Session("s".into())){ExternalOutcome::Listed(v)=>assert_eq!(v.len(),1),x=>panic!("unexpected {x:?}")} } #[test] fn list_partial_is_uncertain(){let j=br#"{"result":{"type":"pane_list"}}"#; assert!(matches!(parse_success(j,"list",None,signal_herdr_nexus::Session("s".into())),ExternalOutcome::Uncertain));} #[test] fn prompt_wrong_result_not_done(){let j=br#"{"result":{"type":"other"}}"#; assert!(matches!(parse_success(j,"prompt",Some("p".into()),signal_herdr_nexus::Session("s".into())),ExternalOutcome::Uncertain));} }
#[cfg(test)]
mod readiness_tests { use super::*; #[test] fn readiness_states_require_positive_field(){let s=signal_herdr_nexus::Session("s".into()); let ready=br#"{"result":{"panes":[{"pane_id":"p","interactive_ready":true}]}}"#; match parse_success(ready,"list",None,s){ExternalOutcome::Listed(v)=>assert_eq!(v[0].state,State::Idle),_=>panic!()} let s=signal_herdr_nexus::Session("s".into()); let unknown=br#"{"result":{"panes":[{"pane_id":"p"}]}}"#; match parse_success(unknown,"list",None,s){ExternalOutcome::Listed(v)=>assert_eq!(v[0].state,State::Open),_=>panic!()} } }
#[cfg(test)] mod missing_agent_tests { use super::*; #[test] fn unstarted_pane_is_open(){let p=br#"{"result":{"type":"pane_list","panes":[{"pane_id":"p"}]}}"#;let a=br#"{"result":{"type":"agent_list","agents":[]}}"#;match parse_list_join(p,a,signal_herdr_nexus::Session("s".into())){ExternalOutcome::Listed(v)=>assert_eq!(v[0].state,State::Open),_=>panic!()}}}
#[cfg(test)] mod listener_tests { use super::*; #[test] fn malformed_frame_does_not_poison_following_decode(){assert!(matches!(parse_success(b"bad","prompt",Some("p".into()),signal_herdr_nexus::Session("s".into())),ExternalOutcome::Uncertain)); let good=br#"{"result":{"type":"agent_prompted"}}"#; assert!(matches!(parse_success(good,"prompt",Some("p".into()),signal_herdr_nexus::Session("s".into())),ExternalOutcome::Done));}}
