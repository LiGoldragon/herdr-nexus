use herdr_nexus_translator::maximum_text_budget;
use herdr_nexus_translator::transport_types::{
    Admission, CoreBootstrap, FrameCapacityBytes, FrameDeadlineMilliseconds,
    RetryDelayMilliseconds, TranslatorBootstrap,
};
use shared_translator::{PeerIdentity, ProcessId, UserId};
use meta_signal_herdr_nexus::{BudgetValue, DepthLimit, NodeLimit};
use shared_translator::{FrameCapacity, FrameWriting, Signalizable};
use std::{env, io};

fn integer(value: Option<String>, name: &str) -> Result<i64, Box<dyn std::error::Error>> {
    value.ok_or_else(|| name.to_owned())?.parse().map_err(Into::into)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let place_text = arguments.next().ok_or("PLACE")?;
    let target = arguments.next().ok_or("TARGET (Core or Translator)")?;
    let peer_identity = PeerIdentity {
        user_id: UserId(integer(arguments.next(), "PEER_USER_ID")?),
        process_id: ProcessId(integer(arguments.next(), "PEER_PROCESS_ID")?),
    };
    let admission = Admission {
        frame_deadline_milliseconds: FrameDeadlineMilliseconds(integer(arguments.next(), "FRAME_DEADLINE_MILLISECONDS")?),
        frame_capacity_bytes: FrameCapacityBytes(integer(arguments.next(), "FRAME_CAPACITY_BYTES")?),
        retry_delay_milliseconds: RetryDelayMilliseconds(integer(arguments.next(), "RETRY_DELAY_MILLISECONDS")?),
    };
    herdr_nexus_wire::ValidAdmission::try_from(&admission)
        .map_err(|message| format!("invalid admission: {message}"))?;
    let place = shared_translator::decode::<shared_translator::Place>(
        &place_text,
        &mut maximum_text_budget(),
    )
        .map_err(|error| format!("{error:?}"))?;
    shared_translator::placement::validate_place_name(&place.place_name.0)?;
    let mut output = io::stdout();
    match target.as_str() {
        "Core" => {
            let instance_id = signal_herdr_nexus::InstanceId(
                arguments.next().ok_or("INSTANCE_ID")?,
            );
            output.write_frame(
                &CoreBootstrap { place, admission, peer_identity, instance_id }.signalize()?,
                FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES),
            )?
        },
        "Translator" => {
            let budget_value = BudgetValue {
                node_limit: NodeLimit(integer(arguments.next(), "TEXT_NODE_LIMIT")?),
                depth_limit: DepthLimit(integer(arguments.next(), "TEXT_DEPTH_LIMIT")?),
            };
            output.write_frame(
                &TranslatorBootstrap { place, admission, budget_value, peer_identity }.signalize()?,
                FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES),
            )?;
        }
        _ => return Err("TARGET must be Core or Translator".into()),
    }
    if arguments.next().is_some() {
        return Err("unexpected bootstrap argument".into());
    }
    Ok(())
}
