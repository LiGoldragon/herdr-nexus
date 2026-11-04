use datom_codec::Budget;
use herdr_nexus_translator::registry;
use herdr_nexus_translator::transport_types::{
    Admission, CoreBootstrap, FrameCapacityBytes, FrameDeadlineMilliseconds, TranslatorBootstrap,
};
use shared_translator::{PeerIdentity, ProcessId, UserId};
use meta_signal_herdr_nexus::{BudgetValue, DepthLimit, NodeLimit};
use shared_translator::{FrameCapacity, FrameWriting, Signalizable};
use std::{env, fs, io};

fn budget() -> Budget {
    Budget {
        remaining: i64::try_from(signal::MAXIMUM_SIGNAL_BYTES).expect("signal bound fits budget"),
        reader: protos::ReaderBudget { remaining: signal::MAXIMUM_SIGNAL_BYTES },
        depth: 0,
        maximum_depth: i64::try_from(signal::MAXIMUM_SIGNAL_DEPTH).expect("signal depth fits budget"),
    }
}

fn integer(value: Option<String>, name: &str) -> Result<i64, Box<dyn std::error::Error>> {
    value.ok_or_else(|| name.to_owned())?.parse().map_err(Into::into)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let registry_path = arguments.next().ok_or("REGISTRY_PATH")?;
    let name = arguments.next().ok_or("NAME")?;
    let target = arguments.next().ok_or("TARGET (Core or Translator)")?;
    let peer_identity = PeerIdentity {
        user_id: UserId(integer(arguments.next(), "PEER_USER_ID")?),
        process_id: ProcessId(integer(arguments.next(), "PEER_PROCESS_ID")?),
    };
    let admission = Admission {
        frame_deadline_milliseconds: FrameDeadlineMilliseconds(integer(arguments.next(), "FRAME_DEADLINE_MILLISECONDS")?),
        frame_capacity_bytes: FrameCapacityBytes(integer(arguments.next(), "FRAME_CAPACITY_BYTES")?),
    };
    herdr_nexus_wire::ValidAdmission::try_from(&admission)
        .map_err(|message| format!("invalid admission: {message}"))?;
    let registry = registry::decode(&fs::read_to_string(registry_path)?, &mut budget())
        .map_err(|error| format!("{error:?}"))?;
    let place = registry::select(&registry, &name).map_err(|error| format!("{error:?}"))?.clone();
    let mut output = io::stdout();
    match target.as_str() {
        "Core" => output.write_frame(
            &CoreBootstrap { place, admission, peer_identity }.signalize()?,
            FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES),
        )?,
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
