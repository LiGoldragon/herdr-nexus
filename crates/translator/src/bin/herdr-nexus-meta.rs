use herdr_nexus_translator::{registry, TextClient};
use herdr_nexus_translator::transport_types::Lane;
use std::{env, fs};

fn budget() -> datom_codec::Budget {
    datom_codec::Budget {
        remaining: i64::try_from(signal::MAXIMUM_SIGNAL_BYTES).expect("signal bound fits budget"),
        reader: protos::ReaderBudget { remaining: signal::MAXIMUM_SIGNAL_BYTES },
        depth: 0,
        maximum_depth: i64::try_from(signal::MAXIMUM_SIGNAL_DEPTH).expect("signal depth fits budget"),
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let registry_path = arguments.next().ok_or("REGISTRY_PATH")?;
    let name = arguments.next().ok_or("NAME")?;
    let text = arguments.next().ok_or("DATOM")?;
    let registry = registry::decode(&fs::read_to_string(registry_path)?, &mut budget())
        .map_err(|error| format!("{error:?}"))?;
    let place = registry::select(&registry, &name).map_err(|error| format!("{error:?}"))?;
    let names = herdr_nexus_wire::socket_names();
    let client = TextClient::new(
        std::path::Path::new(&place.directory.0).join(names.meta_translator_socket.0),
        signal::MAXIMUM_SIGNAL_BYTES,
    );
    println!("{}", client.request(Lane::MetaText, &text)?);
    Ok(())
}
