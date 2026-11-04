use herdr_nexus_translator::{maximum_text_budget, registry, TextClient};
use herdr_nexus_translator::transport_types::Lane;
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let registry_path = arguments.next().ok_or("REGISTRY_PATH")?;
    let name = arguments.next().ok_or("NAME")?;
    let text = arguments.next().ok_or("DATOM")?;
    let registry = registry::decode(&fs::read_to_string(registry_path)?, &mut maximum_text_budget())
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
