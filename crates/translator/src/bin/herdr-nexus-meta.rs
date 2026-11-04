use herdr_nexus_translator::TextClient;
use herdr_nexus_translator::transport_types::Lane;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let name = arguments.next().ok_or("METAFLOW")?;
    let text = arguments.next().ok_or("DATOM")?;
    if arguments.next().is_some() {
        return Err("unexpected argument".into());
    }
    let place = shared_translator::placement::resolve_place(&name)?;
    let names = herdr_nexus_wire::socket_names();
    let client = TextClient::new(
        shared_translator::placement::runtime_path(&place).join(names.meta_translator_socket.0),
        signal::MAXIMUM_SIGNAL_BYTES,
    );
    println!("{}", client.request(Lane::MetaText, &text)?);
    Ok(())
}
