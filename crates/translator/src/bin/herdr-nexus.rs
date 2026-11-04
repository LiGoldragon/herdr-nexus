use datom_codec::{Actualizing, Budget, Potential};
use herdr_nexus_translator::{registry, Client};
use std::{env, fs, io::Write};

fn budget() -> Budget {
    Budget {
        remaining: 1_000_000,
        reader: protos::ReaderBudget {
            remaining: 1_000_000,
        },
        depth: 0,
        maximum_depth: 1024,
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
    let client = Client::new(
        std::path::Path::new(&place.directory.0).join("herdr.sock"),
        shared_translator::FrameCapacity::default(),
    );
    let mut potential = Potential::<signal_herdr_nexus::Query>::from(text.as_str());
    let query = potential
        .actualize(&mut budget())
        .map_err(|error| format!("{error:?}"))?;
    match query {
        signal_herdr_nexus::Query::Subscribe(session) => client.subscribe(session, |response| {
            println!("{}", shared_translator::encode(&response));
            std::io::stdout()
                .flush()
                .map_err(herdr_nexus_translator::ClientError::Io)
        })?,
        query => println!("{}", shared_translator::encode(&client.request(query)?)),
    }
    Ok(())
}
