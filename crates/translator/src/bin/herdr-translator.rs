use std::io::{self, Read};
use herdr_nexus_translator::server;
use shared_translator::{FrameCapacity, FrameReading, Restorable, Signal};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdin = io::stdin();
    let body = stdin.read_frame(FrameCapacity::default())?;
    let place = Signal::<shared_translator::placement::Place>::from(Vec::<u8>::from(body)).restore()?;
    server::run(&place)?;
    Ok(())
}
