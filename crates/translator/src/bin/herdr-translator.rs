use herdr_nexus_translator::server;
use shared_translator::{FrameCapacity, FrameReading, Restorable, Signal};
use std::io;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdin = io::stdin();
    let body = stdin.read_frame(FrameCapacity::default())?;
    let bootstrap = Signal::<herdr_nexus_wire::transport_types::TranslatorBootstrap>::from(
        Vec::<u8>::from(body),
    )
    .restore()?;
    nix::unistd::close(0)?;
    server::run(&bootstrap)?;
    Ok(())
}
