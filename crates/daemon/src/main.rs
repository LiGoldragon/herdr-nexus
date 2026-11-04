use signal::{FrameCapacity, FrameReading, Restorable, Signal};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let place = {
        let mut input = std::io::stdin().lock();
        let body = input.read_frame(bootstrap_capacity)?;
        Signal::<shared_translator::placement::Place>::from(Vec::<u8>::from(body)).restore()?
    };
    let bootstrap = Arc::new(herdr_nexus_daemon::Bootstrap::bind(&place)?);
    let dispatcher = Arc::new(herdr_nexus_daemon::Dispatcher::new(bootstrap.paths()));

    let meta = Arc::clone(&bootstrap);
    std::thread::spawn(move || loop {
        if meta.serve_next_meta(bootstrap_capacity).is_err() {
            return;
        }
    });

    loop {
        let mut stream = bootstrap.accept_public()?;
        let Some(configuration) = bootstrap.snapshot() else {
            continue;
        };
        let capacity = FrameCapacity::from(configuration.frame_capacity_bytes);
        let dispatcher = Arc::clone(&dispatcher);
        herdr_nexus_daemon::serve_public_once(&mut stream, capacity, move |query| {
            dispatcher.dispatch(query, configuration)
        })?;
    }
}
