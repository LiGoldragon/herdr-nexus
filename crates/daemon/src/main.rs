use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let place = {
        let mut input = std::io::stdin().lock();
        let body = input.read_frame(bootstrap_capacity)?;
        let place =
            Signal::<shared_translator::placement::Place>::from(Vec::<u8>::from(body)).restore()?;
        drop(input);
        nix::unistd::close(0)?;
        place
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
        let stream = bootstrap.accept_public()?;
        let Some(configuration) = bootstrap.snapshot() else {
            continue;
        };
        let dispatcher = Arc::clone(&dispatcher);
        std::thread::spawn(move || {
            let result = (|| -> Result<(), Box<dyn std::error::Error>> {
                let capacity = FrameCapacity::from(configuration.frame_capacity_bytes);
                let mut input = herdr_nexus_daemon::DeadlineStream::new(
                    stream.try_clone()?,
                    configuration.call_timeout,
                )?;
                let body = input.read_frame(capacity)?;
                let query =
                    Signal::<signal_herdr_nexus::Query>::from(Vec::<u8>::from(body)).restore()?;
                if let signal_herdr_nexus::Query::Subscribe(session) = query {
                    return dispatcher.subscribe(stream, session, configuration);
                }
                let response = dispatcher.dispatch(query, Arc::clone(&configuration));
                let mut output =
                    herdr_nexus_daemon::DeadlineStream::new(stream, configuration.call_timeout)?;
                output.write_frame(&response.signalize()?, capacity)?;
                Ok(())
            })();
            let _ = result;
        });
    }
}
