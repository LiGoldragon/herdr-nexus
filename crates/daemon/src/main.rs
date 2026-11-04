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
                    configuration.transport_timeout,
                )?;
                let body = input.read_frame(capacity)?;
                let call = Signal::<herdr_nexus_wire::public_call::Query>::from(Vec::<u8>::from(body)).restore()?;
                let herdr_nexus_wire::public_call::Query::Call(call) = call;
                if !herdr_nexus_daemon::same_login(&call.caller) {
                    let refusal = herdr_nexus_wire::public_call::Response::Failed(
                        "caller is outside the bound login".into(),
                    );
                    let mut output = herdr_nexus_daemon::DeadlineStream::new(
                        stream,
                        configuration.transport_timeout,
                    )?;
                    output.write_frame(&refusal.signalize()?, capacity)?;
                    return Ok(());
                }
                let query = call.input.0;
                if let signal_herdr_nexus::Query::Subscribe(session) = query {
                    return dispatcher.subscribe(stream, session, configuration);
                }
                let response = dispatcher.dispatch(query, Arc::clone(&configuration));
                let response = herdr_nexus_wire::public_call::Response::Answered(
                    herdr_nexus_wire::public_payload::Output(response),
                );
                let mut output =
                    herdr_nexus_daemon::DeadlineStream::new(stream, configuration.transport_timeout)?;
                output.write_frame(&response.signalize()?, capacity)?;
                Ok(())
            })();
            let _ = result;
        });
    }
}
