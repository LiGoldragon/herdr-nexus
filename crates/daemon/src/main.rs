use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_capacity = FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES);
    let place = {
        let mut input = std::io::stdin().lock();
        let body = input.read_frame(bootstrap_capacity)?;
        let bootstrap =
            Signal::<herdr_nexus_wire::transport_types::CoreBootstrap>::from(Vec::<u8>::from(body))
                .restore()?;
        bootstrap
    };
    let admission = herdr_nexus_wire::ValidAdmission::try_from(&place.admission)
        .map_err(|message| format!("invalid core bootstrap admission: {message}"))?;
    let bootstrap = Arc::new(herdr_nexus_daemon::Bootstrap::bind(
        &place.place,
        admission,
        place.peer_identity,
        place.publication_channel,
    )?);
    let dispatcher = Arc::new(herdr_nexus_daemon::Dispatcher::new(
        bootstrap.paths(),
        bootstrap.configuration_store(),
        place.instance_id,
    ));

    let meta = Arc::clone(&bootstrap);
    std::thread::spawn(move || loop {
        if let Err(error) = meta.serve_next_meta() {
            eprintln!("herdr Nexus meta listener failed permanently: {error}");
            std::process::exit(1);
        }
    });

    loop {
        let accepted = match bootstrap.accept_public() {
            Ok(accepted) => accepted,
            Err(error) => {
                return Err(format!("herdr Nexus public listener failed permanently: {error}").into());
            }
        };
        let stream = accepted.stream;
        let authority = accepted.authority;
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
                let call =
                    Signal::<herdr_nexus_wire::public_call::Query>::from(Vec::<u8>::from(body))
                        .restore()?;
                let herdr_nexus_wire::public_call::Query::Call(call) = call;
                let actual = herdr_nexus_wire::frame::peer_caller(&stream)?;
                let peer_grant = authority.peer_grant();
                let principal = herdr_nexus_daemon::principal_for(&authority, &actual, &call.caller)?;
                let response = match dispatcher.dispatch_as(
                    principal.clone(),
                    call.input.0,
                    Arc::clone(&configuration),
                ) {
                    herdr_nexus_daemon::PublicDispatch::Subscribe(session) => {
                        return dispatcher.subscribe_bound(
                            stream,
                            principal,
                            peer_grant,
                            session,
                            configuration,
                        )
                    }
                    herdr_nexus_daemon::PublicDispatch::Answered(response) => response,
                };
                let response = herdr_nexus_wire::public_call::Response::Answered(
                    herdr_nexus_wire::public_payload::Output(response),
                );
                let mut output = herdr_nexus_daemon::DeadlineStream::new(
                    stream,
                    configuration.transport_timeout,
                )?;
                output.write_frame(&response.signalize()?, capacity)?;
                Ok(())
            })();
            if let Err(error) = result {
                eprintln!("herdr Nexus public request failed: {error}");
            }
        });
    }
}
