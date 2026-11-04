//! Shared typed ingress and bounded lane framing for the herdr Nexus.

pub mod frame;
pub mod deadline;
pub mod meta_call;
pub mod meta_payload;
pub mod public_call;
pub mod public_payload;
pub mod transport_types;

pub fn socket_names() -> transport_types::SocketNames {
    transport_types::SocketNames {
        public_nexus_socket: transport_types::PublicNexusSocket("herdr.sock".into()),
        meta_nexus_socket: transport_types::MetaNexusSocket("herdr-meta.sock".into()),
        public_translator_socket: transport_types::PublicTranslatorSocket(
            "herdr-translator.sock".into(),
        ),
        meta_translator_socket: transport_types::MetaTranslatorSocket(
            "herdr-meta-translator.sock".into(),
        ),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ValidAdmission {
    pub deadline: std::time::Duration,
    pub frame_capacity: usize,
}

impl TryFrom<&transport_types::Admission> for ValidAdmission {
    type Error = &'static str;

    fn try_from(value: &transport_types::Admission) -> Result<Self, Self::Error> {
        let milliseconds = u64::try_from(value.frame_deadline_milliseconds.0)
            .ok()
            .filter(|value| *value > 0)
            .ok_or("frame deadline must be positive and representable")?;
        let deadline = std::time::Duration::from_millis(milliseconds);
        std::time::Instant::now()
            .checked_add(deadline)
            .ok_or("frame deadline overflows the monotonic clock")?;
        let frame_capacity = usize::try_from(value.frame_capacity_bytes.0)
            .ok()
            .filter(|value| (frame::HEADER_BYTES..=signal::MAXIMUM_SIGNAL_BYTES).contains(value))
            .ok_or("frame capacity is outside the shared wire bound")?;
        Ok(Self { deadline, frame_capacity })
    }
}
