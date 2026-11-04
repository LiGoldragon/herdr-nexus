#[path = "generated/emission.rs"]
mod generated;
pub use generated::*;

/// The temporary Signal-root Emission contract source.
pub const ETHOS: &str = include_str!("../../../ethos/emission.ethos");

impl signal::Contracted for Query {
    const CONTRACT_SOURCE: &'static str = ETHOS;
}

const MANAGED_WORKSPACE_PREFIX: &str = "herdr-nexus-v1:";

/// Encodes the creation correlation into the one external workspace label.
pub fn managed_workspace_label(id: &signal_herdr_nexus::CreationId) -> WorkspaceLabel {
    WorkspaceLabel(format!(
        "{MANAGED_WORKSPACE_PREFIX}{}:{}:{}",
        id.instance_id.0.len(),
        id.instance_id.0,
        id.sequence.0,
    ))
}

/// Recognizes only the canonical label emitted by [`managed_workspace_label`].
pub fn managed_creation_id(label: &WorkspaceLabel) -> Option<signal_herdr_nexus::CreationId> {
    let rest = label.0.strip_prefix(MANAGED_WORKSPACE_PREFIX)?;
    let (length_text, rest) = rest.split_once(':')?;
    let length: usize = length_text.parse().ok()?;
    let instance = rest.get(..length)?;
    let sequence = rest.get(length..)?.strip_prefix(':')?;
    let sequence: i64 = sequence.parse().ok()?;
    if length.to_string() != length_text
        || sequence <= 0
        || sequence.to_string() != rest.get(length + 1..)?
    {
        return None;
    }
    Some(signal_herdr_nexus::CreationId {
        instance_id: signal_herdr_nexus::InstanceId(instance.to_owned()),
        sequence: signal_herdr_nexus::Sequence(sequence),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_workspace_marker_round_trips_delimiter_bearing_instance() {
        let creation = signal_herdr_nexus::CreationId {
            instance_id: signal_herdr_nexus::InstanceId("a:b:λ".into()),
            sequence: signal_herdr_nexus::Sequence(42),
        };
        let label = managed_workspace_label(&creation);
        assert_eq!(managed_creation_id(&label), Some(creation));
    }

    #[test]
    fn managed_workspace_marker_requires_canonical_full_consumption() {
        for label in [
            "herdr-nexus-v1:01:a:1",
            "herdr-nexus-v1:1:a:01",
            "herdr-nexus-v1:2:a:1",
            "herdr-nexus-v1:1:a:1:tail",
            "ordinary-label",
        ] {
            assert_eq!(managed_creation_id(&WorkspaceLabel(label.into())), None);
        }
    }
}
