#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ObservationScope {
    WholeSession,
    PaneId(signal_herdr_nexus::PaneId),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum InvalidationCause {
    Created,
    Closed,
    Exited,
    DetectionChanged,
    StatusChanged,
    PresentationChanged,
    Moved,
    MembershipChanged,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Observe_Data {
    pub session: signal_herdr_nexus::observation_status::Session,
    pub source_generation: signal_herdr_nexus::observation_status::SourceGeneration,
    pub pane_id_vector: std::vec::Vec<signal_herdr_nexus::PaneId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Query {
    Observe(Observe_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Invalidated_Data {
    pub source_generation: signal_herdr_nexus::observation_status::SourceGeneration,
    pub observation_scope: ObservationScope,
    pub invalidation_cause: InvalidationCause,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Detached_Data {
    pub source_generation: signal_herdr_nexus::observation_status::SourceGeneration,
    pub source_failure: signal_herdr_nexus::observation_status::SourceFailure,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Response {
    Attached(signal_herdr_nexus::observation_status::SourceGeneration),
    Invalidated(Invalidated_Data),
    Detached(Detached_Data),
}
