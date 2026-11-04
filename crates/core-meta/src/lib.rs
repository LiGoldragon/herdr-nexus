#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CoreSet {
    StartTimeout(meta_signal_herdr_nexus::Timeout),
    Polling(meta_signal_herdr_nexus::Timeout),
    FrameCapacity(meta_signal_herdr_nexus::ByteLimit),
    TransportTimeout(meta_signal_herdr_nexus::Timeout),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Query {
    Configure(CoreSet),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Response {
    Configured(meta_signal_herdr_nexus::Readiness),
    ConfigureRefused(meta_signal_herdr_nexus::ConfigurationRefusal),
}

/// The generated internal meta contract source.
pub const ETHOS: &str = include_str!("../../../ethos/core-meta.ethos");

impl signal::Contracted for Query {
    const CONTRACT_SOURCE: &'static str = ETHOS;
}
