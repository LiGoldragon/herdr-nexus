#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Starting_Data {
    pub pane: signal_herdr_nexus::Pane,
    pub command: signal_herdr_nexus::Command,
    pub label: signal_herdr_nexus::Label,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Prompting_Data {
    pub pane: signal_herdr_nexus::Pane,
    pub text: signal_herdr_nexus::Text,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Pressing_Data {
    pub pane: signal_herdr_nexus::Pane,
    pub keys: signal_herdr_nexus::Keys,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Operation {
    Creating(signal_herdr_nexus::Workspace),
    Starting(Starting_Data),
    Prompting(Prompting_Data),
    Pressing(Pressing_Data),
    Closing(signal_herdr_nexus::Pane),
    Listing(signal_herdr_nexus::Session),
    Subscribing(signal_herdr_nexus::Session),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Created(signal_herdr_nexus::Outcome),
    Started(signal_herdr_nexus::Outcome),
    Prompted(signal_herdr_nexus::Outcome),
    Pressed(signal_herdr_nexus::Outcome),
    Closed(signal_herdr_nexus::Outcome),
    Listed(signal_herdr_nexus::Outcome),
    Subscribed(signal_herdr_nexus::Outcome),
}
