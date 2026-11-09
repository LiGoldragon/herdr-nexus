#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PaneCloseArgument(pub signal_herdr_nexus::PaneId);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ListArgument {
    Workspace,
    Pane,
    Agent,
}
#[rustfmt::skip]
pub trait Arguable {
    fn arguments(&self) -> std::vec::Vec<String>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_workspacecreate_data_arguable<T: Arguable>() {}
    let _ = assert_workspacecreate_data_arguable::<
        emission_herdr_nexus::WorkspaceCreate_Data,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_agentstart_data_arguable<T: Arguable>() {}
    let _ = assert_agentstart_data_arguable::<emission_herdr_nexus::AgentStart_Data>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_agentprompt_data_arguable<T: Arguable>() {}
    let _ = assert_agentprompt_data_arguable::<emission_herdr_nexus::AgentPrompt_Data>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_sendkeys_data_arguable<T: Arguable>() {}
    let _ = assert_sendkeys_data_arguable::<emission_herdr_nexus::SendKeys_Data>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_panecloseargument_arguable<T: Arguable>() {}
    let _ = assert_panecloseargument_arguable::<PaneCloseArgument>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_listargument_arguable<T: Arguable>() {}
    let _ = assert_listargument_arguable::<ListArgument>;
};
