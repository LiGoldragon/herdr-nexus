#[path = "generated/emission.rs"]
mod generated;
pub use generated::*;

/// The temporary Signal-root Emission contract source.
pub const ETHOS: &str = include_str!("../../../ethos/emission.ethos");

impl signal::Contracted for Query {
    const CONTRACT_SOURCE: &'static str = ETHOS;
}
