//! Capability ports for independent observation delivery and cancellation.
use std::os::fd::BorrowedFd;

pub trait Deliverable {
    type Item;
    type Failure;
    fn deliver(&self, item: Self::Item) -> Result<(), Self::Failure>;
}

pub trait Cancellable {
    fn descriptor(&self) -> BorrowedFd<'_>;
}

pub trait SourceObservable {
    type Request;
    type Observation;
    type Failure;
    fn observe<D, C>(&self, request: Self::Request, delivery: &D, cancellation: &C)
        -> Result<(), Self::Failure>
    where
        D: Deliverable<Item = Self::Observation, Failure = Self::Failure>,
        C: Cancellable;
}
