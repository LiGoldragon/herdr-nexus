//! Pure herdr Nexus state transitions.
//!
//! The proof-of-concept Memory is deliberately process-lifetime and
//! nondurable. External execution and textualization live outside this crate.

pub mod generated;

use emission_herdr_nexus::PaneObservation;
use signal_herdr_nexus::{
    Known, Label, MemoryRefusal, ObservedEffect, Outcome, Pane, Refusal, Session, State,
    StorePhase, StoreRefusal, Workspace,
};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Attempt {
    Create,
    Start(Pane),
    Prompt(Pane),
    Press(Pane),
    Close(Pane),
    List(Session),
    Subscribe(Session),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalResult<T> {
    Succeeded(T),
    Refused(Refusal),
    Unavailable(signal_herdr_nexus::TransportRefusal),
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedPane {
    pub pane: Pane,
    pub workspace: Workspace,
    pub label: Option<Label>,
    pub state: State,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reconciliation {
    pub attempt: Attempt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListingRefusal {
    pub memory_refusal: MemoryRefusal,
    pub observed: Vec<Known>,
}

pub trait Memory {
    fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal>;
    fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal>;
    fn store_owned(&mut self, pane: OwnedPane) -> Result<(), MemoryRefusal>;
    fn close_owned(&mut self, pane: &Pane) -> Result<(), MemoryRefusal>;
    fn store_uncertain(&mut self, reconciliation: Reconciliation) -> Result<(), MemoryRefusal>;
    fn reconcile_listing(
        &mut self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Result<Vec<Known>, ListingRefusal>;
}

#[derive(Default)]
pub struct InMemory {
    panes: HashMap<Pane, OwnedPane>,
    uncertain: Vec<Reconciliation>,
}

impl InMemory {
    pub fn uncertain(&self) -> &[Reconciliation] {
        &self.uncertain
    }

    fn project_listing(&self, session: &Session, observed: &[PaneObservation]) -> Vec<Known> {
        let by_pane: HashMap<&Pane, &PaneObservation> =
            observed.iter().map(|seen| (&seen.pane, seen)).collect();
        self.panes
            .values()
            .filter(|owned| &owned.pane.session == session)
            .filter_map(|owned| {
                owned.label.clone().map(|label| Known {
                    pane: owned.pane.clone(),
                    label,
                    state: reconciled_state(owned, by_pane.get(&owned.pane).copied()),
                })
            })
            .collect()
    }
}

impl Memory for InMemory {
    fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal> {
        Ok(self.panes.get(pane).cloned())
    }

    fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal> {
        Ok(self
            .panes
            .values()
            .filter(|owned| &owned.pane.session == session)
            .cloned()
            .collect())
    }

    fn store_owned(&mut self, pane: OwnedPane) -> Result<(), MemoryRefusal> {
        self.panes.insert(pane.pane.clone(), pane);
        Ok(())
    }

    fn close_owned(&mut self, pane: &Pane) -> Result<(), MemoryRefusal> {
        self.panes.remove(pane);
        Ok(())
    }

    fn store_uncertain(&mut self, reconciliation: Reconciliation) -> Result<(), MemoryRefusal> {
        if let Some(existing) = self
            .uncertain
            .iter_mut()
            .find(|existing| same_uncertainty_locus(&existing.attempt, &reconciliation.attempt))
        {
            *existing = reconciliation;
        } else {
            self.uncertain.push(reconciliation);
        }
        Ok(())
    }

    fn reconcile_listing(
        &mut self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Result<Vec<Known>, ListingRefusal> {
        let known = self.project_listing(session, observed);
        let by_pane: HashMap<&Pane, &PaneObservation> =
            observed.iter().map(|seen| (&seen.pane, seen)).collect();
        for owned in self
            .panes
            .values_mut()
            .filter(|owned| &owned.pane.session == session)
        {
            owned.state = reconciled_state(owned, by_pane.get(&owned.pane).copied());
        }
        Ok(known)
    }
}

fn reconciled_state(owned: &OwnedPane, seen: Option<&PaneObservation>) -> State {
    match seen {
        None => State::Dead,
        Some(seen)
            if seen.state == State::Open
                && matches!(owned.state, State::Busy | State::Idle | State::Dead) =>
        {
            State::Dead
        }
        Some(seen) => seen.state.clone(),
    }
}

fn same_uncertainty_locus(left: &Attempt, right: &Attempt) -> bool {
    use Attempt::*;
    fn pane(attempt: &Attempt) -> Option<&Pane> {
        match attempt {
            Attempt::Start(pane)
            | Attempt::Prompt(pane)
            | Attempt::Press(pane)
            | Attempt::Close(pane) => Some(pane),
            _ => None,
        }
    }
    if let (Some(left), Some(right)) = (pane(left), pane(right)) {
        return left == right;
    }
    match (left, right) {
        (List(a), List(b)) | (Subscribe(a), Subscribe(b)) => a == b,
        (Create, Create) => true,
        _ => false,
    }
}

pub struct Core<M> {
    memory: M,
}

impl<M> Core<M> {
    pub fn new(memory: M) -> Self {
        Self { memory }
    }

    pub fn memory(&self) -> &M {
        &self.memory
    }
}

impl<M: Memory> Core<M> {
    pub fn create(&mut self, workspace: Workspace, external: ExternalResult<Pane>) -> Outcome {
        match external {
            ExternalResult::Succeeded(pane) => {
                let owned = OwnedPane {
                    pane: pane.clone(),
                    workspace,
                    label: None,
                    state: State::Open,
                };
                match self.memory.store_owned(owned) {
                    Ok(()) => Outcome::Created(pane),
                    Err(refusal) => {
                        store_refused(refusal, StorePhase::Observed(ObservedEffect::Created(pane)))
                    }
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(Attempt::Create),
        }
    }

    pub fn start(&mut self, pane: Pane, label: Label, external: ExternalResult<State>) -> Outcome {
        let mut owned = match self.require_owned(&pane) {
            Ok(owned) => owned,
            Err(outcome) => return outcome,
        };
        match external {
            ExternalResult::Succeeded(state) => {
                owned.label = Some(label);
                owned.state = state;
                match self.memory.store_owned(owned) {
                    Ok(()) => Outcome::Done,
                    Err(refusal) => {
                        store_refused(refusal, StorePhase::Observed(ObservedEffect::Done))
                    }
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(Attempt::Start(pane)),
        }
    }

    pub fn prompt(&mut self, pane: Pane, external: ExternalResult<()>) -> Outcome {
        if let Err(outcome) = self.require_owned(&pane) {
            return outcome;
        }
        self.finish_done(Attempt::Prompt(pane), external)
    }

    pub fn press(&mut self, pane: Pane, external: ExternalResult<()>) -> Outcome {
        if let Err(outcome) = self.require_owned(&pane) {
            return outcome;
        }
        self.finish_done(Attempt::Press(pane), external)
    }

    pub fn close(&mut self, pane: Pane, external: ExternalResult<()>) -> Outcome {
        if let Err(outcome) = self.require_owned(&pane) {
            return outcome;
        }
        match external {
            ExternalResult::Succeeded(()) => match self.memory.close_owned(&pane) {
                Ok(()) => Outcome::Done,
                Err(refusal) => store_refused(refusal, StorePhase::Observed(ObservedEffect::Done)),
            },
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(Attempt::Close(pane)),
        }
    }

    pub fn list(
        &mut self,
        session: Session,
        external: ExternalResult<Vec<PaneObservation>>,
    ) -> Outcome {
        match external {
            ExternalResult::Succeeded(observed) => {
                match self.memory.reconcile_listing(&session, &observed) {
                    Ok(owned) => Outcome::Listed(owned),
                    Err(refusal) => store_refused(
                        refusal.memory_refusal,
                        StorePhase::Observed(ObservedEffect::Listed(refusal.observed)),
                    ),
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(Attempt::List(session)),
        }
    }

    pub fn subscribe(&mut self, session: Session, external: ExternalResult<()>) -> Outcome {
        self.finish_done(Attempt::Subscribe(session), external)
    }

    fn require_owned(&self, pane: &Pane) -> Result<OwnedPane, Outcome> {
        match self.memory.owned(pane) {
            Ok(Some(owned)) => Ok(owned),
            Ok(None) => Err(Outcome::Unknown(pane.clone())),
            Err(refusal) => Err(store_refused(refusal, StorePhase::BeforeEmission)),
        }
    }

    /// Checks ownership before an external effect. Callers keep their own
    /// reservation beside this Core while the effect is in flight.
    pub fn preflight_owned(&self, pane: &Pane) -> Result<(), Outcome> {
        self.require_owned(pane).map(|_| ())
    }

    /// Returns whether a session has any owned panes. An empty owned set is a
    /// complete local List result and owes no external observation.
    pub fn preflight_session(&self, session: &Session) -> Result<bool, Outcome> {
        match self.memory.owned_in_session(session) {
            Ok(owned) => Ok(!owned.is_empty()),
            Err(refusal) => Err(store_refused(refusal, StorePhase::BeforeEmission)),
        }
    }

    pub fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, Outcome> {
        self.memory
            .owned_in_session(session)
            .map_err(|refusal| store_refused(refusal, StorePhase::BeforeEmission))
    }

    fn finish_done(&mut self, attempt: Attempt, external: ExternalResult<()>) -> Outcome {
        match external {
            ExternalResult::Succeeded(()) => Outcome::Done,
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(attempt),
        }
    }

    fn retain_uncertain(&mut self, attempt: Attempt) -> Outcome {
        match self.memory.store_uncertain(Reconciliation { attempt }) {
            Ok(()) => Outcome::Uncertain,
            Err(refusal) => store_refused(refusal, StorePhase::Uncertain),
        }
    }
}

fn store_refused(memory_refusal: MemoryRefusal, store_phase: StorePhase) -> Outcome {
    Outcome::StoreRefused(StoreRefusal {
        memory_refusal,
        store_phase,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_herdr_nexus::{Message, Name, PaneId, Root, Value, Variable};
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Call {
        Owned,
        StoreOwned,
        CloseOwned,
        StoreUncertain,
        ReconcileListing,
    }

    struct RefusingMemory {
        memory: InMemory,
        next: Cell<Option<Call>>,
    }

    impl RefusingMemory {
        fn new(memory: InMemory, next: Call) -> Self {
            Self {
                memory,
                next: Cell::new(Some(next)),
            }
        }

        fn refusal(&mut self, call: Call) -> Result<(), MemoryRefusal> {
            if self.next.get() == Some(call) {
                self.next.set(None);
                Err(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                Ok(())
            }
        }
    }

    impl Memory for RefusingMemory {
        fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal> {
            if self.next.get() == Some(Call::Owned) {
                self.next.set(None);
                Err(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                self.memory.owned(pane)
            }
        }

        fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal> {
            if self.next.get() == Some(Call::Owned) {
                self.next.set(None);
                Err(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                self.memory.owned_in_session(session)
            }
        }

        fn store_owned(&mut self, pane: OwnedPane) -> Result<(), MemoryRefusal> {
            self.refusal(Call::StoreOwned)?;
            self.memory.store_owned(pane)
        }

        fn close_owned(&mut self, pane: &Pane) -> Result<(), MemoryRefusal> {
            self.refusal(Call::CloseOwned)?;
            self.memory.close_owned(pane)
        }

        fn store_uncertain(&mut self, reconciliation: Reconciliation) -> Result<(), MemoryRefusal> {
            self.refusal(Call::StoreUncertain)?;
            self.memory.store_uncertain(reconciliation)
        }

        fn reconcile_listing(
            &mut self,
            session: &Session,
            observed: &[PaneObservation],
        ) -> Result<Vec<Known>, ListingRefusal> {
            if self.next.get() == Some(Call::ReconcileListing) {
                self.next.set(None);
                return Err(ListingRefusal {
                    memory_refusal: MemoryRefusal(Message("selected test refusal".into())),
                    observed: self.memory.project_listing(session, observed),
                });
            }
            self.memory.reconcile_listing(session, observed)
        }
    }

    fn pane() -> Pane {
        Pane {
            session: Session("v1".into()),
            pane_id: PaneId("p1".into()),
        }
    }

    fn workspace() -> Workspace {
        Workspace {
            session: Session("v1".into()),
            root: Root("/tmp/v1".into()),
            variable_vector: vec![Variable {
                name: Name("FLOW_ID".into()),
                value: Value("af8fe3".into()),
            }],
        }
    }

    fn seeded() -> InMemory {
        let mut memory = InMemory::default();
        memory
            .store_owned(OwnedPane {
                pane: pane(),
                workspace: workspace(),
                label: Some(Label("mind/versionOne".into())),
                state: State::Open,
            })
            .expect("seed");
        memory
    }

    #[test]
    fn creation_success_is_owned() {
        let mut core = Core::new(InMemory::default());
        assert_eq!(
            core.create(workspace(), ExternalResult::Succeeded(pane())),
            Outcome::Created(pane())
        );
        assert!(core.memory().owned(&pane()).expect("read").is_some());
    }

    #[test]
    fn unknown_pane_stops_before_external_result_matters() {
        let mut core = Core::new(InMemory::default());
        assert_eq!(
            core.prompt(pane(), ExternalResult::Succeeded(())),
            Outcome::Unknown(pane())
        );
    }

    #[test]
    fn external_refusal_is_preserved() {
        let refusal = Refusal {
            error_code: signal_herdr_nexus::ErrorCode("agent_not_ready".into()),
            exit_status_option: None,
            message: Message("agent is busy".into()),
        };
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), ExternalResult::Refused(refusal.clone())),
            Outcome::Refused(refusal)
        );
    }

    #[test]
    fn uncertainty_is_retained_for_reconciliation() {
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(
            core.memory().uncertain(),
            &[Reconciliation {
                attempt: Attempt::Prompt(pane())
            }]
        );
    }

    #[test]
    fn repeated_uncertainty_replaces_the_one_record_for_its_pane() {
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(
            core.press(pane(), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(core.memory().uncertain().len(), 1);
        assert_eq!(core.memory().uncertain()[0].attempt, Attempt::Press(pane()));
    }

    #[test]
    fn observed_success_and_later_store_refusal_are_both_returned() {
        let mut core = Core::new(RefusingMemory::new(InMemory::default(), Call::StoreOwned));
        let outcome = core.create(workspace(), ExternalResult::Succeeded(pane()));
        assert!(matches!(
            outcome,
            Outcome::StoreRefused(StoreRefusal {
                store_phase: StorePhase::Observed(ObservedEffect::Created(_)),
                ..
            })
        ));
    }

    #[test]
    fn uncertainty_store_refusal_is_distinct() {
        let mut core = Core::new(RefusingMemory::new(seeded(), Call::StoreUncertain));
        let outcome = core.press(pane(), ExternalResult::Uncertain);
        assert!(matches!(
            outcome,
            Outcome::StoreRefused(StoreRefusal {
                store_phase: StorePhase::Uncertain,
                ..
            })
        ));
    }

    #[test]
    fn absence_becomes_dead_only_on_complete_successful_listing() {
        let mut core = Core::new(seeded());
        let outcome = core.list(Session("v1".into()), ExternalResult::Succeeded(vec![]));
        assert_eq!(
            outcome,
            Outcome::Listed(vec![Known {
                pane: pane(),
                label: Label("mind/versionOne".into()),
                state: State::Dead,
            }])
        );
    }
}
