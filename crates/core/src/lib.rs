//! Pure herdr Nexus state transitions.
//!
//! The proof-of-concept Memory is deliberately process-lifetime and
//! nondurable. External execution and textualization live outside this crate.

pub mod generated;

pub use generated::operation::Operation;

use emission_herdr_nexus::PaneObservation;
use generated::memory::{Answer, OwnedPane};
use signal_herdr_nexus::{
    Attempt, Command, Keys, Known, Label, MemoryRefusal, ObservedEffect, Outcome, Pane, Refusal,
    Session, State, StorePhase, StoreRefusal, Text, Workspace,
};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalResult<T> {
    Succeeded(T),
    Refused(Refusal),
    Unavailable(signal_herdr_nexus::TransportRefusal),
    Uncertain,
}

pub trait Memory {
    fn owned(&self, pane: &Pane) -> Result<Option<OwnedPane>, MemoryRefusal>;
    fn owned_in_session(&self, session: &Session) -> Result<Vec<OwnedPane>, MemoryRefusal>;
    fn project_listing(
        &self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Result<Vec<Known>, MemoryRefusal>;
    fn store_owned(&mut self, pane: OwnedPane) -> Answer;
    fn close_owned(&mut self, pane: &Pane) -> Answer;
    fn store_attempt(&mut self, pane: &Pane, attempt: Attempt) -> Answer;
    fn reconcile_listing(
        &mut self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Answer;
}

#[derive(Default)]
pub struct InMemory {
    panes: HashMap<Pane, OwnedPane>,
}

impl InMemory {
    fn project_listing(&self, session: &Session, observed: &[PaneObservation]) -> Vec<Known> {
        let by_pane: HashMap<&Pane, &PaneObservation> =
            observed.iter().map(|seen| (&seen.pane, seen)).collect();
        self.panes
            .values()
            .filter(|owned| &owned.pane.session == session)
            .map(|owned| {
                Known {
                    pane: owned.pane.clone(),
                    label_option: owned.label_option.clone(),
                    state: reconciled_state(owned, by_pane.get(&owned.pane).copied()),
                    attempt: owned.attempt.clone(),
                }
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

    fn project_listing(
        &self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Result<Vec<Known>, MemoryRefusal> {
        Ok(self.project_listing(session, observed))
    }

    fn store_owned(&mut self, pane: OwnedPane) -> Answer {
        self.panes.insert(pane.pane.clone(), pane);
        Answer::Succeeded
    }

    fn close_owned(&mut self, pane: &Pane) -> Answer {
        self.panes.remove(pane);
        Answer::Succeeded
    }

    fn store_attempt(&mut self, pane: &Pane, attempt: Attempt) -> Answer {
        if let Some(owned) = self.panes.get_mut(pane) {
            owned.attempt = attempt;
            Answer::Succeeded
        } else {
            Answer::Failed(MemoryRefusal(signal_herdr_nexus::Message(
                "attempt target is not owned".into(),
            )))
        }
    }

    fn reconcile_listing(
        &mut self,
        session: &Session,
        observed: &[PaneObservation],
    ) -> Answer {
        let by_pane: HashMap<&Pane, &PaneObservation> =
            observed.iter().map(|seen| (&seen.pane, seen)).collect();
        for owned in self
            .panes
            .values_mut()
            .filter(|owned| &owned.pane.session == session)
        {
            owned.state = reconciled_state(owned, by_pane.get(&owned.pane).copied());
            owned.attempt = Attempt::Settled;
        }
        Answer::Succeeded
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
                    label_option: None,
                    state: State::Open,
                    attempt: Attempt::Settled,
                };
                match self.memory.store_owned(owned) {
                    Answer::Succeeded => Outcome::Created(pane),
                    Answer::Failed(refusal) => {
                        store_refused(refusal, StorePhase::Observed(ObservedEffect::Created(pane)))
                    }
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => Outcome::Uncertain,
        }
    }

    pub fn start(
        &mut self,
        pane: Pane,
        _command: Command,
        label: Label,
        external: ExternalResult<State>,
    ) -> Outcome {
        let mut owned = match self.require_owned(&pane) {
            Ok(owned) => owned,
            Err(outcome) => return outcome,
        };
        match external {
            ExternalResult::Succeeded(state) => {
                owned.label_option = Some(label);
                owned.state = state;
                owned.attempt = Attempt::Settled;
                match self.memory.store_owned(owned) {
                    Answer::Succeeded => Outcome::Done,
                    Answer::Failed(refusal) => {
                        store_refused(refusal, StorePhase::Observed(ObservedEffect::Done))
                    }
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(&pane),
        }
    }

    pub fn prompt(&mut self, pane: Pane, _text: Text, external: ExternalResult<()>) -> Outcome {
        if let Err(outcome) = self.require_owned(&pane) {
            return outcome;
        }
        self.finish_done(pane, external)
    }

    pub fn press(&mut self, pane: Pane, _keys: Keys, external: ExternalResult<()>) -> Outcome {
        if let Err(outcome) = self.require_owned(&pane) {
            return outcome;
        }
        self.finish_done(pane, external)
    }

    pub fn close(&mut self, pane: Pane, external: ExternalResult<()>) -> Outcome {
        if let Err(outcome) = self.require_owned(&pane) {
            return outcome;
        }
        match external {
            ExternalResult::Succeeded(()) => match self.memory.close_owned(&pane) {
                Answer::Succeeded => Outcome::Done,
                Answer::Failed(refusal) => store_refused(refusal, StorePhase::Observed(ObservedEffect::Done)),
            },
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(&pane),
        }
    }

    pub fn list(
        &mut self,
        session: Session,
        external: ExternalResult<Vec<PaneObservation>>,
    ) -> Outcome {
        match external {
            ExternalResult::Succeeded(observed) => {
                let projected = match self.memory.project_listing(&session, &observed) {
                    Ok(projected) => projected,
                    Err(refusal) => {
                        return store_refused(
                            refusal,
                            StorePhase::Observed(ObservedEffect::Listed(Vec::new())),
                        )
                    }
                };
                match self.memory.reconcile_listing(&session, &observed) {
                    Answer::Succeeded => Outcome::Listed(projected),
                    Answer::Failed(refusal) => store_refused(
                        refusal,
                        StorePhase::Observed(ObservedEffect::Listed(projected)),
                    ),
                }
            }
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => Outcome::Uncertain,
        }
    }

    pub fn subscribe(&mut self, _session: Session, external: ExternalResult<()>) -> Outcome {
        match external {
            ExternalResult::Succeeded(()) => Outcome::Done,
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => Outcome::Uncertain,
        }
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

    fn finish_done(&mut self, pane: Pane, external: ExternalResult<()>) -> Outcome {
        match external {
            ExternalResult::Succeeded(()) => match self.memory.store_attempt(&pane, Attempt::Settled) {
                Answer::Succeeded => Outcome::Done,
                Answer::Failed(refusal) => store_refused(
                    refusal,
                    StorePhase::Observed(ObservedEffect::Done),
                ),
            },
            ExternalResult::Refused(refusal) => Outcome::Refused(refusal),
            ExternalResult::Unavailable(refusal) => Outcome::Unavailable(refusal),
            ExternalResult::Uncertain => self.retain_uncertain(&pane),
        }
    }

    fn retain_uncertain(&mut self, pane: &Pane) -> Outcome {
        match self.memory.store_attempt(pane, Attempt::Uncertain) {
            Answer::Succeeded => Outcome::Uncertain,
            Answer::Failed(refusal) => store_refused(refusal, StorePhase::Uncertain),
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
        StoreAttempt,
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

        fn refusal(&mut self, call: Call) -> Answer {
            if self.next.get() == Some(call) {
                self.next.set(None);
                Answer::Failed(MemoryRefusal(Message("selected test refusal".into())))
            } else {
                Answer::Succeeded
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

        fn project_listing(&self, session: &Session, observed: &[PaneObservation]) -> Result<Vec<Known>, MemoryRefusal> {
            Ok(self.memory.project_listing(session, observed))
        }

        fn store_owned(&mut self, pane: OwnedPane) -> Answer {
            match self.refusal(Call::StoreOwned) {
                Answer::Succeeded => self.memory.store_owned(pane),
                failed => failed,
            }
        }

        fn close_owned(&mut self, pane: &Pane) -> Answer {
            match self.refusal(Call::CloseOwned) {
                Answer::Succeeded => self.memory.close_owned(pane),
                failed => failed,
            }
        }

        fn store_attempt(&mut self, pane: &Pane, attempt: Attempt) -> Answer {
            match self.refusal(Call::StoreAttempt) {
                Answer::Succeeded => self.memory.store_attempt(pane, attempt),
                failed => failed,
            }
        }

        fn reconcile_listing(
            &mut self,
            session: &Session,
            observed: &[PaneObservation],
        ) -> Answer {
            if self.next.get() == Some(Call::ReconcileListing) {
                self.next.set(None);
                return Answer::Failed(MemoryRefusal(Message("selected test refusal".into())));
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
                label_option: Some(Label("mind/versionOne".into())),
                state: State::Open,
                attempt: Attempt::Settled,
            })
            ;
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
            core.prompt(pane(), Text("hello".into()), ExternalResult::Succeeded(())),
            Outcome::Unknown(pane())
        );
    }

    #[test]
    fn memory_refuses_to_claim_an_attempt_change_for_an_absent_pane() {
        let mut memory = InMemory::default();
        assert!(matches!(
            memory.store_attempt(&pane(), Attempt::Uncertain),
            Answer::Failed(_)
        ));
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
            core.prompt(pane(), Text("hello".into()), ExternalResult::Refused(refusal.clone())),
            Outcome::Refused(refusal)
        );
    }

    #[test]
    fn uncertainty_is_bounded_by_the_owned_pane() {
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), Text("hello".into()), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(
            core.memory().owned(&pane()).expect("read").expect("owned").attempt,
            Attempt::Uncertain
        );
    }

    #[test]
    fn repeated_uncertainty_remains_one_owned_pane_fact() {
        let mut core = Core::new(seeded());
        assert_eq!(
            core.prompt(pane(), Text("hello".into()), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        assert_eq!(
            core.press(pane(), Keys(vec!["x".into()]), ExternalResult::Uncertain),
            Outcome::Uncertain
        );
        let owned = core.memory().owned(&pane()).expect("read").expect("owned");
        assert_eq!(owned.attempt, Attempt::Uncertain);
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
        let mut core = Core::new(RefusingMemory::new(seeded(), Call::StoreAttempt));
        let outcome = core.press(pane(), Keys(vec!["x".into()]), ExternalResult::Uncertain);
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
                label_option: Some(Label("mind/versionOne".into())),
                state: State::Dead,
                attempt: Attempt::Settled,
            }])
        );
    }
}
