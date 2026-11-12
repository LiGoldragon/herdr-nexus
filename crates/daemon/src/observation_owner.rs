//! Session-local source/read ordering. Readiness projection is deliberately a separate port.
use signal_herdr_nexus::observation_status::{
    SourceFailure, SourceGeneration, SourceStatus, Unavailable,
};
use signal_herdr_nexus::PaneId;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadIdentity {
    pub generation: SourceGeneration,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerEffect {
    Attach {
        generation: SourceGeneration,
        panes: Vec<PaneId>,
    },
    Cancel(SourceGeneration),
    Read(ReadIdentity),
    Status {
        generation: SourceGeneration,
        status: SourceStatus,
    },
    Apply(ReadIdentity),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SourcePhase {
    Detached,
    Attaching,
    Attached,
}

/// One owner per session. Its caller serializes this and Memory snapshot/registration
/// in the same turn; effects perform no I/O while that turn is held.
pub struct SessionObservationOwner {
    generation: SourceGeneration,
    phase: SourcePhase,
    membership: BTreeSet<PaneId>,
    reading: Option<ReadIdentity>,
    pending: bool,
    next_read: u64,
    subscribers: usize,
}

impl SessionObservationOwner {
    pub fn new(generation: SourceGeneration) -> Self {
        Self {
            generation,
            phase: SourcePhase::Detached,
            membership: BTreeSet::new(),
            reading: None,
            pending: false,
            next_read: 0,
            subscribers: 0,
        }
    }
    // The enclosing Nexus supplies globally unique positive generations.
    pub fn subscribe(&mut self, next: SourceGeneration) -> Vec<OwnerEffect> {
        self.subscribers += 1;
        if self.phase == SourcePhase::Detached {
            self.replace(next)
        } else {
            vec![]
        }
    }
    pub fn configure_source(&mut self, next: SourceGeneration) -> Vec<OwnerEffect> {
        if self.subscribers == 0 {
            vec![]
        } else {
            self.replace(next)
        }
    }
    fn replace(&mut self, next: SourceGeneration) -> Vec<OwnerEffect> {
        assert!(next.0 > self.generation.0, "generation must advance");
        let mut effects = vec![];
        if self.phase != SourcePhase::Detached {
            effects.push(OwnerEffect::Cancel(self.generation));
        }
        self.generation = next;
        self.phase = SourcePhase::Attaching;
        self.reading = None;
        self.pending = false;
        effects.push(OwnerEffect::Status {
            generation: next,
            status: SourceStatus::Unavailable(Unavailable::Attaching),
        });
        effects.push(OwnerEffect::Attach {
            generation: next,
            panes: self.membership.iter().cloned().collect(),
        });
        effects
    }
    pub fn attached(&mut self, generation: SourceGeneration) -> Vec<OwnerEffect> {
        if generation != self.generation || self.phase != SourcePhase::Attaching {
            return vec![];
        }
        self.phase = SourcePhase::Attached;
        let mut effects = vec![OwnerEffect::Status {
            generation,
            status: SourceStatus::AvailableUnverified,
        }];
        effects.extend(self.invalidate(generation));
        effects
    }
    pub fn invalidate(&mut self, generation: SourceGeneration) -> Vec<OwnerEffect> {
        if generation != self.generation || self.phase != SourcePhase::Attached {
            return vec![];
        }
        if self.reading.is_some() {
            self.pending = true;
            return vec![];
        }
        self.next_read = self
            .next_read
            .checked_add(1)
            .expect("read identity exhausted");
        let read = ReadIdentity {
            generation,
            sequence: self.next_read,
        };
        self.reading = Some(read.clone());
        vec![OwnerEffect::Read(read)]
    }
    /// Only complete paired reads may establish membership/absence. The Apply
    /// port must still enforce captured command revisions; it does not infer readiness.
    pub fn read_completed(
        &mut self,
        read: ReadIdentity,
        membership: Option<BTreeSet<PaneId>>,
        next: SourceGeneration,
    ) -> Vec<OwnerEffect> {
        if self.reading.as_ref() != Some(&read) || self.phase != SourcePhase::Attached {
            return vec![];
        }
        self.reading = None;
        let mut effects = vec![];
        if let Some(membership) = membership {
            effects.push(OwnerEffect::Apply(read));
            if membership != self.membership {
                self.membership = membership;
                effects.extend(self.replace(next));
                return effects;
            }
        }
        if std::mem::take(&mut self.pending) {
            effects.extend(self.invalidate(self.generation));
        }
        effects
    }
    pub fn detached(
        &mut self,
        generation: SourceGeneration,
        failure: SourceFailure,
    ) -> Vec<OwnerEffect> {
        if generation != self.generation || self.phase == SourcePhase::Detached {
            return vec![];
        }
        self.phase = SourcePhase::Detached;
        self.reading = None;
        self.pending = false;
        vec![OwnerEffect::Status {
            generation,
            status: SourceStatus::Unavailable(Unavailable::SourceFailure(failure)),
        }]
    }
    pub fn unsubscribe(&mut self) -> Vec<OwnerEffect> {
        self.subscribers = self.subscribers.checked_sub(1).expect("known subscriber");
        if self.subscribers != 0 || self.phase == SourcePhase::Detached {
            return vec![];
        }
        self.phase = SourcePhase::Detached;
        self.reading = None;
        self.pending = false;
        vec![OwnerEffect::Cancel(self.generation)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owner() -> SessionObservationOwner {
        SessionObservationOwner::new(SourceGeneration(0))
    }
    fn read(effects: Vec<OwnerEffect>) -> ReadIdentity {
        effects
            .into_iter()
            .find_map(|e| {
                if let OwnerEffect::Read(r) = e {
                    Some(r)
                } else {
                    None
                }
            })
            .unwrap()
    }
    #[test]
    fn initialization_is_once_and_burst_is_one_followup() {
        let mut owner = owner();
        owner.subscribe(SourceGeneration(1));
        let first = read(owner.attached(SourceGeneration(1)));
        assert!(owner.attached(SourceGeneration(1)).is_empty());
        for _ in 0..20 {
            assert!(owner.invalidate(SourceGeneration(1)).is_empty());
        }
        let next =
            read(owner.read_completed(first.clone(), Some(BTreeSet::new()), SourceGeneration(2)));
        assert_ne!(first, next);
        let effects = owner.read_completed(next, Some(BTreeSet::new()), SourceGeneration(2));
        assert!(!effects.iter().any(|e| matches!(e, OwnerEffect::Read(_))));
    }
    #[test]
    fn loss_does_not_retry_and_old_completion_is_fenced() {
        let mut owner = owner();
        owner.subscribe(SourceGeneration(1));
        let old = read(owner.attached(SourceGeneration(1)));
        assert_eq!(
            owner
                .detached(SourceGeneration(1), SourceFailure::Closed)
                .len(),
            1
        );
        assert!(owner
            .read_completed(old, None, SourceGeneration(2))
            .is_empty());
        assert_eq!(owner.subscribe(SourceGeneration(2)).len(), 2);
        assert!(owner.subscribe(SourceGeneration(3)).is_empty());
        assert!(owner.attached(SourceGeneration(1)).is_empty());
    }
    #[test]
    fn membership_replaces_once_and_last_subscriber_cancels() {
        let mut owner = owner();
        owner.subscribe(SourceGeneration(1));
        let r = read(owner.attached(SourceGeneration(1)));
        let effects =
            owner.read_completed(r, Some([PaneId("p".into())].into()), SourceGeneration(2));
        assert!(effects
            .iter()
            .any(|e| matches!(e, OwnerEffect::Attach { .. })));
        let r = read(owner.attached(SourceGeneration(2)));
        let effects =
            owner.read_completed(r, Some([PaneId("p".into())].into()), SourceGeneration(3));
        assert!(!effects
            .iter()
            .any(|e| matches!(e, OwnerEffect::Attach { .. } | OwnerEffect::Read(_))));
        assert_eq!(
            owner.unsubscribe(),
            vec![OwnerEffect::Cancel(SourceGeneration(2))]
        );
    }
}
