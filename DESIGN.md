# Herdr Nexus version 1 proof

## Boundaries

The Nexus core receives and returns generated binary Signal values. It has no
Datom, JSON, command-line, or process dependency. The translator owns the
external herdr JSON and command line. `ethos/emission.ethos` is temporarily a
Signal root because ethos-zero does not yet check an Emission root.

Ethos-zero currently emits the Rust root names `Query` and `Response` for a
Signal. The vision calls these roots Input and Output. This implementation
keeps the generated names rather than hand-editing generated code; the naming
difference is an audited generator limitation.

## Memory

`InMemory` is a process-lifetime proof backend. It is nondurable and offers no
restart recovery. `ethos/memory.ethos` declares the owned-pane and
reconciliation records and the standard `Answer` returned by every change;
the pinned generator output is checked in every Nix test run. Reads return
typed snapshots separately. The `Memory` trait is the persistence boundary. Tests use a
deterministic `RefusingMemory` to prove three facts separately: refusal before
an external attempt, an observed external success followed by Memory refusal,
and uncertainty whose reconciliation marker could not be stored.

A pane is acted on only after Memory proves ownership. Closing a pane closes
that pane only. A successful complete list may mark an absent owned pane dead;
a refusal or uncertainty changes no pane to dead. Labels are stored unchanged
in Memory and joined onto public observations there; external observations do
not carry labels.

Create is reconciled before it has a pane identity. The launcher gives each
core process a fresh random `InstanceId`; the core pairs it with a monotonic
`Sequence` and stores the resulting `PendingCreation` before emitting. The
translator writes that identifier into the one managed workspace label. A
lost Create result leaves the single pending record actionable. A later
complete workspace snapshot and the same observation cycle's complete pane
snapshot adopt only an exact one-workspace, one-pane join, then consume the
pending record. Ambiguous, malformed, absent, and unrelated observations
leave it pending. This proof Memory remains nondurable, so restart recovery is
outside the proof's guarantee.

## Outcomes repaired from the draft

The vision draft used one broad `Outcome` and did not express the point where
an external effect succeeded but its Memory write failed. The checked contract
keeps one shared `Outcome` and adds `StoreRefused` with a phase:

- `BeforeEmission` — Memory refused before an external attempt.
- `Observed` — the exact successful effect is carried as evidence.
- `Uncertain` — the external effect may have happened and its reconciliation
  marker could not be stored.

Symbolic herdr error codes remain strings. An optional process exit status is
separate. Internal failures never receive invented exit codes.

## Configuration and placement

The Nexus and translator each own a partial configuration. One flat typed
`Set` validates and applies one field; refusal preserves the previous partial
snapshot. The Nexus owns start, polling, frame-capacity, transport and
subscriber-queue limits.
The translator owns the herdr path, call timeout, output limit and text budget.
Each process reports the fields it still awaits and becomes ready only when
all of its fields are present and their cross-field constraints hold. Defaults
are deployment data, not Rust literals. Each attempt captures the fields it
uses without holding Memory across external I/O.

A launcher selects one `Place` from its typed registry and supplies exactly one
typed binary bootstrap carrying the place and admission limits. Admission
includes a positive deployment-chosen accept retry delay: recoverable resource
pressure waits by that delay, while an invalid listener ends the owning
process visibly. Each metaflow
has separate public and meta translator sockets and separate public and meta
Nexus sockets. The selected place is immutable for each bound process. The CLI
takes a metaflow name and the launcher-supplied
registry path; it does not discover sockets through an environment variable or
a hard-coded home directory.

The meta surfaces are admitted under bootstrap limits before runtime settings
exist. Public service becomes ready only after the owning process has a
complete valid configuration. The public text and emission lanes share the
public translator socket; meta text has its own translator socket. Every lane
checks the peer UID at the same-login boundary.

## External calls

Each external command bears the checked `Arguable` trait on its own concrete
argument type. The one typed execution path composes that capability with the
one session prefix and decodes through the same concrete command type; there
is no parallel invocation or decoder selection table.
Successful Herdr JSON is decoded only from stdout. A nonzero process result is
decoded only from Herdr's documented stderr error envelope, with no fallback
between the channels. The bounded runner drains both pipes while retaining at
most the configured joint output limit. `Subscribe` is routed once into its
dedicated stream; it has no parallel one-shot fallback.

## External state

The translator maps herdr status to public state as follows: positively
detected readiness is `Idle`; working or blocked is `Busy`; a present pane
without positive readiness is `Open`; absence becomes `Dead` only after a
successful complete snapshot. Failed, malformed, partial, or timed-out listing
is `Refused` or `Uncertain` and never a complete empty snapshot.
