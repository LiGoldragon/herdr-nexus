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
restart recovery. The `Memory` trait is the persistence boundary. Tests use a
deterministic `RefusingMemory` to prove three facts separately: refusal before
an external attempt, an observed external success followed by Memory refusal,
and uncertainty whose reconciliation marker could not be stored.

A pane is acted on only after Memory proves ownership. Closing a pane closes
that pane only. A successful complete list may mark an absent owned pane dead;
a refusal or uncertainty changes no pane to dead. Labels are stored unchanged
in Memory and joined onto public observations there; external observations do
not carry labels.

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

Runtime configuration is replaced as one validated candidate. It contains the
herdr executable, positive representable polling and timeout durations, and a
frame capacity from one byte through Signal's verified maximum. Defaults are
deployment data, not Rust literals. Each attempt captures one immutable
configuration snapshot.

A launcher selects one `Place` from its typed registry and binds the three
fixed sockets in that metaflow directory. The selected place is immutable for
the bound daemon. The CLI takes a metaflow name and the launcher-supplied
registry path; it does not discover sockets through an environment variable or
a hard-coded home directory. Placement is translator/metaflow integration and
is not implemented by the pure core.

The public socket remains unavailable until the meta surface accepts a valid
configuration. This proof does not invent an unconfigured public error.

## External state

The translator maps herdr status to public state as follows: positively
detected readiness is `Idle`; working or blocked is `Busy`; a present pane
without positive readiness is `Open`; absence becomes `Dead` only after a
successful complete snapshot. Failed, malformed, partial, or timed-out listing
is `Refused` or `Uncertain` and never a complete empty snapshot.
