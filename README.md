# herdr-nexus

Fresh version 1 proof of the Nexus that owns herdr panes. The public, meta, and
Emission contracts are authored in Ethos and generated without hand-written
wire types. The pure core owns pane Memory and outcome transitions; the
translator owns Datom, JSON, and process execution.

`ethos/emission.ethos` is the documented temporary Signal-root stand-in for
Emission. Ethos-zero is unchanged.

The current `InMemory` backend is explicitly nondurable and provides no
restart recovery. Subscriptions acknowledge before delivering changes, carry
no durable event history, and write each change directly with the configured
deadline. All subscribers to one session share one complete-snapshot poll
source. Successful disappearance emits `Dead` once; refused or uncertain polls
preserve the last verified state. A disconnected reader obtains current state
with `List` before subscribing again.

Build the full check through Nix:

```sh
nix build .#checks.x86_64-linux.test --option max-jobs 0 -L
```
