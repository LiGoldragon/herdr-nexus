# Installed-front Herdr v1 proof

## Fixed Place first-launch proof — 2026-11-04

Remote Nix built `/nix/store/slq0b02i6wrqlm7xgyidrriv4vkfn9bk-herdr-nexus-v1`.
The public launcher accepted exactly one inline Place:

```sh
herdr-pair '{ one /run/user/1000/metaflow/one /home/one/.local/state/metaflow/one }'
```

The installed public and meta clients used `herdr-nexus one '<Datom>'` and
`herdr-nexus-meta one '<Datom>'`; they resolved the same runtime from
`XDG_RUNTIME_DIR` and `HOME` without a registry file. The proof launched two
Places, reached both readiness gates, created, started, listed, subscribed,
prompted, pressed, closed, and cleaned both owned runtimes and state fixtures.
Its final witness was `LIVE_HERDR_PROOF_OK`.

Remote `test`, aggregate generated-contract, `delayed-start`,
`paired-relaunch`, and `meta-connection-isolation` checks passed. The last
check retained the R1 proof: a direct unauthorized core-meta connect/close and
malformed and partial-timeout meta-front frames were isolated before later
valid meta and ordinary calls on the same pair.

## Paired front proof — 2026-11-04

Remote Nix built `/nix/store/lxdbk269775akwn0cbdkdh9lfyrjhfyy-herdr-nexus-v1`.
`proof/live-herdr.sh` launched two isolated metaflow directories through
`herdr-pair`. The launcher obtained both child process IDs and supplied each
child the other child's typed peer identity before closing both bootstrap
pipes. Each owner accepted its individual settings and reached `Ready`.

Both metaflows created the overlapping external pane ID `w1:p1` without
crossing directories. The proof then exercised Start, concurrent List,
Subscribe, a live Polling replacement, Prompt, Press, harness exit while the
pane remained present, Close, and cleanup of the owned panes, sessions,
paired processes, sockets, and fixture directory. Its final witness was:

```text
Started.Done
Prompted.Done
Pressed.Done
Pressed.Done
Closed.Done
Closed.Done
LIVE_HERDR_PROOF_OK
```

The remote suite additionally rejects an unpaired same-login emission before
payload decoding and rejects a direct core connection whose kernel process ID
is not the paired translator.
The `paired-relaunch` Nix check terminates one child, witnesses the launcher reap
the survivor, launches a fresh pair with distinct kernel process identities, and
forwards a valid Nexus-owned Set through that fresh pair.

The rerunnable proof is `proof/live-herdr.sh`. It owns a unique directory,
two Herdr sessions, two workspaces, their panes, and only the processes it
starts. Its exit trap closes those resources and prints their identities.

Current remotely checked candidate (the change after the live run adds only the controlled stale-poll regression and this record):

```
/nix/store/hzvd10rigadqxllcm7zab1yckyx6bc9z-herdr-nexus-v1
```

Rerun invocation:

```sh
PROOF_BASE=/tmp/herdr-v1-proof-af8fe3 \
  ./proof/live-herdr.sh \
  /nix/store/hzvd10rigadqxllcm7zab1yckyx6bc9z-herdr-nexus-v1
```

The proof observed both owners reach `Ready`, rejected a malformed meta
request before valid settings, created two isolated metaflows whose Herdr
pane IDs both began at `w1:p1`, and ran their Lists concurrently. It started
Codex through the public front, subscribed, changed polling while the stream
was active, prompted `V1HERDRNONCE`, and exercised Press. After Codex became
idle, Press typed `/quit` and submitted Enter separately; the subscription
reported the harness dead while `herdr pane get` still proved the pane was
present. Both panes then returned `Closed.Done`.

Terminal success witness:

```
Started.Done
Prompted.Done
Pressed.Done
Pressed.Done
Closed.Done
Closed.Done
LIVE_HERDR_PROOF_OK /tmp/herdr-v1-proof-af8fe3
cleanup owns base=/tmp/herdr-v1-proof-af8fe3 sessions=v1proof-li-1495601-one v1proof-li-1495601-two pids=1495609 1495611 1495617 1495619 1495679 1495701 panes=
deleting owned fixture directory /tmp/herdr-v1-proof-af8fe3
```

The current remote Nix test derivation passed, including the controlled stale List/Close race: terminal Dead was delivered exactly once and the released pre-Close List produced no later Open. The aggregate pinned Ethos Zero regeneration check also passed.
