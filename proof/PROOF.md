# Installed-front Herdr v1 proof

The rerunnable proof is `proof/live-herdr.sh`. It owns a unique directory,
two Herdr sessions, two workspaces, their panes, and only the processes it
starts. Its exit trap closes those resources and prints their identities.

Validated candidate:

```
/nix/store/brympvai7ri0vb8q4bnjbfy133cfjqw4-herdr-nexus-v1
```

Invocation:

```sh
PROOF_BASE=/tmp/herdr-v1-proof-af8fe3 \
  ./proof/live-herdr.sh \
  /nix/store/brympvai7ri0vb8q4bnjbfy133cfjqw4-herdr-nexus-v1
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
cleanup owns base=/tmp/herdr-v1-proof-af8fe3 sessions=v1proof-li-1491458-one v1proof-li-1491458-two pids=1491466 1491468 1491474 1491476 1491527 1491548 panes=
deleting owned fixture directory /tmp/herdr-v1-proof-af8fe3
```

The remote Nix test derivation passed core 8, daemon 8, translator 8,
integration 1, runner 5, server 2, and wire 4 tests. The pinned Ethos Zero
operation, memory, and emission regeneration checks also passed.
