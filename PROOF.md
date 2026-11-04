# Version 1 proof record

On 2026-11-03 the remotely built package
`/nix/store/anr5nfr8jbkj88ayfhk03xar9v8bz14y-herdr-nexus-v1` drove the
installed `herdr 0.8.2` through the public and meta sockets. The deployment
fixture supplied all configuration values; the Start timeout was 60,000 ms.

```text
Configure.{ /home/li/.nix-profile/bin/herdr 100 30000 60000 8388608 1048576 }
Configured.Succeeded

Create.{ proofaf8fe3 /tmp/herdr-v1-work-af8fe3 [] }
Created.Created.{ proofaf8fe3 w1:p1 }

Start.{ { proofaf8fe3 w1:p1 } { v1proof Codex Configured [] } «Mind versionOne Secondary proof» }
Started.Done

Prompt.{ { proofaf8fe3 w1:p1 } «Reply only V1HERDRNONCE» }
Prompted.Done

List.proofaf8fe3
Listed.Listed.[ { { proofaf8fe3 w1:p1 }
                  «Mind versionOne Secondary proof»
                  Idle } ]

Close.{ proofaf8fe3 w1:p1 }
Closed.Done

List.proofaf8fe3
Listed.Listed.[]
```

The separate pane-read witness contained the submitted prompt followed by the
model's exact `V1HERDRNONCE` response. Cleanup then stopped and deleted only
the owned `proofaf8fe3` herdr session, stopped the two owned Nexus processes,
and deleted the two uniquely owned temporary directories. Lock 25033 was
released with a typed `Released` reply.

The remote check derivation ran on
`ssh-ng://nix-ssh@prometheus.goldragon.criome`. The valid installed
agent-start result repair is commit
`8a83f1f7fca20db74b5c17c9273bfd304db7abeb`.

An earlier published commit accidentally included local build artifacts:
`2617a3cd682f62085427cf026c5abe97cece4441`. Commit
`aad65723f69dbd210937807745f0a6cd768a8539` removed them from the latest tree;
history was preserved without rewriting the public branch.

The subscription implementation was then rebuilt as
`/nix/store/35km43qgxsi07jln7wdyj8kr5f9yfl7c-herdr-nexus-v1` and exercised
against a second owned installed-herdr session. The long-lived CLI delivered
the acknowledgement before the state changes:

```text
Subscribe.subaf8fe3
Subscribed.Done
Changed.{ { subaf8fe3 w1:p1 }
          Busy }
Changed.{ { subaf8fe3 w1:p1 }
          Idle }
```

The changes followed a real Prompt through the public Nexus. Public Close
returned `Closed.Done`, the final List was empty, and cleanup stopped and
deleted the owned session, processes, and temporary directories. Lock 25037
was released with a typed `Released` reply.
