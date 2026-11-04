# herdr-nexus

Fresh version 1 proof of the Nexus that owns herdr panes. The public, meta, and
Emission contracts are authored in Ethos and generated without hand-written
wire types. The pure core owns pane Memory and outcome transitions; the
translator owns Datom, JSON, and process execution.

`ethos/emission.ethos` is the documented temporary Signal-root stand-in for
Emission. Ethos-zero is unchanged.

The current `InMemory` backend is explicitly nondurable. The daemon entry is a
scaffold until typed socket serving and configuration lifecycle are connected.

Run the proved core and contract tests inside the repository's Nix toolchain:

```sh
nix develop /git/github.com/LiGoldragon/datom-codec --command \
  cargo test --manifest-path crates/core/Cargo.toml
nix develop /git/github.com/LiGoldragon/datom-codec --command \
  cargo test --manifest-path crates/emission/Cargo.toml --features datom
```
