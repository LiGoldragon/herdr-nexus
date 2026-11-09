{
  description = "herdr Nexus v1 — typed core, daemon, and external translator";

  inputs = {
    base.url = "github:LiGoldragon/build-base";
    # The generator that regenerates each committed contract for its check.
    ethos-zero.url = "github:LiGoldragon/ethos-zero";
  };

  # Every crate, third-party or member, is one buildRustCrate derivation.
  # Cargo.nix is generated from Cargo.lock by the build base's crate2nix and
  # committed, so evaluation imports no derivation. After editing a
  # Cargo.toml or Cargo.lock:
  #   nix run github:LiGoldragon/build-base#crate2nix -- generate
  outputs =
    { self, base, ethos-zero }:
    let
      lib = base.inputs.nixpkgs.lib;
      cargoNix = ./Cargo.nix;
      workspaces = lib.genAttrs base.lib.systems (
        system:
        let
          context = base.lib.contexts.${system};
          pkgs = context.pkgs;
          generated = import cargoNix {
            inherit pkgs;
            buildRustCrateForPkgs = context.buildRustCrateForPkgs;
            defaultCrateOverrides = context.crateOverrides;
          };
          # A member built with the features its binaries are written for.
          memberWith =
            name: features:
            (generated.internal.builtRustCratesWithFeatures {
              packageId = generated.workspaceMembers.${name}.packageId;
              inherit features;
              runTests = false;
              buildRustCrateForPkgsFunc = context.buildRustCrateForPkgs;
            }).crates.${generated.workspaceMembers.${name}.packageId}.override
              { crateOverrides = context.crateOverrides; };
          daemon = memberWith "herdr-nexus-daemon" [ ];
          built = memberWith "herdr-nexus-translator" [ "default" "text" ];
          translator = pkgs.runCommand "herdr-translator" { } ''
            mkdir -p "$out/bin"
            cp ${built}/bin/herdr-translator "$out/bin/herdr-translator"
          '';
          cli = pkgs.runCommand "herdr-nexus-cli" { } ''
            mkdir -p "$out/bin" "$out/libexec"
            for executable in herdr-nexus herdr-nexus-meta herdr-nexus-place; do
              cp ${built}/bin/$executable "$out/bin/$executable"
            done
            cp ${built}/bin/herdr-pair "$out/libexec/herdr-pair-launcher"
          '';
          pair = pkgs.writeShellScriptBin "herdr-pair" ''
            if [[ $# -ne 1 ]]; then
              echo "usage: herdr-pair '<Place>'" >&2
              exit 64
            fi
            exec ${cli}/libexec/herdr-pair-launcher "$1" \
              ${daemon}/bin/herdr-nexus-daemon \
              ${translator}/bin/herdr-translator \
              70000 8388608 25 1048576 64
          '';
          default = pkgs.symlinkJoin {
            name = "herdr-nexus-v1";
            paths = [ daemon translator cli pair ];
          };
        in
        {
          packages = { inherit daemon translator cli pair default; };
          inherit generated;
        }
      );
      workspace = base.lib.workspaceOutputs {
        inherit cargoNix;
        # The tests are written for every feature on, as `cargo test
        # --workspace --all-features` ran them.
        memberChecks =
          { pkgs, workspace, buildRustCrateForPkgs, crateOverrides, ... }:
          lib.mapAttrs (
            name: member:
            let
              packageId = member.packageId;
              features = lib.attrNames (workspace.internal.crates.${packageId}.features or { });
              tests =
                (workspace.internal.builtRustCratesWithFeatures {
                  inherit packageId features;
                  runTests = true;
                  buildRustCrateForPkgsFunc =
                    p: (buildRustCrateForPkgs p).override { defaultCrateOverrides = crateOverrides; };
                }).crates.${packageId}.override
                  {
                    buildTests = true;
                    crateOverrides = crateOverrides // {
                      ${name} =
                        attributes:
                        let
                          given = (crateOverrides.${name} or (_: { })) attributes;
                        in
                        given
                        // {
                          preBuild = (given.preBuild or "") + ''
                            export CARGO_TARGET_TMPDIR="$PWD/target/tmp"
                          '';
                          postInstall = (given.postInstall or "") + ''
                            echo "$CARGO_TARGET_TMPDIR" > $out/target-tmpdir
                          '';
                        };
                    };
                  };
            in
            pkgs.runCommand "check-${name}" { passthru = { inherit tests; }; } ''
              cp -r ${tests.src} "$NIX_BUILD_TOP/source"
              chmod -R u+w "$NIX_BUILD_TOP/source"
              cd "$NIX_BUILD_TOP/source"
              mkdir -p "$(cat ${tests}/target-tmpdir)"
              for binary in ${tests}/tests/*; do
                echo "running $(basename "$binary")"
                "$binary"
              done
              touch $out
            ''
          ) workspace.workspaceMembers;
        extraChecks =
          { pkgs, ... }:
          let
            package = (workspaces.${pkgs.stdenv.hostPlatform.system}).packages.default;
            generator = ethos-zero.packages.${pkgs.stdenv.hostPlatform.system}.default;
            contract =
              { name, declaration, committed, normalization ? null }:
              pkgs.runCommand "herdr-${name}-generated"
                (
                  {
                    inherit generator declaration committed;
                  }
                  // lib.optionalAttrs (normalization != null) { inherit normalization; }
                )
                (builtins.readFile (
                  if normalization == null then ./checks/generated-contract.sh
                  else ./checks/generated-contract-normalized.sh
                ));
            contracts = {
              operation-generated = { name = "operation"; declaration = ./ethos/operation.ethos; committed = ./crates/core/src/generated/operation.rs; };
              memory-generated = { name = "memory"; declaration = ./ethos/memory.ethos; committed = ./crates/core/src/generated/memory.rs; };
              emission-generated = { name = "emission"; declaration = ./crates/emission/ethos/emission.ethos; committed = ./crates/emission/src/generated/emission.rs; };
              argument-generated = { name = "argument"; declaration = ./ethos/argument.ethos; committed = ./crates/translator/src/generated/argument.rs; };
              transport-generated = { name = "transport"; declaration = ./ethos/transport.ethos; committed = ./crates/wire/src/transport_types.rs; normalization = "transport"; };
              public-call-generated = { name = "public-call"; declaration = ./ethos/public-call.ethos; committed = ./crates/wire/src/public_call.rs; normalization = "public-call"; };
              public-payload-generated = { name = "public-payload"; declaration = ./ethos/public-payload.ethos; committed = ./crates/wire/src/public_payload.rs; };
              meta-call-generated = { name = "meta-call"; declaration = ./ethos/meta-call.ethos; committed = ./crates/wire/src/meta_call.rs; normalization = "meta-call"; };
              meta-payload-generated = { name = "meta-payload"; declaration = ./ethos/meta-payload.ethos; committed = ./crates/wire/src/meta_payload.rs; };
              core-meta-generated = { name = "core-meta"; declaration = ./crates/core-meta/ethos/core-meta.ethos; committed = ./crates/core-meta/src/lib.rs; normalization = "core-meta"; };
            };
            checked = lib.mapAttrs (_: contract) contracts;
            script =
              name: extra:
              pkgs.runCommand "herdr-${name}"
                ({
                  inherit package;
                  script = ./checks/${name}.sh;
                } // extra)
                ''
                  ${pkgs.bash}/bin/bash "$script" "$package"
                  touch "$out"
                '';
          in
          checked
          // {
            generated = pkgs.runCommand "herdr-generated-contracts" {
              contracts = lib.attrValues checked;
            } ''
              for contract in $contracts; do test -e "$contract"; done
              touch "$out"
            '';
            paired-relaunch = script "paired-relaunch" { };
            place-preparation = script "place-preparation" { };
            delayed-start = script "delayed-start" { };
            meta-connection-isolation = script "meta-connection-isolation" { nativeBuildInputs = [ pkgs.python3 ]; };
          };
      };
    in
    {
      packages = lib.mapAttrs (system: built: workspace.packages.${system} // built.packages) workspaces;
      inherit (workspace) checks devShells;
    };
}
