{
  description = "herdr Nexus v1 — typed core, daemon, and external translator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-build = {
      url = "github:LiGoldragon/rust-build";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    ethos-zero.url = "github:LiGoldragon/ethos-zero/b8052c2404374265dfcddbde83fef76977f78acd";
    herdr-source = {
      url = "path:/nix/store/3jhxk12n1hhjw4a8y4a6fwiqaf37ydp6-source";
      flake = false;
    };
    herdr-patch = {
      url = "path:/nix/store/kwavh83rncsz0sjqxd7pj6z68f9mi9gx-codex-executable-selection.patch";
      flake = false;
    };
    herdr-zig-cache = {
      url = "path:/nix/store/q99mxjl4knvj7jkahjfnxbzi1cxzh42v-herdr-libghostty-vt-zig-cache";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-build, ethos-zero, herdr-source, herdr-patch, herdr-zig-cache }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        rust = rust-build.lib.${system}.fromPkgs pkgs;
        source = rust.cleanSource {
          root = ./.;
          extraFilters = [
            (path: type: type == "regular" && pkgs.lib.hasSuffix ".ethos" (toString path))
          ];
        };
        cargoArtifacts = rust.craneLib.buildDepsOnly {
          src = source;
          cargoExtraArgs = "--workspace --all-features";
        };
        herdrCargoArtifacts = rust.craneLib.buildDepsOnly {
          pname = "herdr-dependencies";
          version = "0.8.2";
          src = herdr-source;
          nativeBuildInputs = [ pkgs.zig_0_15 ];
          LIBGHOSTTY_VT_OPTIMIZE = "ReleaseFast";
          LIBGHOSTTY_VT_SIMD = "true";
          LIBGHOSTTY_VT_ZIG_SYSTEM_DIR = herdr-zig-cache;
          preBuild = ''
            export ZIG_GLOBAL_CACHE_DIR="$TMPDIR/zig-global-cache"
            mkdir -p "$ZIG_GLOBAL_CACHE_DIR"
          '';
        };
        exactBackendPackage = rust.craneLib.buildPackage {
          pname = "herdr";
          version = "0.8.2";
          src = herdr-source;
          patches = [ herdr-patch ];
          cargoArtifacts = herdrCargoArtifacts;
          doCheck = false;
          nativeBuildInputs = [ pkgs.zig_0_15 ];
          LIBGHOSTTY_VT_OPTIMIZE = "ReleaseFast";
          LIBGHOSTTY_VT_SIMD = "true";
          LIBGHOSTTY_VT_ZIG_SYSTEM_DIR = herdr-zig-cache;
          preBuild = ''
            export ZIG_GLOBAL_CACHE_DIR="$TMPDIR/zig-global-cache"
            mkdir -p "$ZIG_GLOBAL_CACHE_DIR"
          '';
          meta.mainProgram = "herdr";
        };
        backendDeploymentModule = { lib, ... }: {
          options = {
            package = lib.mkOption { type = lib.types.package; };
            configurationRoot = lib.mkOption { type = lib.types.listOf lib.types.str; };
            stateRoot = lib.mkOption { type = lib.types.listOf lib.types.str; };
            claimName = lib.mkOption { type = lib.types.str; };
            readinessTimeout = lib.mkOption { type = lib.types.ints.positive; };
            probeTimeout = lib.mkOption { type = lib.types.ints.positive; };
            stopTimeout = lib.mkOption { type = lib.types.ints.positive; };
            pollInterval = lib.mkOption { type = lib.types.ints.positive; };
            outputLimit = lib.mkOption { type = lib.types.ints.positive; };
          };
        };
        backendDefaults = {
          package = exactBackendPackage;
          configurationRoot = [ "herdr-backend" "config" ];
          stateRoot = [ "herdr-backend" "state" ];
          claimName = "herdr-backend.owner";
          readinessTimeout = 30000;
          probeTimeout = 1000;
          stopTimeout = 5000;
          pollInterval = 25;
          outputLimit = 1048576;
        };
        normalComponent = value:
          value != "." && value != ".." && builtins.match "[A-Za-z0-9._-]+" value != null;
        daemonPackage = rust.craneLib.buildPackage {
          pname = "herdr-nexus-daemon";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "-p herdr-nexus-daemon --bin herdr-nexus-daemon --no-default-features";
        };
        translatorPackage = rust.craneLib.buildPackage {
          pname = "herdr-translator";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "-p herdr-nexus-translator --features text --bin herdr-translator";
        };
        cliPackage = rust.craneLib.buildPackage {
          pname = "herdr-nexus-cli";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "-p herdr-nexus-translator --features text --bin herdr-nexus --bin herdr-nexus-meta --bin herdr-nexus-place --bin herdr-pair";
          postInstall = ''
            mkdir -p "$out/libexec"
            mv "$out/bin/herdr-pair" "$out/libexec/herdr-pair-launcher"
          '';
        };
        fakeBackend = pkgs.stdenv.mkDerivation {
          pname = "herdr-native-fake-backend";
          version = "0.1.0";
          dontUnpack = true;
          buildPhase = ''
            $CC -std=c11 -Wall -Wextra -Werror \
              ${./tests/nativefixture/fake-backend.c} -o fake-backend
          '';
          installPhase = ''
            mkdir -p "$out/bin"
            install -m755 fake-backend "$out/bin/fake-backend"
          '';
          meta.mainProgram = "fake-backend";
        };
        mkPair = overrides:
          let
            deployment = (pkgs.lib.evalModules {
              modules = [ backendDeploymentModule { config = backendDefaults // overrides; } ];
            }).config;
            valid = deployment.configurationRoot != []
              && deployment.stateRoot != []
              && builtins.all normalComponent deployment.configurationRoot
              && builtins.all normalComponent deployment.stateRoot
              && normalComponent deployment.claimName;
            vector = values: "[ ${builtins.concatStringsSep " " values} ]";
            deploymentText = "{ ${pkgs.lib.getExe deployment.package} ${vector deployment.configurationRoot} ${vector deployment.stateRoot} ${deployment.claimName} ${toString deployment.readinessTimeout} ${toString deployment.probeTimeout} ${toString deployment.stopTimeout} ${toString deployment.pollInterval} ${toString deployment.outputLimit} }";
          in assert valid;
          pkgs.writeShellScriptBin "herdr-pair" ''
            if [[ $# -ne 1 ]]; then
              echo "usage: herdr-pair '<Place>'" >&2
              exit 64
            fi
            exec ${cliPackage}/libexec/herdr-pair-launcher "$1" \
              '${deploymentText}' \
              ${daemonPackage}/bin/herdr-nexus-daemon \
              ${translatorPackage}/bin/herdr-translator \
              70000 8388608 25 1048576 64
          '';
        delayedPairPackage = pkgs.symlinkJoin {
          name = "herdr-nexus-delayed-start-fixture";
          paths = [ daemonPackage translatorPackage cliPackage (mkPair { package = fakeBackend; }) ];
        };
      in {
        lib = { inherit mkPair; };
        packages = rec {
        daemon = daemonPackage;
        translator = translatorPackage;
        cli = cliPackage;
        pair = mkPair {};
        default = pkgs.symlinkJoin {
          name = "herdr-nexus-v1";
          paths = [ daemon translator cli pair ];
        };
        };
        checks = rec {
          test = rust.craneLib.cargoTest {
            pname = "herdr-nexus-test";
            version = "0.1.0";
            src = source;
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--workspace --all-features";
          };
          operation-generated = pkgs.runCommand "herdr-operation-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/operation.ethos;
            committed = ./crates/core/src/generated/operation.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          memory-generated = pkgs.runCommand "herdr-memory-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/memory.ethos;
            committed = ./crates/core/src/generated/memory.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          emission-generated = pkgs.runCommand "herdr-emission-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./crates/emission/ethos/emission.ethos;
            committed = ./crates/emission/src/generated/emission.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          argument-generated = pkgs.runCommand "herdr-argument-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/argument.ethos;
            committed = ./crates/translator/src/generated/argument.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          backend-deployment-generated = pkgs.runCommand "herdr-backend-deployment-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/backend-deployment.ethos;
            committed = ./crates/translator/src/generated/backend_deployment.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          transport-generated = pkgs.runCommand "herdr-transport-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/transport.ethos;
            committed = ./crates/wire/src/transport_types.rs;
            normalization = "transport";
          } (builtins.readFile ./checks/generated-contract-normalized.sh);
          public-call-generated = pkgs.runCommand "herdr-public-call-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/public-call.ethos;
            committed = ./crates/wire/src/public_call.rs;
            normalization = "public-call";
          } (builtins.readFile ./checks/generated-contract-normalized.sh);
          public-payload-generated = pkgs.runCommand "herdr-public-payload-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/public-payload.ethos;
            committed = ./crates/wire/src/public_payload.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          meta-call-generated = pkgs.runCommand "herdr-meta-call-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/meta-call.ethos;
            committed = ./crates/wire/src/meta_call.rs;
            normalization = "meta-call";
          } (builtins.readFile ./checks/generated-contract-normalized.sh);
          meta-payload-generated = pkgs.runCommand "herdr-meta-payload-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/meta-payload.ethos;
            committed = ./crates/wire/src/meta_payload.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
          core-meta-generated = pkgs.runCommand "herdr-core-meta-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./crates/core-meta/ethos/core-meta.ethos;
            committed = ./crates/core-meta/src/lib.rs;
            normalization = "core-meta";
          } (builtins.readFile ./checks/generated-contract-normalized.sh);
          generated = pkgs.runCommand "herdr-generated-contracts" {
            contracts = [
              operation-generated memory-generated emission-generated argument-generated
              backend-deployment-generated
              transport-generated public-call-generated public-payload-generated
              meta-call-generated meta-payload-generated core-meta-generated
            ];
          } ''
            for contract in $contracts; do test -e "$contract"; done
            touch "$out"
          '';
          paired-relaunch = pkgs.runCommand "herdr-paired-relaunch" {
            package = self.packages.${system}.default;
            script = ./checks/paired-relaunch.sh;
          } ''
            ${pkgs.bash}/bin/bash "$script" "$package"
            touch "$out"
          '';
          delayed-start = pkgs.runCommand "herdr-delayed-start" {
            package = delayedPairPackage;
            backend = "${fakeBackend}/bin/fake-backend";
            script = ./checks/delayed-start.sh;
          } ''
            ${pkgs.bash}/bin/bash "$script" "$package" "$backend"
            touch "$out"
          '';
          meta-connection-isolation = pkgs.runCommand "herdr-meta-connection-isolation" {
            package = self.packages.${system}.default;
            backend = pkgs.lib.getExe exactBackendPackage;
            script = ./checks/meta-connection-isolation.sh;
            nativeBuildInputs = [ pkgs.python3 ];
          } ''
            ${pkgs.bash}/bin/bash "$script" "$package" "$backend"
            touch "$out"
          '';
          backend-pair-independent = pkgs.runCommand "herdr-backend-pair-independent" {
            package = self.packages.${system}.default;
            fake = "${fakeBackend}/bin/fake-backend";
            script = ./checks/backend_pair_independent.sh;
            nativeBuildInputs = [ pkgs.bash pkgs.binutils pkgs.coreutils pkgs.gnugrep pkgs.procps ];
          } ''
            ${pkgs.bash}/bin/bash "$script" "$package" "$fake"
            touch "$out"
          '';
          backend-runtime = pkgs.runCommand "herdr-backend-runtime" {
            backend = exactBackendPackage;
            script = ./checks/backend-runtime.sh;
          } ''
            ${pkgs.bash}/bin/bash "$script" "$backend/bin/herdr"
            touch "$out"
          '';
        };
        devShells.default = pkgs.mkShell {
          packages = [ rust.toolchain ];
        };
      });
}
