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
  };

  outputs = { self, nixpkgs, flake-utils, rust-build, ethos-zero }:
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
      in {
        packages = rec {
        daemon = rust.craneLib.buildPackage {
          pname = "herdr-nexus-daemon";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "-p herdr-nexus-daemon --bin herdr-nexus-daemon --no-default-features";
        };
        translator = rust.craneLib.buildPackage {
          pname = "herdr-translator";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "-p herdr-nexus-translator --features text --bin herdr-translator";
        };
        cli = rust.craneLib.buildPackage {
          pname = "herdr-nexus-cli";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "-p herdr-nexus-translator --features text --bin herdr-nexus --bin herdr-nexus-meta --bin herdr-nexus-place --bin herdr-pair";
        };
        default = pkgs.symlinkJoin {
          name = "herdr-nexus-v1";
          paths = [ daemon translator cli ];
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
            declaration = ./ethos/emission.ethos;
            committed = ./crates/emission/src/generated/emission.rs;
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
            declaration = ./ethos/core-meta.ethos;
            committed = ./crates/core-meta/src/lib.rs;
            normalization = "core-meta";
          } (builtins.readFile ./checks/generated-contract-normalized.sh);
          generated = pkgs.runCommand "herdr-generated-contracts" {
            contracts = [
              operation-generated memory-generated emission-generated
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
            package = self.packages.${system}.default;
            script = ./checks/delayed-start.sh;
          } ''
            ${pkgs.bash}/bin/bash "$script" "$package"
            touch "$out"
          '';
        };
        devShells.default = pkgs.mkShell {
          packages = [ rust.toolchain ];
        };
      });
}
