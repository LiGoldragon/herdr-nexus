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
          cargoExtraArgs = "-p herdr-nexus-translator --features text --bin herdr-nexus --bin herdr-nexus-meta --bin herdr-nexus-place";
        };
        default = pkgs.symlinkJoin {
          name = "herdr-nexus-v1";
          paths = [ daemon translator cli ];
        };
        };
        checks = {
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
          emission-generated = pkgs.runCommand "herdr-emission-generated" {
            generator = ethos-zero.packages.${system}.default;
            declaration = ./ethos/emission.ethos;
            committed = ./crates/emission/src/generated/emission.rs;
          } (builtins.readFile ./checks/generated-contract.sh);
        };
        devShells.default = pkgs.mkShell {
          packages = [ rust.toolchain ];
        };
      });
}
