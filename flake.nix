{
  description = "herdr Nexus v1 — typed core, daemon, and external translator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-build = {
      url = "github:LiGoldragon/rust-build";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-build }:
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
          cargoExtraArgs = "-p herdr-nexus-translator --bin herdr-translator --no-default-features";
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
        checks.test = rust.craneLib.cargoTest {
          pname = "herdr-nexus-test";
          version = "0.1.0";
          src = source;
          inherit cargoArtifacts;
          cargoTestExtraArgs = "--workspace --all-features";
        };
        devShells.default = pkgs.mkShell {
          packages = [ rust.toolchain ];
        };
      });
}
