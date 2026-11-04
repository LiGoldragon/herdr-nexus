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
        packages.default = rust.craneLib.buildPackage {
          src = source;
          inherit cargoArtifacts;
          cargoExtraArgs = "--workspace --all-features";
        };
        checks.test = rust.craneLib.cargoTest {
          src = source;
          inherit cargoArtifacts;
          cargoTestExtraArgs = "--workspace --all-features";
        };
        devShells.default = pkgs.mkShell {
          packages = [ rust.toolchain ];
        };
      });
}
