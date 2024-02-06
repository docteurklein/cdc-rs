{
  description = "cdc-rs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    nix2container = {
      url = "github:nlewo/nix2container";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crate2nix.url = "github:nix-community/crate2nix";
  };

  nixConfig = {
    extra-trusted-public-keys = "eigenvalue.cachix.org-1:ykerQDDa55PGxU25CETy9wF6uVDpadGGXYrFNJA3TUs=";
    extra-substituters = "https://eigenvalue.cachix.org";
    allow-import-from-derivation = true;
  };

  outputs = inputs @ { self , nixpkgs , flake-parts , crate2nix , nix2container , ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      imports = [
      ];

      perSystem = { system, pkgs, lib, inputs', ... }:
        let
          nix2c = nix2container.packages.${system}.nix2container;
          cargoNix = inputs.crate2nix.tools.${system}.appliedCargoNix {
            name = "cdc-rs";
            src = ./.;
          };
        in
        rec {
          checks = {
            rustnix = cargoNix.rootCrate.build.override {
              runTests = true;
            };
          };

          devShells = {
            default = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [
                cargo
                rustc
                rustfmt
                clippy
                openssl.dev
                pkg-config
                rust-analyzer-unwrapped
              ];
            };
          };

          packages = {
            default = packages.cdc-rs;

            cdc-rs = (inputs.crate2nix.tools.${system}.appliedCargoNix {
              name = "cdc-rs";
              src = ./.;
            }).rootCrate.build;

            cdc-rs-bis = pkgs.rustPlatform.buildRustPackage {
              pname = "cdc-rs";
              version = "0.1.0";
              src = ./.;
              cargoLock.lockFile = ./Cargo.lock;

              nativeBuildInputs = with pkgs; [
                openssl.dev
                pkg-config
              ];
            };

            docker = nix2c.buildImage {
              name = "docteurklein/cdc-rs";
              maxLayers = 125;
              # resolvedByNix = true;
              config = {
                Entrypoint = [ "${packages.cdc-rs}/bin/cdc-rs" ];
              };
            };
          };
      };
    };
}
