{
  description = "cdc-rs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    kubenix = {
      url = "github:hall/kubenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
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

  outputs = inputs @ { self , nixpkgs , flake-parts , crate2nix , nix2container , kubenix, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      perSystem = { system, pkgs, lib, inputs', ... }:
        let
          nix2c = nix2container.packages.${system}.nix2container;

          cdc-rs = inputs.crate2nix.tools.${system}.appliedCargoNix {
            name = "cdc-rs";
            src = ./.;
          };
        in
        rec {
          checks = {
            # rustnix = cdc-rs.rootCrate.build.override {
            #   runTests = true;
            # };
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
                k3s
                kubectl
              ];
            };
          };

          packages = {
            default = packages.cdc-rs;

            cdc-rs = cdc-rs.rootCrate.build;

            # cdc-rs-bis = pkgs.rustPlatform.buildRustPackage {
            #   pname = "cdc-rs";
            #   version = "0.1.0";
            #   src = ./.;
            #   cargoLock.lockFile = ./Cargo.lock;

            #   nativeBuildInputs = with pkgs; [
            #     openssl.dev
            #     pkg-config
            #   ];
            # };

            oci-image = nix2c.buildImage {
              name = "docteurklein/cdc-rs";
              maxLayers = 125;
              config = {
                Entrypoint = [ "${packages.cdc-rs}/bin/cdc-rs" ];
              };
              copyToRoot = pkgs.buildEnv {
                name = "utils";
                paths = with pkgs; [
                  bashInteractive
                  coreutils
                  sqlite
                ];
                pathsToLink = [ "/bin" ];
              };
            };

            kube = (kubenix.evalModules.${system} {
              module = { kubenix, config, ... }: {
                imports = [
                  kubenix.modules.k8s
                  kubenix.modules.docker
                ];

                docker = {
                  # registry.url = "docker.io";
                  images.cdc-rs.image = self.packages.${system}.oci-image;
                };

                kubernetes.resources.deployments."cdc-rs".spec = {
                  selector.matchLabels.app = "cdc-rs";
                  template.metadata.labels.app = "cdc-rs";
                  template.spec = {
                    containers."cdc-rs" = {
                      # image = config.docker.images."cdc-rs".path;
                      image = "docteurklein/cdc-rs:latest";
                      imagePullPolicy = "IfNotPresent";
                      volumeMounts = {
                        "/script/cdc-rs".name = "cdc-rs-script";
                        "/state/cdc-rs".name = "cdc-rs-state";
                      };
                      args = [
                        "--state" "/state/cdc-rs/state.sqlite"
                        "--script" "/script/cdc-rs/script.rhai"
                      ];
                      env = [
                        { name = "SOURCE"; value = "mysql://replicator:a9e21296-4d24-48c1-89db-51536cf7b583@172.28.0.217:3306"; }
                        { name = "REGEX"; value = "test1.*"; }
                        { name = "SERVER_ID"; value = "123456"; }
                      ];
                    };
                    volumes = [
                      { name = "cdc-rs-script"; configMap.name = "cdc-rs-script"; }
                      { name = "cdc-rs-state"; persistentVolumeClaim.claimName = "cdc-rs-state"; }
                    ];
                  };
                };

                kubernetes.resources.configMaps."cdc-rs-script".data."script.rhai" = builtins.readFile ./test.rhai;

                kubernetes.resources.persistentVolumeClaims."cdc-rs-state" = {
                  metadata.name = "cdc-rs-state";
                  spec = {
                    accessModes = [
                      "ReadWriteOnce"
                    ];
                    resources.requests.storage = "100Mi";
                  };
                };
              };
            }).config.kubernetes.result;
          };
      };
    };
}
