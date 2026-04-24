{
  description = "Rust development environment";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    git-hooks.url = "github:cachix/git-hooks.nix";
    git-hooks.inputs.nixpkgs.follows = "nixpkgs";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    fenix = {
      url = "https://flakehub.com/f/nix-community/fenix/0.1";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ self, ... }:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.git-hooks.flakeModule
        inputs.treefmt-nix.flakeModule
      ];
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      perSystem =
        {
          config,
          lib,
          pkgs,
          system,
          ...
        }:
        let
          pname = "graylog-cli";
          version = "0.0.3-alpha";
          commonArgs = {
            inherit pname version;
            src = self;
            cargoLock.lockFile = ./Cargo.lock;
          };
          nativePackage = pkgs.rustPlatform.buildRustPackage (
            commonArgs
            // {
              buildInputs = lib.optionals pkgs.stdenv.isDarwin [
                pkgs.libiconv
              ];
            }
          );
          windowsTarget = "x86_64-pc-windows-gnu";
          windowsPkgs = pkgs.pkgsCross.mingwW64;
          windowsToolchain =
            with inputs.fenix.packages.${system};
            combine [
              stable.cargo
              stable.rustc
              targets.${windowsTarget}.stable.rust-std
            ];
          windowsRustPlatform = windowsPkgs.makeRustPlatform {
            cargo = windowsToolchain;
            rustc = windowsToolchain;
          };
          windowsPackage = windowsRustPlatform.buildRustPackage (
            commonArgs
            // {
              cargoBuildTarget = windowsTarget;
              depsBuildBuild = lib.optionals pkgs.stdenv.isDarwin [
                pkgs.libiconv
              ];
              NIX_LDFLAGS = lib.optionalString pkgs.stdenv.isDarwin "-L${pkgs.libiconv}/lib";
              stdenv = windowsPkgs.stdenv;
            }
          );
        in
        {
          packages = {
            default = nativePackage;
            graylog-cli-windows = windowsPackage;
          };
          treefmt = {
            programs.nixfmt.enable = true;
            programs.nixfmt.package = pkgs.nixfmt;
            programs.rustfmt.enable = true;
          };
          pre-commit.settings.hooks = {
            treefmt.enable = true;
          };
          devShells.default = pkgs.mkShell {
            inherit (config.pre-commit) shellHook;
            packages =
              with pkgs;
              [
                rustToolchain
                cargo-deny
                cargo-edit
                cargo-watch
                openssl
                pkg-config
                rust-analyzer
              ]
              ++ config.pre-commit.settings.enabledPackages;

            env = {
              RUST_SRC_PATH = "${pkgs.rustToolchain}/lib/rustlib/src/rust/library";
            };
          };
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = lib.attrValues self.overlays;
          };
        };
      flake.overlays.default = final: prev: {
        nodejs = final.nodejs_24;
        rustToolchain =
          with inputs.fenix.packages.${prev.stdenv.hostPlatform.system};
          combine (
            with stable;
            [
              clippy
              rustc
              cargo
              rustfmt
              rust-src
            ]
          );
      };
    };
}
