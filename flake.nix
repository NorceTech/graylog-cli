{
  description = "graylog-cli - Rust CLI for Graylog";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
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
          version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package.version;

          windowsTarget = "x86_64-pc-windows-gnu";

          # One toolchain everywhere, like the example: fenix nightly
          # `complete.toolchain` (rustc, cargo, clippy, rustfmt, rust-src)
          # plus the Windows std so the cross build reuses the exact same
          # compiler. Every build and both dev shells share this.
          toolchain = inputs.fenix.packages.${system}.combine [
            inputs.fenix.packages.${system}.complete.toolchain
            inputs.fenix.packages.${system}.targets.${windowsTarget}.latest.rust-std
          ];

          craneLib = (inputs.crane.mkLib pkgs).overrideToolchain toolchain;

          commonArgs = {
            inherit pname version;
            src = craneLib.cleanCargoSource self;
            strictDeps = true;
            # Integration tests need loopback networking (the ping test serves
            # a local HTTP server), which the nix sandbox blocks. The suite
            # runs with network in the CI test job instead.
            doCheck = false;
          };

          # Dependencies are built once and reused by the package build, so a
          # source-only change never recompiles the dependency tree.
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          nativePackage = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              # On Darwin, Nix embeds its own store path for libiconv into the
              # binary.  Rewrite it to the system path so the binary runs on
              # machines without Nix installed.
              postInstall = lib.optionalString pkgs.stdenv.isDarwin ''
                install_name_tool \
                  -change "$(otool -L $out/bin/graylog-cli \
                    | awk '/libiconv/{print $1}')" \
                  /usr/lib/libiconv.2.dylib \
                  $out/bin/graylog-cli
              '';
            }
          );

          # The mingw cross build keeps nixpkgs' rustPlatform (crane has no
          # equivalent of pkgsCross' cross stdenv wiring here), but it is fed
          # the same fenix toolchain as everything else.
          windowsPkgs = pkgs.pkgsCross.mingwW64;
          windowsRustPlatform = windowsPkgs.makeRustPlatform {
            cargo = toolchain;
            rustc = toolchain;
          };
          windowsPackage = windowsRustPlatform.buildRustPackage {
            inherit pname version;
            src = self;
            cargoLock.lockFile = ./Cargo.lock;
            # Same reason as crane above: the ping integration test needs
            # loopback networking, unavailable in the nix sandbox.
            doCheck = false;
            cargoBuildTarget = windowsTarget;
            depsBuildBuild = lib.optionals pkgs.stdenv.isDarwin [
              pkgs.libiconv
            ];
            NIX_LDFLAGS = lib.optionalString pkgs.stdenv.isDarwin "-L${pkgs.libiconv}/lib";
            stdenv = windowsPkgs.stdenv;
          };
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
            programs.rustfmt.package = toolchain;
            programs.prettier.enable = true;
          };

          pre-commit.settings.hooks = {
            treefmt.enable = true;
          };

          devShells = {
            default = pkgs.mkShell {
              inherit (config.pre-commit) shellHook;
              packages = [
                toolchain
                pkgs.bacon
                pkgs.cargo-deny
                pkgs.cargo-edit
                pkgs.cargo-udeps
              ]
              ++ config.pre-commit.settings.enabledPackages;
              env = {
                RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
              };
            };

            # Lean shell for CI jobs that only need cargo + cargo-deny.
            ci = pkgs.mkShell {
              packages = [
                toolchain
                pkgs.cargo-deny
              ];
            };
          };

          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = lib.attrValues self.overlays;
          };
        };

      flake.overlays.fenix = inputs.fenix.overlays.default;
    };
}
