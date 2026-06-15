{
  # waitbus Nix flake — SKELETON. First publish at v0.4.1.
  #
  # Built with uv2nix (org: pyproject-nix; tool: uv2nix), reading
  # uv.lock directly. uv.lock is the single source of truth — PEP 751
  # pylock.toml is not consumed by any Nix tool as of this writing, so
  # it is intentionally not used here.
  #
  # If flake.lock is absent or stale, regenerate it with:
  #     nix flake lock
  #
  # Production interpreter is Python 3.13 (NOT 3.14 — Hydra failures on
  # niche packages). sourcePreference = "wheel".
  description = "waitbus — workstation GitHub Actions status reporter + broadcast bus + MCP server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
    };
  };

  outputs =
    { self, nixpkgs, pyproject-nix, pyproject-build-systems, uv2nix, ... }:
    let
      inherit (nixpkgs) lib;

      # uv2nix reads uv.lock; the workspace overlay materialises the
      # locked closure into a Python package set.
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
      overlay = workspace.mkPyprojectOverlay { sourcePreference = "wheel"; };

      forAllSystems = lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      pkgsFor = system: nixpkgs.legacyPackages.${system};

      pythonSetFor =
        system:
        let
          pkgs = pkgsFor system;
          python = pkgs.python313;
        in
        (pkgs.callPackage pyproject-nix.build.packages { inherit python; }).overrideScope (
          lib.composeManyExtensions [
            pyproject-build-systems.overlays.default
            overlay
          ]
        );
    in
    {
      packages = forAllSystems (
        system:
        let
          pythonSet = pythonSetFor system;
        in
        {
          default = pythonSet.mkVirtualEnv "waitbus-env" workspace.deps.default;
        }
      );

      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/waitbus";
        };
      });

      # ---- Linux supervisor module (SKELETON) ---------------------------
      # systemd user services with LoadCredentialEncrypted= secret
      # delivery. Unit wiring is a TODO; the option surface is stubbed
      # so downstream NixOS configs can already reference it.
      nixosModules.waitbus =
        { config, lib, pkgs, ... }:
        let
          cfg = config.services.waitbus;
        in
        {
          options.services.waitbus = {
            enable = lib.mkEnableOption "waitbus daemons (listener, broadcast, etag-poll, watchdog)";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.default;
              description = "The waitbus package to run.";
            };
          };
          config = lib.mkIf cfg.enable {
            # TODO: systemd.user.services.waitbus-broadcast etc., each with
            # LoadCredentialEncrypted=broadcast-token:... and the
            # hardening block (NoNewPrivileges, MemoryDenyWriteExecute,
            # UMask=0077, LimitNOFILE=1024, locale pinning).
            assertions = [
              {
                assertion = cfg.package != null;
                message = "services.waitbus.package must be set";
              }
            ];
          };
        };

      # ---- macOS supervisor module (SKELETON) ---------------------------
      # launchd daemons + Keychain (`security find-generic-password`)
      # secret reads. Mitigate the /nix/store mount race with wait4path.
      darwinModules.waitbus =
        { config, lib, pkgs, ... }:
        let
          cfg = config.services.waitbus;
        in
        {
          options.services.waitbus = {
            enable = lib.mkEnableOption "waitbus launchd daemons";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.default;
              description = "The waitbus package to run.";
            };
          };
          config = lib.mkIf cfg.enable {
            # TODO: launchd.daemons.waitbus-broadcast etc. ProgramArguments
            # must wait4path /nix/store before exec to dodge
            # LnL7/nix-darwin#1043; secrets via security
            # find-generic-password rather than systemd-creds.
            assertions = [
              {
                assertion = cfg.package != null;
                message = "services.waitbus.package must be set";
              }
            ];
          };
        };
    };
}
