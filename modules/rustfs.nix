# RustFS service module for cybersec/cyberphy devenv.
# Adapted from cachix/devenv services.rustfs for the *pinned* devenv modules
# (92ad9c70) which lack processes.<name>.ports.allocate — we pin fixed ports
# via RUSTFS_* env instead (project standard: API 9010, console 9011).
{ pkgs, lib, config, inputs, ... }:

let
  cfg = config.services.rustfs;
  types = lib.types;

  # Prefer overlay package (release binary with console). Fall back to flake
  # only if overlay missing — flake build typically lacks /rustfs/console UI.
  defaultPackage =
    if pkgs ? rustfs then pkgs.rustfs
    else if inputs ? rustfs then inputs.rustfs.packages.${pkgs.stdenv.system}.default
    else throw "services.rustfs: set overlays so pkgs.rustfs comes from nix/rustfs.nix";

  bindAddr = if cfg.bind == null then "0.0.0.0" else cfg.bind;
  readyHost =
    if cfg.bind == null || cfg.bind == "0.0.0.0" || cfg.bind == "::"
    then "127.0.0.1"
    else cfg.bind;
  apiAddr = "${bindAddr}:${toString cfg.port}";
  consoleAddr = "${bindAddr}:${toString cfg.consolePort}";
in
{
  options.services.rustfs = {
    enable = lib.mkEnableOption "RustFS object storage (S3-compatible; replaces devenv services.minio)";

    package = lib.mkOption {
      type = types.package;
      description = "RustFS package (from github:rustfs/rustfs overlay)";
      default = defaultPackage;
      defaultText = lib.literalExpression "pkgs.rustfs or inputs.rustfs.packages.\${system}.default";
    };

    bind = lib.mkOption {
      type = types.nullOr types.str;
      default = "127.0.0.1";
      description = ''
        IP interface to bind. Use "0.0.0.0" so K8s pods can reach host S3
        via the node IP. `null` means all interfaces (same as 0.0.0.0).
      '';
    };

    port = lib.mkOption {
      type = types.port;
      default = 9000;
      description = "TCP port for the S3 API.";
    };

    consolePort = lib.mkOption {
      type = types.port;
      default = 9001;
      description = "TCP port for the web console.";
    };

    consoleEnable = lib.mkOption {
      type = types.bool;
      default = true;
      description = "Enable the web console.";
    };

    accessKey = lib.mkOption {
      type = types.str;
      default = "rustfsadmin";
      description = "Access key (5–20 characters).";
    };

    secretKey = lib.mkOption {
      type = types.str;
      default = "rustfsadmin";
      description = "Secret key (8–40 characters).";
    };

    extraEnvironment = lib.mkOption {
      type = types.attrsOf types.str;
      default = { };
      description = "Extra RUSTFS_* environment variables.";
    };
  };

  config = lib.mkIf cfg.enable {
    packages = [ cfg.package ];

    env = {
      RUSTFS_PORT = toString cfg.port;
      RUSTFS_CONSOLE_PORT = toString cfg.consolePort;
      RUSTFS_ADDRESS = apiAddr;
      RUSTFS_CONSOLE_ADDRESS = consoleAddr;
      RUSTFS_CONSOLE_ENABLE = if cfg.consoleEnable then "true" else "false";
      RUSTFS_ACCESS_KEY = cfg.accessKey;
      RUSTFS_SECRET_KEY = cfg.secretKey;
      # Default under DEVENV_STATE; projects usually override via extraEnvironment
      # (cyberphy: /raid/build/cyberphy/data/).
      RUSTFS_DATA_DIR = config.env.DEVENV_STATE + "/rustfs/data";
    } // cfg.extraEnvironment;

    processes.rustfs = {
      exec = ''
        set -euo pipefail
        mkdir -p "''${RUSTFS_DATA_DIR}"
        echo "Starting RustFS on ''${RUSTFS_ADDRESS} (console ''${RUSTFS_CONSOLE_ADDRESS})"
        exec ${cfg.package}/bin/rustfs "''${RUSTFS_DATA_DIR}"
      '';
      process-compose = {
        readiness_probe = {
          http_get = {
            host = readyHost;
            port = cfg.port;
            path = "/health";
          };
          initial_delay_seconds = 2;
          period_seconds = 3;
          failure_threshold = 20;
        };
      };
    };

    tasks."devenv:rustfs:setup" = {
      exec = ''
        mkdir -p "''${RUSTFS_DATA_DIR:-$DEVENV_STATE/rustfs/data}"
      '';
      before = [ "devenv:processes:rustfs" ];
    };
  };
}
