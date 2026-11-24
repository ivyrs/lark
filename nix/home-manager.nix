{ packageFor }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  inherit (lib)
    mkEnableOption
    mkIf
    mkMerge
    mkOption
    types
    ;

  cfg = config.services.lark;
  isLinux = pkgs.stdenv.hostPlatform.isLinux;
  toml = pkgs.formats.toml { };
  singleLine = path: !(lib.hasInfix "\n" path || lib.hasInfix "\r" path);
  runtimePath = types.addCheck types.str (
    path: lib.hasPrefix "/" path && !(builtins.hasContext path) && singleLine path
  );
  relativePath = types.addCheck types.str (
    path:
    !(builtins.hasContext path)
    && singleLine path
    && lib.all (
      part:
      !(lib.elem part [
        ""
        "."
        ".."
      ])
    ) (lib.splitString "/" path)
  );
  # EnvironmentFile uses a bare path, not ExecStart's quoted argument syntax.
  # It strips trailing whitespace and expands glob patterns.
  environmentPath = types.addCheck runtimePath (
    path:
    !(lib.any (char: lib.hasInfix char path) [
      "*"
      "?"
      "["
      "\\"
    ])
    && !(lib.any (char: lib.hasSuffix char path) [
      " "
      "\t"
    ])
  );

  configPath =
    if cfg.configFile != null then cfg.configFile else "${config.xdg.configHome}/${cfg.settingsPath}";
  configSource = toml.generate "lark-config.toml" cfg.settings;
  escapeSpecifiers = lib.replaceStrings [ "%" ] [ "%%" ];
  # ExecStart is parsed by systemd, not by a shell.
  execArg = arg: lib.replaceStrings [ "$" ] [ "$$" ] (escapeSpecifiers (builtins.toJSON arg));
in
{
  options.services.lark = {
    enable = mkEnableOption "the Lark music player";

    package = mkOption {
      type = types.package;
      default = packageFor pkgs;
      defaultText = lib.literalMD "The Lark package from this flake for the host platform.";
      description = "The Lark package to install and use for the user service.";
    };

    enableService = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Run larkd as a systemd user service tied to graphical-session.target.
        Disable this to install Lark and optionally manage its settings without
        starting a daemon at graphical login.
      '';
    };

    settings = mkOption {
      type = types.nullOr toml.type;
      default.plugin = [
        {
          name = "local";
          exec = [ (lib.getExe' cfg.package "lark-source-local") ];
          config.music_dir = "~/Music";
        }
      ];
      defaultText = lib.literalMD "A local source using the packaged lark-source-local and ~/Music.";
      description = ''
        Lark configuration serialized to TOML at {option}`services.lark.settingsPath`.
        A `plugin` list replaces the default local source; include it explicitly
        if you want local music alongside other plugins. Use absolute executable
        paths for plugins that are not packaged next to larkd.

        Set to `null` to use an existing unmanaged file without installing or
        replacing it. These settings are stored in the Nix store: do not put
        passwords or other secrets here.
      '';
    };

    settingsPath = mkOption {
      type = relativePath;
      default = "lark/config.toml";
      example = "lark/nix.toml";
      description = ''
        Configuration file location relative to {option}`xdg.configHome`, used
        both for generated settings and as the default unmanaged configuration
        location. Use a different filename to leave an existing Lark config in
        place while managing a separate service configuration.
      '';
    };

    configFile = mkOption {
      type = types.nullOr runtimePath;
      default = null;
      example = "/home/alice/.config/lark/manual.toml";
      description = ''
        Optional absolute runtime path to an unmanaged config file, as a string
        (not a Nix path). Requires {option}`services.lark.settings` to be `null`.
        When unset, the daemon uses {option}`services.lark.settingsPath` under
        {option}`xdg.configHome`.
      '';
    };

    environmentFile = mkOption {
      type = types.nullOr environmentPath;
      default = null;
      example = "/home/alice/.config/lark/private.env";
      description = ''
        Absolute runtime path to a private systemd EnvironmentFile, as a string
        (not a Nix path or glob). For the Navidrome plugin, put a
        `NAVIDROME_PASSWORD=…` assignment here (or use the variable named by its
        `password_env`). Create the file separately with permissions such as
        0600. The module does not read, generate or copy its contents.

        When configured, the unit skips startup if this file does not exist.
        Start the unit after provisioning the file, or restart it after rotating
        the password. This checks for the file, not the variables within it.
      '';
    };
  };

  config = mkMerge [
    (mkIf cfg.enable {
      assertions = [
        {
          assertion = isLinux;
          message = "services.lark is only supported on Linux.";
        }
        {
          assertion = cfg.settings == null || cfg.configFile == null;
          message = "services.lark.configFile requires services.lark.settings = null.";
        }
      ];
    })
    (mkIf (cfg.enable && isLinux) {
      home.packages = [ cfg.package ];

      xdg.configFile = mkIf (cfg.settings != null) {
        "${cfg.settingsPath}".source = configSource;
      };

      systemd.user.services.lark = mkIf cfg.enableService {
        Unit = {
          Description = "Lark music daemon";
          PartOf = [ "graphical-session.target" ];
          # The graphical target already orders itself after its wanted units.
          After = [
            "graphical-session-pre.target"
            "pipewire.service"
            "pipewire-pulse.service"
            "pulseaudio.service"
          ];
        }
        // lib.optionalAttrs (cfg.settings != null) { X-Restart-Triggers = [ configSource ]; }
        // lib.optionalAttrs (cfg.environmentFile != null) {
          ConditionPathExists = escapeSpecifiers cfg.environmentFile;
        };

        Service = {
          ExecStart = "${lib.getExe' cfg.package "larkd"} --config ${execArg configPath}";
          Restart = "on-failure";
          RestartSec = 5;
        }
        // lib.optionalAttrs (cfg.environmentFile != null) {
          EnvironmentFile = escapeSpecifiers cfg.environmentFile;
        };

        Install.WantedBy = [ "graphical-session.target" ];
      };
    })
  ];
}
