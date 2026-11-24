{ pkgs, home-manager }:
let
  inherit (pkgs) lib;
  # A fixture is enough to check all rendering and does not rebuild Lark.
  defaultPackage = pkgs.writeShellScriptBin "lark-fixture" "exit 0";
  customPackage = pkgs.writeShellScriptBin "lark-custom-fixture" "exit 0";
  module = import ../home-manager.nix {
    packageFor =
      packagePkgs:
      if packagePkgs.stdenv.hostPlatform.isLinux then
        defaultPackage
      else
        throw "the flake does not publish a non-Linux Lark package";
  };
  evaluateWith =
    testPkgs: services:
    (home-manager.lib.homeManagerConfiguration {
      pkgs = testPkgs;
      modules = [
        module
        {
          home.username = "lark-test";
          home.homeDirectory = "/home/lark-test";
          home.stateVersion = "26.05";
          xdg.configHome = "/home/lark-test/custom config";
          services.lark = services;
        }
      ];
    }).config;
  evaluate = evaluateWith pkgs;
  cfg = evaluate { enable = true; };
  disabled = evaluate { };
  noService = evaluate {
    enable = true;
    enableService = false;
  };
  manual = evaluate {
    enable = true;
    settings = null;
  };
  overridden = evaluate {
    enable = true;
    settings = null;
    configFile = "/home/lark-test/manual $conf%ig.toml";
  };
  # Intentionally not provisioned; evaluation and builds must not read it.
  envPath = "/home/lark-test/private dir/lark%secret.env";
  customized = evaluate {
    enable = true;
    package = customPackage;
    settingsPath = "lark/nix.toml";
    environmentFile = envPath;
    settings.plugin = [
      {
        name = "navidrome";
        exec = [ "${customPackage}/bin/lark-source-navidrome" ];
        config = {
          url = "https://example.invalid";
          username = "fixture";
          password_env = "LARK_NAV_PASSWORD";
        };
      }
      {
        name = "mpd";
        exec = [ "${customPackage}/bin/lark-frontend-mpd" ];
        config.port = 6600;
      }
    ];
  };
  unit = c: c.systemd.user.services.lark;
  renderedUnit = c: c.xdg.configFile."systemd/user/lark.service".source;
  hasSettings = c: c.xdg.configFile ? "lark/config.toml";
  rejected = config: !(builtins.tryEval config.home.activationPackage.drvPath).success;
  darwinPkgs = import pkgs.path { system = "aarch64-darwin"; };
in
assert !(disabled.systemd.user.services ? lark);
assert !(hasSettings disabled);
assert !(lib.elem defaultPackage disabled.home.packages);
assert hasSettings noService;
assert !(noService.systemd.user.services ? lark);
assert lib.elem defaultPackage noService.home.packages;
assert hasSettings cfg;
assert lib.elem defaultPackage cfg.home.packages;
assert (unit cfg).Unit.PartOf == [ "graphical-session.target" ];
assert lib.elem "pipewire.service" (unit cfg).Unit.After;
assert (unit cfg).Install.WantedBy == [ "graphical-session.target" ];
assert cfg.xdg.configFile ? "systemd/user/graphical-session.target.wants/lark.service";
assert (unit cfg).Service.Restart == "on-failure";
assert !(hasSettings manual) && !(hasSettings overridden);
assert ((unit manual).Unit.X-Restart-Triggers or [ ]) == [ ];
assert !((unit manual).Service ? EnvironmentFile);
assert !((unit manual).Unit ? ConditionPathExists);
assert
  (unit manual).Service.ExecStart
  == [ ''${defaultPackage}/bin/larkd --config "/home/lark-test/custom config/lark/config.toml"'' ];
assert
  (unit overridden).Service.ExecStart
  == [ ''${defaultPackage}/bin/larkd --config "/home/lark-test/manual $$conf%%ig.toml"'' ];
assert !(hasSettings customized);
assert lib.elem customPackage customized.home.packages;
assert
  (unit customized).Unit.X-Restart-Triggers == [ customized.xdg.configFile."lark/nix.toml".source ];
assert (unit customized).Service.EnvironmentFile == "/home/lark-test/private dir/lark%%secret.env";
assert (unit customized).Unit.ConditionPathExists == (unit customized).Service.EnvironmentFile;
assert ((unit customized).Service.Environment or [ ]) == [ ];
assert !(builtins.hasContext (unit customized).Service.EnvironmentFile);
assert rejected (evaluate {
  enable = true;
  configFile = "/home/lark-test/manual.toml";
});
assert rejected (evaluate {
  enable = true;
  settingsPath = "/outside.toml";
});
assert rejected (evaluate {
  enable = true;
  settingsPath = "lark/../traversal.toml";
});
assert rejected (evaluate {
  enable = true;
  environmentFile = ./home-manager.nix;
});
assert rejected (evaluate {
  enable = true;
  environmentFile = "${./home-manager.nix}";
});
assert rejected (evaluateWith darwinPkgs { enable = true; });
pkgs.runCommand "lark-home-manager-check"
  {
    nativeBuildInputs = [ pkgs.python3 ];
    defaultConfig = cfg.xdg.configFile."lark/config.toml".source;
    customConfig = customized.xdg.configFile."lark/nix.toml".source;
    customUnit = renderedUnit customized;
    inherit defaultPackage customPackage;
  }
  ''
    python3 - <<'PY'
    import os
    from pathlib import Path
    import tomllib

    def config(var):
        return tomllib.loads(Path(os.environ[var]).read_text())

    assert config("defaultConfig")["plugin"] == [{
        "name": "local",
        "exec": [os.environ["defaultPackage"] + "/bin/lark-source-local"],
        "config": {"music_dir": "~/Music"},
    }]
    nav, mpd = config("customConfig")["plugin"]
    package = os.environ["customPackage"]
    assert nav == {
        "name": "navidrome",
        "exec": [package + "/bin/lark-source-navidrome"],
        "config": {
            "url": "https://example.invalid",
            "username": "fixture",
            "password_env": "LARK_NAV_PASSWORD",
        },
    }
    assert mpd == {
        "name": "mpd",
        "exec": [package + "/bin/lark-frontend-mpd"],
        "config": {"port": 6600},
    }
    unit = Path(os.environ["customUnit"]).read_text().splitlines()
    assert f'ExecStart={package}/bin/larkd --config "/home/lark-test/custom config/lark/nix.toml"' in unit
    assert "EnvironmentFile=/home/lark-test/private dir/lark%%secret.env" in unit
    assert "ConditionPathExists=/home/lark-test/private dir/lark%%secret.env" in unit
    PY
    touch "$out"
  ''
