# Nix package and Home Manager

The flake exports `packages.<system>.default` (also `.lark`), `apps.<system>.default`,
`apps.<system>.larkd`, and `homeManagerModules.default` (also `.lark`).
The user-service module is Linux-only. Packaging on other advertised platforms
has not been verified here.

## Package

```sh
nix build .#default
```

New source files must be tracked by Git before using a Git flake reference.
A committed revision is required for a reproducible input pin. The package
cleans build artifacts and Python caches from its source; it includes these
entrypoints without needing the checkout, Python on PATH, or `nix develop`:

- `lark`, `larkd`, `lark-source-local`
- `lark-source-navidrome`, `lark-frontend-mpd`

Use the latter names in plugin `exec` lists instead of `python3` and a checkout
path. The daemon and Python launchers supply a CA bundle if `SSL_CERT_FILE` is
unset; an explicit caller certificate bundle is respected. TLS verification is
never disabled. The native Linux build runs Rust tests, Python source/frontend
unit tests, and an installed-binary smoke with an empty PATH and temporary state.

## Home Manager

Add a committed Lark revision as a flake input, then import
`inputs.lark.homeManagerModules.default` in the user's Home Manager modules.
For local development a `git+file:///absolute/path/to/lark` input can be pinned;
that is machine-local, not a portable replacement for a published repository.

```nix
{ config, inputs, pkgs, ... }:
let
  package = inputs.lark.packages.${pkgs.stdenv.hostPlatform.system}.default;
in {
  imports = [ inputs.lark.homeManagerModules.default ];
  services.lark = {
    enable = true;
    inherit package;
    # Use a separate file to preserve an existing handmade config.toml.
    settingsPath = "lark/nix.toml";
    environmentFile = "${config.xdg.configHome}/lark/environment";
    settings.plugin = [
      { name = "local"; exec = [ "${package}/bin/lark-source-local" ];
        config.music_dir = "~/Music"; }
      { name = "navidrome"; exec = [ "${package}/bin/lark-source-navidrome" ];
        config = { url = "https://music.example.com"; username = "your-user";
          password_env = "NAVIDROME_PASSWORD"; }; }
      { name = "mpd"; exec = [ "${package}/bin/lark-frontend-mpd" ];
        config = { host = "127.0.0.1"; port = 6600; }; }
    ];
  };
}
```

`settings.plugin` replaces the default local-only list. With `settings = null`,
the module leaves config unmanaged; `configFile` can select an existing absolute
runtime filename. `enableService = false` installs the package/config without
autostart. Generated settings live in the Nix store: never put passwords there.

## Runtime password

The module does **not** create or read `environmentFile`. Provision it separately,
or point the option at an appropriate runtime sops/agenix-generated environment
file. Do not use `builtins.readFile`, a Nix path literal, or a secret value in the
settings. A literal absolute string references the runtime path without copying
its contents into the store.

For a manually managed file, create it privately without overwriting an existing
one, then edit it (do not put the password in shell history or this repository):

```sh
mkdir -p ~/.config/lark
(umask 077; touch ~/.config/lark/environment)
chmod 600 ~/.config/lark/environment
$EDITOR ~/.config/lark/environment
```

Add a systemd EnvironmentFile assignment such as
`NAVIDROME_PASSWORD="your password"`, using its quoting rules for quotes and
backslashes. If the plugin uses another `password_env`, use that name instead.
The unit skips startup until this file exists; it cannot check its variables.
After provisioning or rotating it, start/restart the user service explicitly.

## Activation and data

Review the system/Home Manager changes, then perform your normal build/switch.
Do not run the new service alongside a manually started larkd on the same socket:
stop the terminal daemon during the handover, then use:

```sh
systemctl --user start lark
systemctl --user status lark
journalctl --user -u lark -f
lark sources
```

The service follows `graphical-session.target` and uses the current user's
PipeWire/ALSA session. There is no root/system daemon, audio permission change,
firewall opening, or forced lingering. The existing XDG library, history and
plugin state are reused; no database reset or cache purge is performed. The
queue is still lost on daemon restart. Preserve backups before major upgrades.
MPD remains loopback-only; retain your working rmpc config/address. Existing
playerctl/MPRIS media keys are unchanged because Lark does not yet implement MPRIS.
