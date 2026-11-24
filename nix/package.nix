{
  lib,
  stdenv,
  rustPlatform,
  pkg-config,
  alsa-lib,
  sqlite,
  cacert,
  makeWrapper,
  python3,
  larkSrc ? ../.,
}:

let
  source = lib.cleanSourceWith {
    src = lib.cleanSource larkSrc;
    filter =
      path: type:
      let
        name = baseNameOf path;
      in
      !(
        type == "directory"
        && lib.elem name [
          "target"
          "__pycache__"
          ".pytest_cache"
          ".mypy_cache"
          ".ruff_cache"
        ]
      )
      && builtins.match "result(-.*)?" name == null
      && !(lib.hasSuffix ".pyc" name || lib.hasSuffix ".pyo" name);
  };
  manifest = builtins.fromTOML (builtins.readFile "${source}/Cargo.toml");
in
rustPlatform.buildRustPackage {
  pname = "lark";
  version = manifest.workspace.package.version;
  src = source;
  cargoLock.lockFile = "${source}/Cargo.lock";
  cargoBuildFlags = [
    "--workspace"
    "--locked"
  ];
  cargoTestFlags = [
    "--workspace"
    "--locked"
  ];

  nativeBuildInputs = [
    pkg-config
    makeWrapper
  ];
  # The Nix cargo setup hook uses system SQLite, which includes FTS5.
  buildInputs = [ sqlite ] ++ lib.optionals stdenv.hostPlatform.isLinux [ alsa-lib ];

  nativeCheckInputs = [ python3 ];
  preCheck = ''
    export HOME="$(mktemp -d)"
    export XDG_CONFIG_HOME="$HOME/config" XDG_DATA_HOME="$HOME/data"
    export XDG_STATE_HOME="$HOME/state" XDG_CACHE_HOME="$HOME/cache" XDG_RUNTIME_DIR="$HOME/run"
    mkdir -p "$XDG_RUNTIME_DIR"
    export SSL_CERT_FILE="${cacert}/etc/ssl/certs/ca-bundle.crt"
  '';
  postCheck = ''
    for plugin in navidrome mpd; do
      env -i HOME="$HOME" TMPDIR="$TMPDIR" SSL_CERT_FILE="$SSL_CERT_FILE" "$(command -v python3)" -I -B -m unittest discover -s "plugins/$plugin" -v
    done
  '';

  postInstall = ''
    install_plugin() {
      local plugin="$1" script="$2" executable="$3"
      local installed="$out/share/lark/plugins/$plugin/$script"
      install -Dm644 "plugins/$plugin/$script" "$installed"
      install -Dm644 "plugins/$plugin/README.md" "$out/share/doc/lark/plugins/$plugin/README.md"
      cat > "$out/bin/$executable" <<EOF
    #!${stdenv.shell}
    if [ -z "\''${SSL_CERT_FILE+x}" ]; then
      export SSL_CERT_FILE="${cacert}/etc/ssl/certs/ca-bundle.crt"
    fi
    exec "${python3}/bin/python3" -I "$installed" "\$@"
    EOF
      chmod 755 "$out/bin/$executable"
    }

    wrapProgram "$out/bin/larkd" \
      --set-default SSL_CERT_FILE "${cacert}/etc/ssl/certs/ca-bundle.crt"
    install_plugin navidrome lark_source_navidrome.py lark-source-navidrome
    install_plugin mpd lark_frontend_mpd.py lark-frontend-mpd
    install -m644 README.md DEVELOPMENT.md config.example.toml "$out/share/doc/lark/"
  '';

  doInstallCheck = stdenv.hostPlatform.isLinux;
  nativeInstallCheckInputs = [ python3 ];
  installCheckPhase = ''
    runHook preInstallCheck
    python3 -I -B nix/tests/package-smoke.py "$out"
    runHook postInstallCheck
  '';

  meta = {
    description = "Terminal music client and plugin-based playback daemon";
    homepage = "https://github.com/ivyrs/lark";
    license = with lib.licenses; [
      mit
      asl20
    ];
    mainProgram = "lark";
    platforms = lib.platforms.linux ++ lib.platforms.darwin;
  };
}
