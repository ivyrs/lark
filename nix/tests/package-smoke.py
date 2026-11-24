"""Verify that a built package runs all its plugins without a checkout or PATH."""

from contextlib import ExitStack
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

# Share the same loopback fixture and protocol client as the development checks.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from mpd_smoke import MPDConnection, argument
from navidrome_smoke import NavidromeFixture
from smoke import wait_until


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def check(package):
    binaries = package / "bin"
    with ExitStack() as cleanup:
        root = Path(cleanup.enter_context(tempfile.TemporaryDirectory(prefix="lark-nix-")))
        fixture = NavidromeFixture()
        cleanup.callback(fixture.close)
        music = root / "Music"
        music.mkdir()
        (music / "local-fixture.wav").write_bytes(fixture.audio)
        # Replace the environment (rather than merging it) to avoid inheriting
        # personal credentials, sockets, state, or Python import paths.
        env = {
            "HOME": str(root),
            "PATH": str(root / "empty-path"),
            "XDG_CONFIG_HOME": str(root / "config"),
            "XDG_DATA_HOME": str(root / "data"),
            "XDG_STATE_HOME": str(root / "state"),
            "XDG_RUNTIME_DIR": str(root),
            "LARK_SOCKET": str(root / "lark.sock"),
            "LARK_LOG": "info",
            "NAVIDROME_PASSWORD": fixture.password,
        }
        config = root / "lark.toml"
        config.write_text(f"""output = "null"
[[plugin]]
name = "local"
exec = ["lark-source-local"]
[plugin.config]
music_dir = "~/Music"
[[plugin]]
name = "navidrome"
exec = ["lark-source-navidrome"]
[plugin.config]
url = {json.dumps(fixture.url)}
username = {json.dumps(fixture.username)}
allow_http = true
[[plugin]]
name = "mpd"
exec = ["lark-frontend-mpd"]
[plugin.config]
host = "127.0.0.1"
port = 0
""")
        log = root / "daemon.log"
        with log.open("w") as output:
            daemon = subprocess.Popen(
                [binaries / "larkd", "--config", config], env=env, cwd=root,
                stdout=output, stderr=output,
            )
        cleanup.callback(stop, daemon)

        def rpc(method, params=None):
            command = [binaries / "lark", "raw", method]
            if params is not None:
                command.append(json.dumps(params))
            result = subprocess.run(command, env=env, cwd=root, check=True,
                                    capture_output=True, text=True, timeout=10)
            return json.loads(result.stdout)

        def ready():
            logs = log.read_text()
            if daemon.poll() is not None:
                raise AssertionError("installed larkd exited")
            listener = re.search(r"MPD listening on 127\.0\.0\.1:(\d+)", logs)
            if listener and Path(env["LARK_SOCKET"]).exists():
                sources = {source["name"]: source["tracks"] for source in rpc("sources")}
                if sources == {"local": 1, "navidrome": 2}:
                    return int(listener.group(1))
            return None

        try:
            port = wait_until(ready, "local and Navidrome sync and MPD listener", timeout=20)
            client = MPDConnection(port)
            cleanup.callback(client.close)
            catalogue = client.command("listallinfo")
            assert any(line.startswith("file: local:track:") for line in catalogue), catalogue
            remote = next(line.partition(": ")[2] for line in catalogue
                          if line.startswith("file: navidrome:track:"))
            client.command("addid " + argument(remote))
            client.command("play 0")
            wait_until(lambda: rpc("history"), "HTTP fixture playback via packaged MPD frontend")
            assert fixture.count("stream") >= 1, "no HTTP stream requested"
        except Exception as error:
            raise AssertionError(f"installed Lark smoke check failed:\n{log.read_text()}") from error
    print("Installed Lark binaries and plugin launchers work without PATH or checkout")


if __name__ == "__main__":
    check(Path(sys.argv[1]).resolve())
