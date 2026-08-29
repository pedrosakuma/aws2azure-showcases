"""
End-to-end verification for the dynein-dynamodb case study.

Downloads the official `dynein` CLI release binary (awslabs/dynein) and
drives it through a real table lifecycle against aws2azure -> the local
Cosmos DB emulator:

  1. Wait for aws2azure's health check to pass.
  2. `dy admin create table` -- CreateTable.
  3. `dy put` / `dy get` -- PutItem / GetItem.
  4. `dy query` -- Query (partition-key condition).
  5. `dy scan` -- Scan.
  6. `dy del` -- DeleteItem, then confirm the item is gone via `dy scan`.
  7. `dy admin delete table` -- DeleteTable (needs a pseudo-tty for its
     interactive y/n confirmation prompt -- a real dynein limitation for
     scripted/CI use, worked around here with the `script` command).

dynein is built on the standard Rust AWS SDK, which honors the
AWS_ENDPOINT_URL environment variable out of the box -- no dynein code
changes or custom build needed, only env vars pointed at aws2azure. This
is a genuine, unmodified real-world DynamoDB client exercising aws2azure's
DynamoDB -> Cosmos DB module end-to-end.

Requires: `docker compose up -d` already run, and
`python3 scripts/bootstrap_cosmos_db.py` already executed once to create
the underlying Cosmos database.
"""
import os
import platform
import pty
import select
import stat
import subprocess
import sys
import tarfile
import time
import urllib.request

DYNEIN_VERSION = "v0.3.0"
DYNEIN_ASSET = "dynein-linux.tar.gz"
DYNEIN_URL = (
    f"https://github.com/awslabs/dynein/releases/download/{DYNEIN_VERSION}/{DYNEIN_ASSET}"
)

PROXY_HEALTH_URL = "http://localhost:8080"
ENDPOINT_URL = "http://dynamodb.127.0.0.1.nip.io:8080"
ACCESS_KEY = "AKIA-DYNEIN-DYNAMODB-SHOWCASE"
SECRET_KEY = "showcase-local-dev-secret-not-real"
REGION = "us-east-1"
TABLE_NAME = "DyneinShowcase"

WORKDIR = os.path.dirname(os.path.abspath(__file__))
DYNEIN_BIN = os.path.join(WORKDIR, "dy")


def download_dynein() -> None:
    if os.path.exists(DYNEIN_BIN):
        return
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "AMD64"):
        print(f"FAIL: dynein download only wired for linux/x86_64 in this script "
              f"(got {platform.system()}/{platform.machine()})")
        sys.exit(1)
    archive_path = os.path.join(WORKDIR, DYNEIN_ASSET)
    print(f"downloading dynein {DYNEIN_VERSION}...")
    urllib.request.urlretrieve(DYNEIN_URL, archive_path)
    with tarfile.open(archive_path) as tf:
        tf.extractall(WORKDIR)  # noqa: S202 -- trusted, pinned release asset
    os.remove(archive_path)
    st = os.stat(DYNEIN_BIN)
    os.chmod(DYNEIN_BIN, st.st_mode | stat.S_IEXEC)
    print("dynein ready")


def dy_env() -> dict:
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
    env["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY
    env["AWS_REGION"] = REGION
    env["AWS_ENDPOINT_URL"] = ENDPOINT_URL
    return env


def run_dy(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        [DYNEIN_BIN, *args],
        env=dy_env(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"`dy {' '.join(args)}` failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result.stdout


def wait_for_proxy() -> None:
    for _ in range(60):
        try:
            run_dy("--region", REGION, "admin", "list", check=True)
            print("aws2azure is up and DynamoDB requests are routing")
            return
        except Exception:
            time.sleep(2)
    print("FAIL: aws2azure never became reachable via dynein")
    sys.exit(1)


def delete_table_via_pty() -> str:
    master, slave = pty.openpty()
    proc = subprocess.Popen(
        [DYNEIN_BIN, "--region", REGION, "admin", "delete", "table", TABLE_NAME],
        stdin=slave, stdout=slave, stderr=slave,
        env=dy_env(),
    )
    os.close(slave)
    output = b""
    answered = False
    deadline = time.time() + 20
    while time.time() < deadline:
        ready, _, _ = select.select([master], [], [], 1)
        if master in ready:
            try:
                chunk = os.read(master, 1024)
            except OSError:
                break
            if not chunk:
                break
            output += chunk
            if not answered and b"[y/n]" in chunk:
                os.write(master, b"y\n")
                answered = True
        if proc.poll() is not None:
            time.sleep(0.5)
            try:
                output += os.read(master, 4096)
            except OSError:
                pass
            break
    else:
        proc.kill()
    os.close(master)
    proc.wait(timeout=5)
    if proc.returncode != 0:
        raise AssertionError(
            f"dy admin delete table exited {proc.returncode}: {output.decode(errors='replace')}"
        )
    return output.decode(errors="replace")


def main() -> None:
    download_dynein()
    wait_for_proxy()

    out = run_dy("--region", REGION, "admin", "create", "table", TABLE_NAME,
                  "--keys", "pk,S", "sk,N")
    assert "status: ACTIVE" in out, f"table did not report ACTIVE: {out}"
    print(f"created table '{TABLE_NAME}' via dynein CreateTable")

    run_dy("--region", REGION, "--table", TABLE_NAME, "put", "pk1", "sk1",
           "-i", '{"sk": 1, "message": "hello from dynein"}')
    print("put an item via dynein PutItem")

    out = run_dy("--region", REGION, "--table", TABLE_NAME, "get", "pk1", "1")
    assert "hello from dynein" in out, f"GetItem did not return the item: {out}"
    print("read the item back via dynein GetItem")

    out = run_dy("--region", REGION, "--table", TABLE_NAME, "query", "pk1")
    assert "pk1" in out, f"Query did not return the item: {out}"
    print("queried the item via dynein Query")

    out = run_dy("--region", REGION, "--table", TABLE_NAME, "scan")
    assert "pk1" in out, f"Scan did not return the item: {out}"
    print("scanned the table via dynein Scan")

    run_dy("--region", REGION, "--table", TABLE_NAME, "del", "pk1", "1")
    out = run_dy("--region", REGION, "--table", TABLE_NAME, "scan")
    assert "pk1" not in out, f"item still present after DeleteItem: {out}"
    print("deleted the item via dynein DeleteItem, confirmed via Scan")

    # `dy admin delete table` prompts for an interactive y/n confirmation
    # and panics with a "not a terminal" IO error if stdin isn't a real
    # tty -- a genuine dynein limitation for scripted/CI use. Piping
    # "y" via a plain pipe doesn't satisfy it (dynein still sees a
    # non-tty stdin and panics), so we allocate a real pseudo-terminal
    # with Python's `pty` module and answer the prompt once it appears.
    output = delete_table_via_pty()
    assert "has been started" in output, (
        f"DeleteTable did not confirm as started:\n{output}"
    )
    print("deleted the table via dynein DeleteTable")

    print("PASS: full dynein create/put/get/query/scan/del/delete-table "
          "lifecycle succeeded end-to-end via aws2azure/Cosmos DB")


if __name__ == "__main__":
    main()
