"""
Runs the `differential_s3` DAG (see ../dags/differential_s3.py) once against
LocalStack S3 directly and once against aws2azure+Azurite, then diffs the
final XCom summary between the two runs.

This is the app-level counterpart to aws2azure's own wire-protocol Tier-2
conformance suite (LocalStack vs proxy-over-Azurite, at the HTTP response
level): here we drive an actual Airflow DAG on each backend and compare
outcomes an application would actually observe.

Usage:
    python3 scripts/run_differential.py

Requires Docker and the `requests` package. Must be run from the
`differential/` directory (docker compose files use relative includes).
"""
import subprocess
import sys
import time

import requests

DAG_ID = "differential_s3"
WEBSERVER = "http://localhost:8090"
AUTH = ("admin", "admin")

BACKENDS = {
    "localstack": "docker-compose.localstack.yml",
    "aws2azure": "docker-compose.aws2azure.yml",
}


def compose(compose_file, *args):
    subprocess.run(["docker", "compose", "-f", compose_file, *args], check=True)


def wait_for_webserver(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{WEBSERVER}/health", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit("webserver did not become reachable in time")


def api(method, path, **kwargs):
    r = requests.request(method, f"{WEBSERVER}/api/v1{path}", auth=AUTH, timeout=15, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}


def trigger_and_wait(timeout=300):
    api("PATCH", f"/dags/{DAG_ID}", json={"is_paused": False})
    run_id = api("POST", f"/dags/{DAG_ID}/dagRuns", json={})["dag_run_id"]
    print(f"  triggered dag run {run_id}")

    deadline = time.time() + timeout
    while time.time() < deadline:
        state = api("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")["state"]
        if state in ("success", "failed"):
            break
        time.sleep(5)
    else:
        sys.exit(f"dag run {run_id} did not finish within {timeout}s")

    if state != "success":
        sys.exit(f"dag run {run_id} did not succeed (state={state})")
    print(f"  dag run {run_id} succeeded")
    return run_id


def fetch_summary_xcom(run_id):
    resp = api(
        "GET",
        f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances/summarize/xcomEntries/return_value",
    )
    return resp["value"]


def run_backend(name, compose_file):
    print(f"[{name}] starting stack ({compose_file}) ...")
    compose(compose_file, "up", "--build", "-d")
    try:
        wait_for_webserver()
        run_id = trigger_and_wait()
        summary = fetch_summary_xcom(run_id)
        print(f"[{name}] summary: {summary}")
        return summary
    finally:
        print(f"[{name}] tearing down ...")
        compose(compose_file, "down", "-v")


def main():
    results = {}
    for name, compose_file in BACKENDS.items():
        results[name] = run_backend(name, compose_file)

    baseline_name = "localstack"
    baseline = results[baseline_name]
    mismatches = []
    for name, summary in results.items():
        if name == baseline_name:
            continue
        if summary != baseline:
            mismatches.append((name, summary))

    print("\n=== Differential result ===")
    for name, summary in results.items():
        print(f"{name}: {summary}")

    if mismatches:
        print("\nFAIL: divergence between backends:")
        for name, summary in mismatches:
            print(f"  {baseline_name} != {name}")
            print(f"    {baseline_name}: {baseline}")
            print(f"    {name}: {summary}")
        sys.exit(1)

    print(
        f"\nPASS: {DAG_ID} produced an identical final state on all "
        f"backends ({', '.join(BACKENDS)})"
    )


if __name__ == "__main__":
    main()
