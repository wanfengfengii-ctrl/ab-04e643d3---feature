#!/usr/bin/env python3
"""One-shot acceptance service (`verify`).

It runs against the Docker Compose stack:

  1. executes the pytest contract suite against the healthy API instance;
  2. starts *additional* API workers in this container with deterministic
     fault injection enabled, sharing the same durable PostgreSQL database,
     and verifies the two mandated crash points plus cross-instance cursor
     continuation.

Production API runs with FAULT_POINTS unset (injection disabled); fault
workers here are launched explicitly and only for acceptance.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid

import httpx

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "eventsvc")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "eventsvc")
DB_NAME = os.environ.get("DB_NAME", "eventsvc")
BASE_URL = os.environ.get("BASE_URL", "http://api:8080").rstrip("/")
CLOCK_TOKEN = os.environ.get("CLOCK_OVERRIDE_TOKEN", "acceptance-clock")
FAULT_TOKEN = os.environ.get("FAULT_TOKEN", "acceptance-fault")

FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(label)


class Api:
    def __init__(self, base_url: str, now: float = 1_900_000_000.0):
        self.base_url = base_url.rstrip("/")
        self.http = httpx.Client(base_url=self.base_url, timeout=20.0)
        self.now = now

    def headers(self) -> dict:
        return {"X-Now": f"{self.now:.3f}", "X-Now-Token": CLOCK_TOKEN}

    def req(self, method: str, path: str, json_body=None, params=None,
            expect_error=False, raw=False):
        resp = self.http.request(method, path, json=json_body, params=params,
                                 headers=self.headers())
        if raw:
            return resp
        if expect_error:
            return resp.status_code, resp.json()["error"]
        if resp.status_code >= 400:
            raise AssertionError(f"{method} {path} -> {resp.status_code} {resp.text}")
        return resp.status_code, resp.json()

    def wait_ready(self, timeout: float = 40.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = self.http.get("/health/ready", timeout=2)
                if r.status_code == 200 and r.json().get("database") == "up":
                    return True
            except httpx.TransportError:
                pass
            time.sleep(0.4)
        return False


def start_worker(port: int, fault_points: str, log_path: str,
                 extra_env: dict | None = None) -> subprocess.Popen:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    workdir = "/srv" if os.path.isdir("/srv/app") else repo_root
    env = dict(os.environ)
    env.update({
        "DB_HOST": DB_HOST, "DB_PORT": DB_PORT, "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD, "DB_NAME": DB_NAME,
        "FAULT_POINTS": fault_points,
        "FAULT_TOKEN": FAULT_TOKEN,
        "CLOCK_OVERRIDE_TOKEN": CLOCK_TOKEN,
    })
    if extra_env:
        env.update(extra_env)
    log = open(log_path, "wb")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        env=env, stdout=log, stderr=subprocess.STDOUT, cwd=workdir,
    )
    return proc


def wait_exit(proc: subprocess.Popen, timeout: float = 10.0) -> int | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rc = proc.poll()
        if rc is not None:
            return rc
        time.sleep(0.2)
    return None


def run_pytest() -> bool:
    print("=== Stage 1: pytest contract suite against", BASE_URL, "===")
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    workdir = "/srv" if os.path.isdir("/srv/app") else repo_root
    env = dict(os.environ)
    env["BASE_URL"] = BASE_URL
    env["CLOCK_OVERRIDE_TOKEN"] = CLOCK_TOKEN
    rc = subprocess.run([sys.executable, "-m", "pytest", "tests", "-q", "--tb=short"],
                        env=env, cwd=workdir).returncode
    check("pytest contract suite (rc=0)", rc == 0, f"rc={rc}")
    return rc == 0


def scenario_crash_after_commit() -> None:
    print("\n=== Stage 2: crash AFTER durable commit, BEFORE response ===")
    main = Api(BASE_URL)
    port = 8090
    proc = start_worker(port, "crash-after-commit", "/tmp/fault1.log")
    try:
        worker = Api(f"http://127.0.0.1:{port}")
        check("fault worker became ready", worker.wait_ready())

        producer = f"crashprod_{uuid.uuid4().hex[:10]}"
        stream = f"crashstream_{uuid.uuid4().hex[:10]}"
        tx_id = f"t_{uuid.uuid4().hex[:10]}"
        worker.req("POST", "/v1/producers", {"name": producer, "epoch": 1})
        worker.req("POST", "/v1/transactions",
                   {"producer": producer, "txId": tx_id, "epoch": 1})
        events = [{"stream": stream, "key": f"k{i}", "payload": {"i": i}}
                  for i in range(5)]
        worker.req("PUT", f"/v1/transactions/{producer}/{tx_id}/batch",
                   {"epoch": 1, "firstSequence": 1, "events": events})

        crashed = False
        try:
            worker.req("POST", f"/v1/transactions/{producer}/{tx_id}/commit",
                       {"epoch": 1, "faultToken": FAULT_TOKEN}, raw=True)
        except (httpx.TransportError, httpx.RemoteProtocolError):
            crashed = True
        check("commit request died with the process (response lost)", crashed)
        rc = wait_exit(proc)
        check("fault worker actually terminated", rc is not None, f"rc={rc}")

        # A healthy instance (the permanent API) must expose the durable
        # result immediately, despite the lost response.
        sc, body = main.req("GET", f"/v1/transactions/{producer}/{tx_id}")
        check("committed result visible after crash", body["status"] == "committed",
              json.dumps(body))
        position = body["commitPosition"]
        check("commitPosition assigned", isinstance(position, int) and position > 0)

        # Retrying the commit returns the SAME position and creates no
        # second batch of events.
        sc, retry = main.req("POST", f"/v1/transactions/{producer}/{tx_id}/commit",
                             {"epoch": 1})
        check("commit retry returns same commitPosition",
              retry["commitPosition"] == position and retry["status"] == "committed")

        # Exactly one envelope, exactly five events visible.
        _, snap = main.req("POST", "/v1/snapshots",
                           {"streams": [stream], "ttlSeconds": 3600, "pageSize": 100})
        envelopes = snap["page"]["transactions"]
        match = [e for e in envelopes if e["txId"] == tx_id]
        check("exactly one visible envelope for retried tx", len(match) == 1,
              f"found {len(match)}")
        if match:
            check("no duplicate events (exactly 5)",
                  len(match[0]["events"]) == 5,
                  f"found {len(match[0]['events'])}")
            check("envelope sequence contiguous from 1",
                  [e["sequence"] for e in match[0]["events"]] == [1, 2, 3, 4, 5])
        main.req("DELETE", f"/v1/snapshots/{snap['snapshotId']}")
        return_position = position
    finally:
        if proc.poll() is None:
            proc.kill()


def scenario_crash_during_reclaim() -> None:
    print("\n=== Stage 3: crash DURING historical reclamation ===")
    main = Api(BASE_URL)
    port = 8091
    producer = f"recprod_{uuid.uuid4().hex[:10]}"
    stream = f"recstream_{uuid.uuid4().hex[:10]}"
    group = f"recgrp_{uuid.uuid4().hex[:10]}"

    # Commit one envelope via the healthy instance.
    main.req("POST", "/v1/producers", {"name": producer, "epoch": 1})
    tx_id = f"t_{uuid.uuid4().hex[:10]}"
    main.req("POST", "/v1/transactions",
             {"producer": producer, "txId": tx_id, "epoch": 1})
    main.req("PUT", f"/v1/transactions/{producer}/{tx_id}/batch",
             {"epoch": 1, "firstSequence": 1,
              "events": [{"stream": stream, "key": "k", "payload": {"ok": True}}]})
    _, committed = main.req("POST", f"/v1/transactions/{producer}/{tx_id}/commit",
                            {"epoch": 1})
    position = committed["commitPosition"]

    main.req("POST", "/v1/consumer-groups", {"name": group})
    main.req("POST", f"/v1/consumer-groups/{group}/ack",
             {"position": position + 1})

    proc = start_worker(port, "crash-during-reclaim", "/tmp/fault2.log")
    try:
        worker = Api(f"http://127.0.0.1:{port}")
        check("reclaim fault worker ready", worker.wait_ready())
        crashed = False
        try:
            worker.req("POST", "/v1/retention/reclaim",
                       {"faultToken": FAULT_TOKEN}, raw=True)
        except (httpx.TransportError, httpx.RemoteProtocolError):
            crashed = True
        check("reclaim request died mid-sweep", crashed)
        check("reclaim worker terminated", wait_exit(proc) is not None)

        # The sweep must have rolled back atomically: envelope fully present,
        # no half-reclaimed state, group ack intact.
        _, tx = main.req("GET", f"/v1/transactions/{producer}/{tx_id}")
        check("envelope survived crashed reclaim intact",
              tx["status"] == "committed" and tx["commitPosition"] == position)
        _, snap = main.req("POST", "/v1/snapshots",
                           {"streams": [stream], "ttlSeconds": 3600})
        ids = [t["txId"] for t in snap["page"]["transactions"]]
        check("events not partially deleted by crashed reclaim", tx_id in ids)
        main.req("DELETE", f"/v1/snapshots/{snap['snapshotId']}")
        _, grp = main.req("GET", f"/v1/consumer-groups/{group}")
        check("ack state survived crashed reclaim", grp["ackPosition"] == position + 1)
    finally:
        if proc.poll() is None:
            proc.kill()

    # A normal reclaim on a healthy instance now completes and removes the
    # envelope as a whole; it must not resurrect later.
    _, body = main.req("POST", "/v1/retention/reclaim", {})
    check("subsequent reclaim reports deletion",
          position in body["positions"], f"positions={body['positions']}")
    sc, err = main.req("GET", f"/v1/transactions/{producer}/{tx_id}",
                       expect_error=True)
    check("reclaimed envelope is gone (404)", sc == 404)
    _, body2 = main.req("POST", "/v1/retention/reclaim", {})
    check("reclaimed envelope does not resurrect", position not in body2["positions"])
    main.req("DELETE", f"/v1/consumer-groups/{group}")


def scenario_cursor_across_instances() -> None:
    print("\n=== Stage 4: cursor continuation on a different healthy instance ===")
    main = Api(BASE_URL)
    port = 8092
    producer = f"multiprod_{uuid.uuid4().hex[:10]}"
    stream = f"multistream_{uuid.uuid4().hex[:10]}"
    main.req("POST", "/v1/producers", {"name": producer, "epoch": 1})
    positions = []
    for i in range(4):
        tx_id = f"t{i}_{uuid.uuid4().hex[:8]}"
        main.req("POST", "/v1/transactions",
                 {"producer": producer, "txId": tx_id, "epoch": 1})
        main.req("PUT", f"/v1/transactions/{producer}/{tx_id}/batch",
                 {"epoch": 1, "firstSequence": i + 1,
                  "events": [{"stream": stream, "key": f"k{i}", "payload": {"i": i}}]})
        _, b = main.req("POST", f"/v1/transactions/{producer}/{tx_id}/commit",
                        {"epoch": 1})
        positions.append(b["commitPosition"])

    _, snap = main.req("POST", "/v1/snapshots",
                       {"streams": [stream], "ttlSeconds": 3600, "pageSize": 2})
    snapshot_id = snap["snapshotId"]
    try:
        proc = start_worker(port, "", "/tmp/worker3.log")
        try:
            other = Api(f"http://127.0.0.1:{port}")
            check("second instance ready", other.wait_ready())
            seen = [t["commitPosition"] for t in snap["page"]["transactions"]]
            cursor = snap["page"]["nextCursor"]
            has_more = snap["page"]["hasMore"]
            while has_more:
                _, page_body = other.req("GET", "/v1/streams/read",
                                         params={"cursor": cursor, "pageSize": 2})
                page = page_body["page"]
                seen.extend(t["commitPosition"] for t in page["transactions"])
                cursor = page["nextCursor"]
                has_more = page["hasMore"]
            check("cross-instance continuation no dup/gap", seen == positions,
                  f"seen={seen} expected={positions}")
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=10)
    finally:
        main.req("DELETE", f"/v1/snapshots/{snapshot_id}")


def scenario_idle_group_expiry() -> None:
    print("\n=== Stage 5: idle consumer group invalidation is durable, no in-memory state ===")
    # A worker configured with an idle TTL of 100 seconds; time is advanced
    # deterministically via X-Now. Restarting the worker must not change the
    # expiry decision because it is recomputed from stored last_ack_at.
    main = Api(BASE_URL)
    port = 8093
    group = f"idlegrp_{uuid.uuid4().hex[:10]}"
    proc = start_worker(port, "", "/tmp/worker5.log",
                        {"CONSUMER_GROUP_IDLE_TTL_SECONDS": "100"})
    try:
        worker = Api(f"http://127.0.0.1:{port}", now=1_900_000_000.0)
        check("idle-TTL worker ready", worker.wait_ready())
        worker.req("POST", "/v1/consumer-groups", {"name": group})
        worker.req("POST", f"/v1/consumer-groups/{group}/ack", {"position": 1})
        # 99 seconds idle: still active.
        worker.now += 99
        _, g = worker.req("GET", f"/v1/consumer-groups/{group}")
        check("group active before idle horizon", g["ackPosition"] == 1)
        # Past 100 seconds, a reclaim path invalidates it durably.
        worker.now += 2
        sc, err = worker.req("POST", "/v1/retention/reclaim", {}, expect_error=True)
        # No groups remain after idle deletion (the group itself was the only
        # one), so reject policy returns RETENTION_NOTHING -- which proves the
        # group was deleted. Direct GET confirms 404.
        check("reclaim observed group-less state after idle expiry",
              sc == 409 and err["code"] == "RETENTION_NOTHING", f"{sc} {err}")
        sc, err = worker.req("GET", f"/v1/consumer-groups/{group}", expect_error=True)
        check("idle group invalidated (404) purely from stored timestamps",
              sc == 404 and err["code"] == "CONSUMER_GROUP_NOT_FOUND")
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=10)


def scenario_no_group_delete_policy() -> None:
    print("\n=== Stage 6: no-group policy=delete uses a time horizon ===")
    main = Api(BASE_URL, now=1_900_000_000.0)
    port = 8094
    proc = start_worker(port, "", "/tmp/worker6.log", {
        "RETENTION_NO_GROUP_POLICY": "delete",
        "RETENTION_NO_GROUP_HORIZON_SECONDS": "100",
    })
    try:
        worker = Api(f"http://127.0.0.1:{port}", now=1_900_000_200.0)
        check("no-group worker ready", worker.wait_ready())

        # Commit at base time through the main API (deterministic t0).
        producer = f"ngprod_{uuid.uuid4().hex[:10]}"
        stream = f"ngstream_{uuid.uuid4().hex[:10]}"
        main.req("POST", "/v1/producers", {"name": producer, "epoch": 1})
        tx_id = f"t_{uuid.uuid4().hex[:10]}"
        main.req("POST", "/v1/transactions",
                 {"producer": producer, "txId": tx_id, "epoch": 1})
        main.req("PUT", f"/v1/transactions/{producer}/{tx_id}/batch",
                 {"epoch": 1, "firstSequence": 1,
                  "events": [{"stream": stream, "key": "k", "payload": {}}]})
        _, committed = main.req("POST", f"/v1/transactions/{producer}/{tx_id}/commit",
                                {"epoch": 1})
        position = committed["commitPosition"]

        # There must be no groups in the deployment at this point (suite and
        # earlier stages clean them up).
        _, listing = worker.req("GET", "/v1/consumer-groups")
        no_groups = len(listing["consumerGroups"]) == 0
        check("no consumer groups exist for delete-policy stage", no_groups,
              f"groups={[g['name'] for g in listing['consumerGroups']][:5]}")

        # Worker time is t0+200 with horizon 100: envelope qualifies.
        _, body = worker.req("POST", "/v1/retention/reclaim", {})
        check("delete policy reclaimed aged envelope without groups",
              position in body["positions"], f"positions={body['positions']}")
        sc, _ = main.req("GET", f"/v1/transactions/{producer}/{tx_id}",
                         expect_error=True)
        check("aged envelope gone after policy=delete reclaim", sc == 404)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=10)


def main_entry() -> int:
    main = Api(BASE_URL)
    if not main.wait_ready():
        print("API never became ready; aborting")
        return 1

    ok_suite = run_pytest()
    scenario_crash_after_commit()
    scenario_crash_during_reclaim()
    scenario_cursor_across_instances()
    scenario_idle_group_expiry()
    scenario_no_group_delete_policy()

    print("\n================ ACCEPTANCE SUMMARY ================")
    if ok_suite and not FAILURES:
        print("ALL ACCEPTANCE CHECKS PASSED")
        return 0
    print(f"{len(FAILURES)} FAILURE(S):")
    for f in FAILURES:
        print(" -", f)
    return 1


if __name__ == "__main__":
    sys.exit(main_entry())
