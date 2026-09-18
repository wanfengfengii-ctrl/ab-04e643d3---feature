"""Shared pytest fixtures for black-box contract tests against a live API.

Tests speak only HTTP (never touch the database) so they verify the real
network contract. Deterministic time is supplied through the authenticated
``X-Now`` header -- no sleeps.
"""
from __future__ import annotations

import itertools
import os
import time
import uuid

import httpx
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080").rstrip("/")
CLOCK_TOKEN = os.environ.get("CLOCK_OVERRIDE_TOKEN", "acceptance-clock")
FAULT_TOKEN_VALUE = os.environ.get("FAULT_TOKEN", "acceptance-fault")

# A fixed, far-future-ish base so TTL math is exact.
BASE_NOW = 1_900_000_000.0


class Client:
    def __init__(self) -> None:
        self.http = httpx.Client(base_url=BASE_URL, timeout=30.0)
        self.now = BASE_NOW
        self._seq = itertools.count(1)
        self.tracked_snapshots: list[str] = []

    # -- deterministic clock -------------------------------------------------
    def headers(self) -> dict:
        return {"X-Now": f"{self.now:.3f}", "X-Now-Token": CLOCK_TOKEN}

    def advance(self, seconds: float) -> None:
        self.now += seconds

    # -- low level -----------------------------------------------------------
    def request(self, method: str, path: str, json=None, params=None,
                headers=None, expect_error=False):
        h = dict(self.headers())
        if headers:
            h.update(headers)
        resp = self.http.request(method, path, json=json, params=params, headers=h)
        if expect_error:
            assert resp.status_code >= 400, (
                f"expected error, got {resp.status_code}: {resp.text}")
            return resp.status_code, resp.json()["error"]
        assert resp.status_code < 400, (
            f"{method} {path} failed: {resp.status_code} {resp.text}")
        return resp.status_code, resp.json()

    def raw(self, method: str, path: str, **kw):
        headers = kw.pop("headers", None) or {}
        h = dict(self.headers())
        h.update(headers)
        return self.http.request(method, path, headers=h, **kw)

    # -- domain helpers ------------------------------------------------------
    def unique(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def register_producer(self, name=None, epoch=1, expect_error=False):
        name = name or self.unique("prod")
        sc, body = self.request("POST", "/v1/producers",
                                {"name": name, "epoch": epoch},
                                expect_error=expect_error)
        return (sc, body) if expect_error else (name, body)

    def create_tx(self, producer, tx_id=None, epoch=1, expect_error=False):
        tx_id = tx_id or self.unique("tx")
        sc, body = self.request("POST", "/v1/transactions",
                               {"producer": producer, "txId": tx_id, "epoch": epoch},
                               expect_error=expect_error)
        return (sc, body) if expect_error else (tx_id, body)

    def write(self, producer, tx_id, epoch, first_sequence, events, expect_error=False):
        return self.request(
            "PUT", f"/v1/transactions/{producer}/{tx_id}/batch",
            {"epoch": epoch, "firstSequence": first_sequence, "events": events},
            expect_error=expect_error)

    def commit(self, producer, tx_id, epoch, fault_token=None, expect_error=False):
        body = {"epoch": epoch}
        if fault_token:
            body["faultToken"] = fault_token
        return self.request(
            "POST", f"/v1/transactions/{producer}/{tx_id}/commit", body,
            expect_error=expect_error)

    def abort(self, producer, tx_id, epoch, expect_error=False):
        return self.request(
            "POST", f"/v1/transactions/{producer}/{tx_id}/abort", {"epoch": epoch},
            expect_error=expect_error)

    def get_tx(self, producer, tx_id):
        return self.request("GET", f"/v1/transactions/{producer}/{tx_id}")

    def snapshot(self, streams, ttl=3600, page_size=100):
        sc, body = self.request("POST", "/v1/snapshots", {
            "streams": streams, "ttlSeconds": ttl, "pageSize": page_size})
        self.tracked_snapshots.append(body["snapshotId"])
        return sc, body

    def read_page(self, cursor, page_size=None, expect_error=False):
        params = {"cursor": cursor}
        if page_size is not None:
            params["pageSize"] = page_size
        return self.request("GET", "/v1/streams/read", params=params,
                            expect_error=expect_error)

    def make_group(self, name=None):
        name = name or self.unique("grp")
        _, body = self.request("POST", "/v1/consumer-groups", {"name": name})
        return name, body

    def ack(self, name, position, expect_error=False):
        return self.request("POST", f"/v1/consumer-groups/{name}/ack",
                            {"position": position}, expect_error=expect_error)

    def reclaim(self, fault_token=None, expect_error=False):
        body = {}
        if fault_token:
            body["faultToken"] = fault_token
        return self.request("POST", "/v1/retention/reclaim", body,
                            expect_error=expect_error)

    def read_all(self, snapshot_body):
        """Drain a snapshot from its first page to the end."""
        txs = list(snapshot_body["page"]["transactions"])
        cursor = snapshot_body["page"]["nextCursor"]
        has_more = snapshot_body["page"]["hasMore"]
        while has_more:
            _, body = self.read_page(cursor)
            page = body["page"]
            txs.extend(page["transactions"])
            cursor = page["nextCursor"]
            has_more = page["hasMore"]
        return txs, cursor


@pytest.fixture(scope="session")
def client() -> Client:
    c = Client()
    # Wait for readiness.
    deadline = time.time() + 60
    while True:
        try:
            r = c.http.get("/health/ready", timeout=2)
            if r.status_code == 200 and r.json()["database"] == "up":
                break
        except httpx.TransportError:
            pass
        if time.time() > deadline:
            raise RuntimeError("API did not become ready")
        time.sleep(1)
    yield c
    c.http.close()


@pytest.fixture(autouse=True)
def _isolate(client: Client):
    """Reset the deterministic clock and purge global shared state
    (consumer groups, snapshots) around every test. Producers/streams use
    unique names, so committed history never collides between tests.

    Runs with real wall-clock headers (X-Now at BASE_NOW) so the DELETE
    admin calls are never themselves expired."""
    client.now = BASE_NOW
    yield
    client.now = BASE_NOW
    for sid in list(client.tracked_snapshots):
        try:
            client.request("DELETE", f"/v1/snapshots/{sid}")
        except AssertionError:
            pass
    client.tracked_snapshots.clear()
    try:
        _, groups = client.request("GET", "/v1/consumer-groups")
        for g in groups.get("consumerGroups", []):
            client.request("DELETE", f"/v1/consumer-groups/{g['name']}")
    except AssertionError:
        pass
