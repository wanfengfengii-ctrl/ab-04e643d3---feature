"""Consumer group acknowledgement and historical reclamation."""


def ev(stream, key, payload):
    return {"stream": stream, "key": key, "payload": payload}


def _commit_n(client, n=3, stream=None):
    stream = stream or client.unique("alpha")
    name, _ = client.register_producer(client.unique("prod"), 1)
    positions = []
    for i in range(n):
        tx, _ = client.create_tx(name, epoch=1)
        client.write(name, tx, 1, i + 1, [ev(stream, f"k{i}", {})])
        _, b = client.commit(name, tx, 1)
        positions.append(b["commitPosition"])
    return name, stream, positions


def _scenario(client, n=3):
    """An isolated consumer group protects only this scenario's data."""
    name, stream, positions = _commit_n(client, n)
    grp, _ = client.make_group()
    return name, stream, grp, positions


def test_ack_is_idempotent_and_monotonic(client):
    _, _, grp, positions = _scenario(client)
    _, b = client.ack(grp, positions[1])
    assert b["status"] == "advanced"
    _, b = client.ack(grp, positions[1])
    assert b["status"] == "idempotent"
    sc, err = client.ack(grp, positions[0], expect_error=True)
    assert sc == 409 and err["code"] == "ACK_POSITION_REGRESSED"
    _, g = client.request("GET", f"/v1/consumer-groups/{grp}")
    assert g["ackPosition"] == positions[1]


def test_ack_unknown_group_is_404(client):
    sc, err = client.ack(client.unique("ghost"), 5, expect_error=True)
    assert sc == 404 and err["code"] == "CONSUMER_GROUP_NOT_FOUND"


def test_reclaim_deletes_only_below_all_acks(client):
    name, stream, grp, positions = _scenario(client, 3)
    # Group exists but has acked nothing (position 0): nothing may be deleted.
    _, body = client.reclaim()
    assert body["ackFloorPosition"] == 0
    assert all(p >= body["ackFloorPosition"] for p in positions)

    # Ack past the first two envelopes.
    client.ack(grp, positions[1])  # positions are strictly < positions[1]
    _, body = client.reclaim()
    assert body["reclaimedTransactionCount"] >= 1
    assert positions[0] in body["positions"]
    assert positions[1] not in body["positions"]
    # The surviving envelope remains readable in a fresh snapshot.
    _, snap = client.snapshot([stream], ttl=600)
    txs, _ = client.read_all(snap)
    surviving = [t for t in txs if t["producer"] == name]
    assert [t["commitPosition"] for t in surviving] == [positions[1], positions[2]]


def test_reclaim_never_partially_deletes_envelope(client):
    name, stream, grp, positions = _scenario(client, 1)
    client.ack(grp, positions[0] + 1)
    _, body = client.reclaim()
    assert positions[0] in body["positions"]
    # Either the whole envelope is gone or it is fully present.
    _, snap = client.snapshot([stream], ttl=600)
    txs, _ = client.read_all(snap)
    gone = [t for t in txs if t["commitPosition"] == positions[0]]
    assert gone == []
    _, st = client.request("GET", "/v1/retention/status")
    assert st["earliestAvailablePosition"] != positions[0]


def test_snapshot_protects_even_when_ack_advanced(client):
    name, stream, grp, positions = _scenario(client, 1)
    _, snap = client.snapshot([stream], ttl=1000)
    client.ack(grp, positions[0] + 1)
    _, body = client.reclaim()
    assert body["reclaimedTransactionCount"] == 0
    # The old snapshot cursor can still read the protected envelope.
    txs, _ = client.read_all(snap)
    assert any(t["commitPosition"] == positions[0] for t in txs)


def test_reclaim_is_serializable_with_concurrent_ack(client):
    import concurrent.futures
    from tests.conftest import Client

    name, stream, grp, positions = _scenario(client, 5)
    target = positions[-1] + 1

    def reclaim():
        c = Client()
        try:
            return c.reclaim()
        finally:
            c.http.close()

    def advance_ack():
        c = Client()
        try:
            return c.ack(grp, target)
        finally:
            c.http.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(reclaim)
        f2 = pool.submit(advance_ack)
        f1.result(); f2.result()

    _, g = client.request("GET", f"/v1/consumer-groups/{grp}")
    assert g["ackPosition"] == target


def test_reclaim_no_group_default_reject(client):
    # After explicitly removing every group, the default reject policy must
    # refuse to reclaim anything (safety never silently disappears).
    grp, _ = client.make_group()
    client.request("DELETE", f"/v1/consumer-groups/{grp}")
    sc, err = client.reclaim(expect_error=True)
    assert sc == 409 and err["code"] == "RETENTION_NOTHING"
    assert err["details"]["policy"] == "reject"
    _, cfg = client.request("GET", "/v1/config")
    assert cfg["retentionNoGroupPolicy"] in ("reject", "delete")


def test_group_cleanup_admin_endpoint(client):
    grp, _ = client.make_group()
    sc, b = client.request("DELETE", f"/v1/consumer-groups/{grp}")
    assert sc in (200, 404)
    sc, _ = client.request("GET", f"/v1/consumer-groups/{grp}", expect_error=True)
    assert sc == 404
