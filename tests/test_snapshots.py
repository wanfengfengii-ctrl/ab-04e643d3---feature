"""Multi-stream read snapshots, opaque cursors and TTL expiry."""


def ev(stream, key, payload):
    return {"stream": stream, "key": key, "payload": payload}


def _commit(client, name, seq, events, epoch=1):
    tx, _ = client.create_tx(name, epoch=epoch)
    client.write(name, tx, epoch, seq, events)
    _, b = client.commit(name, tx, epoch)
    return tx, b["commitPosition"]


def _producer_with_n(client, n, streams=None):
    if streams is None:
        streams = (client.unique("alpha"), client.unique("beta"))
    name, _ = client.register_producer(client.unique("prod"), 1)
    positions = []
    next_seq = 1
    for i in range(n):
        events = [ev(s, f"k{i}", {"i": i, "s": s}) for s in streams]
        _, pos = _commit(client, name, next_seq, events)
        next_seq += len(events)
        positions.append(pos)
    return name, streams, positions


def test_snapshot_fixes_high_watermark(client):
    name, streams, positions = _producer_with_n(client, 3)
    _, snap = client.snapshot(list(streams), ttl=600)
    hw = snap["highWatermark"]
    assert hw == positions[-1]
    # A commit after snapshot creation never enters the existing snapshot.
    _, pos_late = _commit(client, name, 7, [ev(streams[0], "late", {})])
    assert pos_late > hw
    txs, _ = client.read_all(snap)
    assert [t["commitPosition"] for t in txs] == positions


def test_pages_are_whole_envelopes_in_strict_position_order(client):
    _, streams, positions = _producer_with_n(client, 6)
    _, snap = client.snapshot(list(streams), ttl=600, page_size=2)
    page = snap["page"]
    assert page["transactionCount"] == 2
    seen = [t["commitPosition"] for t in page["transactions"]]
    assert seen == positions[:2]
    all_txs, _ = client.read_all(snap)
    assert [t["commitPosition"] for t in all_txs] == positions
    for t in all_txs:
        # each envelope has both selected streams' complete subset
        assert {e["stream"] for e in t["events"]} == set(streams)


def test_stream_filtering_only_matching_envelopes(client):
    sa, sb, sg = client.unique("a"), client.unique("b"), client.unique("g")
    name, _ = client.register_producer(client.unique("prod"), 1)
    _commit(client, name, 1, [ev(sa, "a", {})])
    _commit(client, name, 2, [ev(sb, "b", {})])
    _commit(client, name, 3, [ev(sa, "a2", {}), ev(sg, "g", {})])

    _, snap = client.snapshot([sa], ttl=600)
    txs, _ = client.read_all(snap)
    mine = [t for t in txs if t["producer"] == name]
    assert len(mine) == 2
    for t in mine:
        assert all(e["stream"] == sa for e in t["events"])
    assert {t["events"][0]["key"] for t in mine} == {"a", "a2"}


def test_cursor_continuation_no_dup_or_gap_across_pages(client):
    _, streams, positions = _producer_with_n(client, 10)
    _, snap = client.snapshot([streams[0]], ttl=600, page_size=3)
    # Start from the first page already returned by snapshot creation.
    collected = [t["commitPosition"] for t in snap["page"]["transactions"]]
    cursor = snap["page"]["nextCursor"]
    has_more = snap["page"]["hasMore"]
    page_no = 1
    while has_more:
        _, body = client.read_page(cursor, page_size=3)
        page = body["page"]
        assert page["transactionCount"] <= 3
        collected.extend(t["commitPosition"] for t in page["transactions"])
        cursor = page["nextCursor"]
        has_more = page["hasMore"]
        page_no += 1
    # One extra continuation past the end must return a stable empty page.
    _, tail = client.read_page(cursor, page_size=3)
    assert tail["page"]["transactions"] == []
    assert tail["page"]["hasMore"] is False
    assert collected == positions
    assert page_no == 4  # ceil(10/3) = 4 pages


def test_cursor_is_tamper_evident(client):
    _, streams, _ = _producer_with_n(client, 1)
    _, snap = client.snapshot([streams[0]], ttl=600)
    cursor = snap["page"]["nextCursor"]
    tampered = ("A" if cursor[0] != "A" else "B") + cursor[1:]
    sc, err = client.read_page(tampered, expect_error=True)
    assert sc == 400 and err["code"] == "CURSOR_INVALID"
    sc, err = client.read_page("not-a-cursor", expect_error=True)
    assert sc == 400 and err["code"] == "CURSOR_INVALID"


def test_cursor_bound_to_stream_set(client):
    sa, sb = client.unique("a"), client.unique("b")
    name, _ = client.register_producer(client.unique("prod"), 1)
    _commit(client, name, 1, [ev(sa, "a", {}), ev(sb, "b", {})])
    _, snap_a = client.snapshot([sa], ttl=600)
    _, snap_b = client.snapshot([sb], ttl=600)
    # Alpha's signed cursor only ever yields alpha events.
    _, body = client.read_page(snap_a["page"]["nextCursor"])
    for t in body["page"]["transactions"]:
        if t["producer"] == name:
            assert {e["stream"] for e in t["events"]} == {sa}
    # Rewriting the bound stream set inside the cursor invalidates its HMAC.
    import base64
    import hashlib
    import hmac as _hmac
    import json as _json
    from app.validation import canonical_json
    blob = base64.urlsafe_b64decode(snap_a["page"]["nextCursor"].encode())
    payload, sig = blob[:-32], blob[-32:]
    doc = _json.loads(payload)
    doc["streams"] = [sb]
    forged = base64.urlsafe_b64encode(
        canonical_json(doc).encode() + sig).decode()
    sc, err = client.read_page(forged, expect_error=True)
    assert sc == 400 and err["code"] == "CURSOR_INVALID"
    # Beta's own snapshot works independently.
    assert snap_b["streams"] == [sb]


def test_cursor_expired_after_ttl_returns_410_with_earliest(client):
    sa = client.unique("a")
    name, _ = client.register_producer(client.unique("prod"), 1)
    _commit(client, name, 1, [ev(sa, "a", {})])
    _, snap = client.snapshot([sa], ttl=100)
    cursor = snap["page"]["nextCursor"]
    client.advance(101)
    sc, err = client.read_page(cursor, expect_error=True)
    assert sc == 410 and err["code"] == "CURSOR_EXPIRED"
    assert "earliestAvailablePosition" in err["details"]
    assert "createSnapshot" in err["details"]
    # It must NOT silently continue from the earliest surviving record.
    sc2, err2 = client.read_page(cursor, expect_error=True)
    assert sc2 == 410


def test_expired_snapshot_does_not_block_retention(client):
    sa = client.unique("a")
    name, _ = client.register_producer(client.unique("prod"), 1)
    _, pos = _commit(client, name, 1, [ev(sa, "a", {})])
    grp, _ = client.make_group()
    client.snapshot([sa], ttl=100)
    client.advance(101)
    client.ack(grp, pos + 1)
    _, body = client.reclaim()
    assert pos in body["positions"]
    assert body["expiredSnapshotCount"] >= 1


def test_live_snapshot_protects_history_from_retention(client):
    sa = client.unique("a")
    name, _ = client.register_producer(client.unique("prod"), 1)
    _, pos = _commit(client, name, 1, [ev(sa, "a", {})])
    grp, _ = client.make_group()
    _, snap = client.snapshot([sa], ttl=1000)
    client.ack(grp, pos + 1)
    _, body = client.reclaim()
    assert pos not in body["positions"]
    assert body["reclaimedTransactionCount"] == 0 or all(
        p != pos for p in body["positions"])
    _, st = client.request("GET", "/v1/retention/status")
    assert st["liveSnapshotCount"] >= 1
    # The protected envelope is still readable through the live snapshot.
    txs, _ = client.read_all(snap)
    assert any(t["commitPosition"] == pos for t in txs)


def test_snapshot_empty_for_unused_stream(client):
    s = client.unique("never-used")
    _, snap = client.snapshot([s], ttl=600)
    assert snap["page"]["transactions"] == []
    assert snap["page"]["hasMore"] is False
    # Continuation stays empty; high watermark is global, not per-stream.
    _, body = client.read_page(snap["page"]["nextCursor"])
    assert body["page"]["transactions"] == []
    assert body["page"]["hasMore"] is False


def test_cursor_exact_ttl_boundary(client):
    sa = client.unique("a")
    name, _ = client.register_producer(client.unique("prod"), 1)
    _commit(client, name, 1, [ev(sa, "a", {})])
    _, snap = client.snapshot([sa], ttl=100)
    cursor = snap["page"]["nextCursor"]
    # One second before expiry: still readable.
    client.advance(99)
    _, ok_body = client.read_page(cursor)
    assert ok_body["highWatermark"] == snap["highWatermark"]
    # Exactly at expiry instant: expired.
    client.advance(1)
    sc, err = client.read_page(cursor, expect_error=True)
    assert sc == 410 and err["code"] == "CURSOR_EXPIRED"


def test_deleted_snapshot_cursor_returns_410_not_invalid(client):
    sa = client.unique("a")
    _, snap = client.snapshot([sa], ttl=600)
    cursor = snap["page"]["nextCursor"]
    sc, _ = client.request("DELETE", f"/v1/snapshots/{snap['snapshotId']}")
    assert sc in (200, 404)
    client.tracked_snapshots.remove(snap["snapshotId"])
    sc, err = client.read_page(cursor, expect_error=True)
    assert sc == 410 and err["code"] == "CURSOR_EXPIRED"
    assert isinstance(err["details"]["earliestAvailablePosition"], int)
    assert err["details"]["createSnapshot"]["path"] == "/v1/snapshots"


def test_clock_override_requires_correct_token(client):
    sa = client.unique("a")
    _, snap = client.snapshot([sa], ttl=600)
    resp = client.raw(
        "GET", "/v1/streams/read",
        params={"cursor": snap["page"]["nextCursor"]},
        headers={"X-Now": "1", "X-Now-Token": "wrong-token"})
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "CLOCK_OVERRIDE_FORBIDDEN"
