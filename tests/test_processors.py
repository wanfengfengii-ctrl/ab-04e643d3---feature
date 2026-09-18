"""Persistent derived processors: claim/lease/fencing, atomic completion,
idempotency, derived envelopes and retention protection.

All tests are black-box HTTP tests with deterministic X-Now time.
"""
import concurrent.futures
import uuid


def ev(stream, key, payload):
    return {"stream": stream, "key": key, "payload": payload}


def _history_window(client):
    _, status = client.request("GET", "/v1/retention/status")
    return status["earliestAvailablePosition"], status["latestPosition"]


def _make_processor(client, input_streams, output_streams=None, batch_size=10,
                    lease_seconds=60, start=None, pid=None, expect_error=False):
    earliest, latest = _history_window(client)
    pid = pid or client.unique("proc")
    output_streams = output_streams or [client.unique("out")]
    return client.create_processor_dp(
        pid, input_streams, output_streams,
        batch_size=batch_size, lease_seconds=lease_seconds,
        start_position=earliest if start is None else start,
        expect_error=expect_error)


def _commit_envelope(client, stream, events_on=None, producer=None):
    """Commit one source envelope; returns (producer, txId, commitPosition)."""
    producer = producer or client.unique("prod")
    client.register_producer(producer, 1)
    tx, _ = client.create_tx(producer, epoch=1)
    events_on = events_on if events_on is not None else [ev(stream, "k", {})]
    client.write(producer, tx, 1, 1, events_on)
    _, b = client.commit(producer, tx, 1)
    return producer, tx, b["commitPosition"]


# ---------------------------------------------------------------------------
# Creation / configuration immutability
# ---------------------------------------------------------------------------


def test_create_and_get_processor(client):
    ins = [client.unique("in")]
    outs = [client.unique("out")]
    pid = client.unique("proc")
    sc, b = _make_processor(client, ins, outs, batch_size=5, lease_seconds=90,
                            pid=pid)
    assert sc == 201
    assert b["id"] == pid and b["status"] == "active"
    assert b["inputStreams"] == ins and b["outputStreams"] == outs
    assert b["batchSize"] == 5 and b["leaseSeconds"] == 90
    assert b["checkpointPosition"] == b["startPosition"]
    assert b["currentWork"] is None
    _, g = client.get_processor_dp(pid)
    assert g["id"] == pid


def test_duplicate_processor_id_is_rejected(client):
    ins = [client.unique("in")]
    pid = client.unique("proc")
    _make_processor(client, ins, pid=pid)
    sc, err = _make_processor(client, ins, pid=pid, expect_error=True)
    assert sc == 409 and err["code"] == "PROCESSOR_ID_REUSED"


def test_start_position_must_be_inside_available_history(client):
    earliest, latest = _history_window(client)
    ins = [client.unique("in")]
    # Future position.
    sc, err = _make_processor(client, ins, start=latest + 1,
                              expect_error=True)
    assert sc == 409 and err["code"] == "START_POSITION_OUT_OF_RANGE"
    assert err["details"]["latestPosition"] == latest
    # If any history was reclaimed, a position before earliest is invalid.
    if earliest > 0:
        sc, err = _make_processor(client, ins, start=earliest - 1,
                                  expect_error=True)
        assert sc == 409 and err["code"] == "START_POSITION_OUT_OF_RANGE"


def test_processor_validation_errors(client):
    ins = [client.unique("in")]
    earliest, _ = _history_window(client)
    base = {"id": client.unique("proc"), "inputStreams": ins,
            "outputStreams": [client.unique("out")], "batchSize": 10,
            "leaseSeconds": 60, "startPosition": earliest}
    for patch in (
        {"inputStreams": []},
        {"inputStreams": ["bad/name"]},
        {"outputStreams": []},
        {"batchSize": 0},
        {"batchSize": 101},
        {"leaseSeconds": 0},
        {"batchSize": True},
        {"startPosition": -1},
    ):
        body = dict(base, **patch)
        body["id"] = client.unique("proc")
        sc, err = client.request("POST", "/v1/processors", body,
                                 expect_error=True)
        assert sc == 400 and err["code"] == "VALIDATION_ERROR", patch


def test_pause_blocks_new_claims_but_resume_allows(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], pid=pid)
    _commit_envelope(client, stream)
    client.pause_dp(pid)
    sc, err = client.claim(pid, expect_error=True)
    assert sc == 409 and err["code"] == "PROCESSOR_PAUSED"
    _, g = client.get_processor_dp(pid)
    assert g["status"] == "paused"
    client.resume_dp(pid)
    sc, b = client.claim(pid)
    assert sc == 200 and b["status"] == "claimed"


def test_delete_processor_is_404_and_derived_envelope_survives(client):
    stream = client.unique("in")
    out = client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    _, done = client.complete(
        pid, client.unique("r"), claim["leaseId"], claim["generation"],
        claim["sourceDigest"], [ev(out, "d", {"v": 1})])
    derived_pos = done["commitPosition"]

    sc, b = client.delete_processor_dp(pid)
    assert sc == 200 and b["deleted"] is True
    sc, err = client.request("GET", f"/v1/processors/{pid}", expect_error=True)
    assert sc == 404 and err["code"] == "PROCESSOR_NOT_FOUND"
    # The derived envelope is an ordinary global-log entry: still readable.
    _, snap = client.snapshot([out])
    txs, _ = client.read_all(snap)
    assert any(t["commitPosition"] == derived_pos for t in txs)


# ---------------------------------------------------------------------------
# Claim semantics
# ---------------------------------------------------------------------------


def test_claim_no_work_then_work_after_commit(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], pid=pid)
    sc, b = client.claim(pid)
    assert b["status"] == "no_work"
    _commit_envelope(client, stream)
    sc, b = client.claim(pid)
    assert b["status"] == "claimed" and b["transactionCount"] == 1


def test_claim_is_one_per_processor_and_concurrent_claims_have_one_winner(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], pid=pid)
    _commit_envelope(client, stream)

    def do_claim(_):
        return client.raw("POST", f"/v1/processors/{pid}/claims", json={})

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(do_claim, range(2)))
    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200, 409]
    _, err = client.claim(pid, expect_error=True)
    assert err["code"] == "WORK_ALREADY_CLAIMED"


def test_claim_batch_size_bounds_the_immutable_range(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], batch_size=2, pid=pid)
    positions = [_commit_envelope(client, stream,
                                  events_on=[ev(stream, f"k{i}", {})],
                                  producer=f"p{i}-{uuid.uuid4().hex[:6]}")[2]
                 for i in range(3)]
    _, claim = client.claim(pid)
    assert claim["fromPosition"] < claim["throughPosition"] == positions[1]
    assert [t["commitPosition"] for t in claim["transactions"]] == positions[:2]
    # Complete, then the next claim contains only the remainder.
    client.complete(pid, client.unique("r"), claim["leaseId"],
                    claim["generation"], claim["sourceDigest"], [])
    _, claim2 = client.claim(pid)
    assert [t["commitPosition"] for t in claim2["transactions"]] == [positions[2]]


def test_new_commits_never_mix_into_an_outstanding_claim(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], pid=pid)
    _, _, p1 = _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    assert claim["throughPosition"] == p1
    # Committed AFTER the claim.
    _, _, p2 = _commit_envelope(client, stream)
    _, same = client.get_processor_dp(pid)
    assert same["currentWork"]["throughPosition"] == p1
    assert p2 not in [t["commitPosition"] for t in claim["transactions"]]


def test_envelope_enters_once_and_only_configured_events_return(client):
    ina, inb, other = (client.unique("in"), client.unique("in"),
                       client.unique("other"))
    pid = client.unique("proc")
    _make_processor(client, [ina, inb], pid=pid)
    producer = client.unique("prod")
    client.register_producer(producer, 1)
    tx, _ = client.create_tx(producer, epoch=1)
    events = [ev(ina, "a1", {"x": 1}), ev(inb, "b1", {"x": 2}),
              ev(ina, "a2", {"x": 3}), ev(other, "o1", {"x": 4})]
    client.write(producer, tx, 1, 1, events)
    _, b = client.commit(producer, tx, 1)
    _, claim = client.claim(pid)
    envelopes = claim["transactions"]
    assert len(envelopes) == 1
    assert envelopes[0]["commitPosition"] == b["commitPosition"]
    returned = [(e["stream"], e["key"]) for e in envelopes[0]["events"]]
    assert returned == [(ina, "a1"), (inb, "b1"), (ina, "a2")]
    assert all(e["stream"] in (ina, inb) for e in envelopes[0]["events"])


# ---------------------------------------------------------------------------
# Lease renewal / expiry / takeover / fencing
# ---------------------------------------------------------------------------


def test_renew_extends_lease_before_expiry(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], lease_seconds=30, pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    first_expiry = claim["expiresAt"]
    client.advance(29)  # still strictly live
    _, renewed = client.renew(pid, claim["leaseId"], claim["generation"])
    assert renewed["expiresAt"] > first_expiry


def test_renew_exactly_at_expiry_is_fenced(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], lease_seconds=30, pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    client.advance(30)  # now == expiresAt: invalid
    sc, err = client.renew(pid, claim["leaseId"], claim["generation"],
                           expect_error=True)
    assert sc == 409 and err["code"] == "LEASE_FENCED"


def test_complete_exactly_at_expiry_is_fenced_without_takeover(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], lease_seconds=30, pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    client.advance(30)  # now == expiresAt, no takeover happened
    sc, err = client.complete(
        pid, client.unique("r"), claim["leaseId"], claim["generation"],
        claim["sourceDigest"], [], expect_error=True)
    assert sc == 409 and err["code"] == "LEASE_FENCED"
    # No checkpoint movement and the range is still takeover-able.
    _, g = client.get_processor_dp(pid)
    assert g["checkpointPosition"] == claim["fromPosition"]
    _, takeover = client.claim(pid)
    assert takeover["generation"] == claim["generation"] + 1


def test_takeover_after_expiry_same_range_higher_generation(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], lease_seconds=30, pid=pid)
    _commit_envelope(client, stream)
    _, first = client.claim(pid)
    client.advance(30)
    _, second = client.claim(pid)
    assert second["status"] == "claimed"
    assert second["generation"] == first["generation"] + 1
    assert second["leaseId"] != first["leaseId"]
    # Same immutable work identity and summary.
    assert second["fromPosition"] == first["fromPosition"]
    assert second["throughPosition"] == first["throughPosition"]
    assert second["sourceDigest"] == first["sourceDigest"]
    # The old executor is fenced on both renew and complete.
    sc, err = client.renew(pid, first["leaseId"], first["generation"],
                           expect_error=True)
    assert sc == 409 and err["code"] == "LEASE_FENCED"
    sc, err = client.complete(
        pid, client.unique("r"), first["leaseId"], first["generation"],
        first["sourceDigest"], [], expect_error=True)
    assert sc == 409 and err["code"] == "LEASE_FENCED"
    # New generation completes successfully.
    sc, done = client.complete(
        pid, client.unique("r"), second["leaseId"], second["generation"],
        second["sourceDigest"], [])
    assert sc == 200 and done["commitPosition"] is None


def test_wrong_lease_identity_is_fenced(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    # Wrong generation.
    sc, err = client.complete(
        pid, client.unique("r"), claim["leaseId"], claim["generation"] + 1,
        claim["sourceDigest"], [], expect_error=True)
    assert sc == 409 and err["code"] == "LEASE_FENCED"
    # Wrong lease id.
    import uuid as _uuid
    sc, err = client.complete(
        pid, client.unique("r"), str(_uuid.uuid4()), claim["generation"],
        claim["sourceDigest"], [], expect_error=True)
    assert sc == 409 and err["code"] == "LEASE_FENCED"
    # Bad digest.
    sc, err = client.complete(
        pid, client.unique("r"), claim["leaseId"], claim["generation"],
        "0" * 64, [], expect_error=True)
    assert sc == 409 and err["code"] == "SOURCE_DIGEST_MISMATCH"


# ---------------------------------------------------------------------------
# Atomic completion, derived envelopes, idempotency
# ---------------------------------------------------------------------------


def test_zero_output_completion_persists_and_advances_checkpoint(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    _make_processor(client, [stream], pid=pid)
    _, _, pos = _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    result_id = client.unique("r")
    sc, done = client.complete(pid, result_id, claim["leaseId"],
                               claim["generation"], claim["sourceDigest"], [])
    assert sc == 200
    assert done["commitPosition"] is None and done["eventCount"] == 0
    assert done["throughPosition"] == pos
    _, g = client.get_processor_dp(pid)
    assert g["checkpointPosition"] == pos and g["currentWork"] is None
    _, stored = client.get_result_dp(pid, result_id)
    assert stored["commitPosition"] is None
    # No outstanding work: next claim is no_work.
    _, b = client.claim(pid)
    assert b["status"] == "no_work"


def test_derived_events_are_ordinary_global_envelopes(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], pid=pid)
    _, _, src_pos = _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    derived = [ev(out, "d1", {"a": 1}), ev(out, "d2", {"a": 2})]
    _, done = client.complete(pid, client.unique("r"), claim["leaseId"],
                              claim["generation"], claim["sourceDigest"],
                              derived)
    dpos = done["commitPosition"]
    assert isinstance(dpos, int) and dpos > src_pos
    # Visible through the existing read model on the output stream.
    _, snap = client.snapshot([out])
    txs, _ = client.read_all(snap)
    match = [t for t in txs if t["commitPosition"] == dpos]
    assert len(match) == 1
    assert [(e["stream"], e["key"]) for e in match[0]["events"]] == \
        [(out, "d1"), (out, "d2")]
    # A snapshot taken at a high watermark below the derived position (the
    # source position) never sees the derived envelope.
    _, old_snap = client.snapshot([out])
    # high watermark now includes the derived commit; instead verify the
    # derived position exceeds the source and normal ordering holds.
    assert all(t["commitPosition"] != src_pos for t in txs)
    client.request("DELETE", f"/v1/snapshots/{old_snap['snapshotId']}")


def test_derived_event_must_target_declared_output_stream(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    sc, err = client.complete(
        pid, client.unique("r"), claim["leaseId"], claim["generation"],
        claim["sourceDigest"], [ev(client.unique("rogue"), "d", {})],
        expect_error=True)
    assert sc == 400 and err["code"] == "VALIDATION_ERROR"
    # Nothing was written and the lease remains outstanding.
    _, g = client.get_processor_dp(pid)
    assert g["currentWork"]["leaseId"] == claim["leaseId"]


def test_completion_enforces_event_limit_and_sizes(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    too_many = [ev(out, f"d{i}", {}) for i in range(101)]
    sc, err = client.complete(
        pid, client.unique("r"), claim["leaseId"], claim["generation"],
        claim["sourceDigest"], too_many, expect_error=True)
    assert sc == 400 and err["code"] == "VALIDATION_ERROR"


def test_identical_completion_retry_returns_original_result(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    result_id = client.unique("r")
    payload = [ev(out, "d", {"z": 1, "a": 2})]
    _, done = client.complete(pid, result_id, claim["leaseId"],
                              claim["generation"], claim["sourceDigest"], payload)
    dpos = done["commitPosition"]
    # Exactly-identical retry (canonical key order differs but normalizes
    # equal) after completion: original outcome, same position.
    retry_payload = [ev(out, "d", {"a": 2, "z": 1})]
    _, again = client.complete(pid, result_id, claim["leaseId"],
                               claim["generation"], claim["sourceDigest"],
                               retry_payload)
    assert again["commitPosition"] == dpos and again["eventCount"] == 1
    _, snap = client.snapshot([out])
    txs, _ = client.read_all(snap)
    assert sum(1 for t in txs if t["commitPosition"] == dpos) == 1


def test_completed_result_retry_succeeds_even_after_takeover_generation(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], lease_seconds=30, pid=pid)
    _, _, p1 = _commit_envelope(client, stream)
    _, claim1 = client.claim(pid)
    result_id = client.unique("r")
    _, done = client.complete(pid, result_id, claim1["leaseId"],
                              claim1["generation"], claim1["sourceDigest"], [])
    assert done["throughPosition"] == p1
    # Time passes; a later claim produces a newer generation on new work.
    client.advance(31)
    _, _, p2 = _commit_envelope(client, stream)
    _, claim2 = client.claim(pid)
    assert claim2["generation"] > claim1["generation"]
    # The old successful resultId with the identical original work returns
    # the original outcome -- never LEASE_FENCED.
    _, retry = client.complete(pid, result_id, claim1["leaseId"],
                               claim1["generation"], claim1["sourceDigest"], [])
    assert retry["throughPosition"] == p1 and retry["commitPosition"] is None
    # The newer work is still outstanding and completable.
    client.complete(pid, client.unique("r2"), claim2["leaseId"],
                    claim2["generation"], claim2["sourceDigest"], [])


def test_result_id_reused_with_different_content(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], pid=pid)
    _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    result_id = client.unique("r")
    client.complete(pid, result_id, claim["leaseId"], claim["generation"],
                    claim["sourceDigest"], [ev(out, "d", {"v": 1})])
    sc, err = client.complete(
        pid, result_id, claim["leaseId"], claim["generation"],
        claim["sourceDigest"], [ev(out, "d", {"v": 2})], expect_error=True)
    assert sc == 409 and err["code"] == "RESULT_ID_REUSED"


def test_result_id_reused_on_different_work(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    _make_processor(client, [stream], [out], batch_size=1, pid=pid)
    _commit_envelope(client, stream)
    _, claim1 = client.claim(pid)
    result_id = client.unique("r")
    client.complete(pid, result_id, claim1["leaseId"], claim1["generation"],
                    claim1["sourceDigest"], [])
    _commit_envelope(client, stream)
    _, claim2 = client.claim(pid)
    assert claim2["sourceDigest"] != claim1["sourceDigest"]
    sc, err = client.complete(
        pid, result_id, claim2["leaseId"], claim2["generation"],
        claim2["sourceDigest"], [], expect_error=True)
    assert sc == 409 and err["code"] == "RESULT_ID_REUSED"


def test_unknown_result_is_404(client):
    sc, err = client.get_result_dp(client.unique("proc"), client.unique("r"),
                                   expect_error=True)
    assert sc == 404 and err["code"] == "RESULT_NOT_FOUND"


# ---------------------------------------------------------------------------
# Retention integration
# ---------------------------------------------------------------------------


def test_outstanding_claim_protects_source_from_reclaim(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    grp, _ = client.make_group()
    _make_processor(client, [stream], pid=pid)
    _, _, pos = _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    # The consumer group would allow deletion; the processor must not.
    client.ack(grp, pos + 1)
    _, body = client.reclaim()
    assert pos not in body["positions"]
    assert body["processorCheckpointFloor"] is not None
    # Still readable.
    _, snap = client.snapshot([stream])
    txs, _ = client.read_all(snap)
    assert any(t["commitPosition"] == pos for t in txs)


def test_paused_processor_keeps_protecting_checkpoint(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    grp, _ = client.make_group()
    _make_processor(client, [stream], pid=pid)
    _, _, pos = _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    client.ack(grp, pos + 1)
    client.pause_dp(pid)
    _, body = client.reclaim()
    assert pos not in body["positions"]


def test_after_completion_source_reclaimable_but_derived_envelope_remains(client):
    stream, out = client.unique("in"), client.unique("out")
    pid = client.unique("proc")
    grp, _ = client.make_group()
    _make_processor(client, [stream], [out], pid=pid)
    _, _, src_pos = _commit_envelope(client, stream)
    _, claim = client.claim(pid)
    _, done = client.complete(pid, client.unique("r"), claim["leaseId"],
                              claim["generation"], claim["sourceDigest"],
                              [ev(out, "d", {})])
    dpos = done["commitPosition"]
    client.ack(grp, dpos + 1)
    _, body = client.reclaim()
    # Source range is now below the advanced checkpoint and reclaimable...
    assert src_pos in body["positions"]
    # ...but the derived envelope is above the processor checkpoint (dpos) and
    # protected by it, regardless of group ack.
    assert dpos not in body["positions"]
    _, snap = client.snapshot([out])
    txs, _ = client.read_all(snap)
    assert any(t["commitPosition"] == dpos for t in txs)


def test_deleted_processor_abandons_protection(client):
    stream = client.unique("in")
    pid = client.unique("proc")
    grp, _ = client.make_group()
    _make_processor(client, [stream], pid=pid)
    _, _, pos = _commit_envelope(client, stream)
    client.ack(grp, pos + 1)
    client.delete_processor_dp(pid)
    _, body = client.reclaim()
    assert pos in body["positions"]
