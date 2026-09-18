"""Transaction batch, sequence, idempotent retry and terminal semantics."""


def ev(stream, key, payload):
    return {"stream": stream, "key": key, "payload": payload}


def _setup(client):
    name, _ = client.register_producer(client.unique("prod"), 1)
    return name


def test_first_sequence_must_be_last_plus_one(client):
    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    sc, err = client.write(name, tx, 1, 2, [ev("s", "k", {})], expect_error=True)
    assert sc == 409 and err["code"] == "INVALID_FIRST_SEQUENCE"
    assert err["details"]["expectedFirstSequence"] == 1
    client.write(name, tx, 1, 1, [ev("s", "k", {})])
    _, b = client.commit(name, tx, 1)
    pos1 = b["commitPosition"]

    tx2, _ = client.create_tx(name, epoch=1)
    client.write(name, tx2, 1, 2, [ev("s", "k2", {"x": 1})])
    _, b2 = client.commit(name, tx2, 1)
    assert b2["commitPosition"] > pos1
    assert b2["firstSequence"] == 2


def test_sequences_are_implicit_and_contiguous(client):
    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    events = [ev("s1", f"k{i}", {"i": i}) for i in range(3)]
    client.write(name, tx, 1, 1, events)
    _, b = client.commit(name, tx, 1)
    _, snap = client.snapshot(["s1"])
    txs, _ = client.read_all(snap)
    target = [t for t in txs if t["txId"] == tx][0]
    seqs = [e["sequence"] for e in target["events"]]
    assert seqs == [1, 2, 3]


def test_identical_batch_retry_returns_same_tx(client):
    name = _setup(client)
    tx = client.unique("tx")
    client.create_tx(name, tx_id=tx, epoch=1)
    batch = [ev("s1", "k", {"z": 1, "a": 2})]
    client.write(name, tx, 1, 1, batch)
    # Same content, different JSON key order in payload must canonicalize equal.
    retry = [ev("s1", "k", {"a": 2, "z": 1})]
    sc, b = client.write(name, tx, 1, 1, retry)
    assert sc == 200 and b["status"] == "open"
    _, cb = client.commit(name, tx, 1)
    # commit retry is idempotent and yields same position
    _, cb2 = client.commit(name, tx, 1)
    assert cb2["commitPosition"] == cb["commitPosition"]
    assert cb2["eventCount"] == 1


def test_different_batch_same_txid_is_tx_id_reused(client):
    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    client.write(name, tx, 1, 1, [ev("s1", "k", {"v": 1})])
    sc, err = client.write(name, tx, 1, 1, [ev("s1", "k", {"v": 2})], expect_error=True)
    assert sc == 409 and err["code"] == "TX_ID_REUSED"
    # Different firstSequence with identical events is also reuse.
    sc, err = client.write(name, tx, 1, 2, [ev("s1", "k", {"v": 1})], expect_error=True)
    assert sc == 409 and err["code"] == "TX_ID_REUSED"


def test_reused_txid_after_commit_is_conflict_not_overwrite(client):
    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    client.write(name, tx, 1, 1, [ev("s1", "k", {"v": 1})])
    _, cb = client.commit(name, tx, 1)
    # Creating a tx with the same txId returns the existing record.
    _, b = client.create_tx(name, tx_id=tx, epoch=1)
    assert b["status"] == "committed" and b["commitPosition"] == cb["commitPosition"]
    # Writing different content to the committed txId is TX_ID_REUSED.
    sc, err = client.write(name, tx, 1, 2, [ev("s1", "k", {"v": 99})], expect_error=True)
    assert sc == 409 and err["code"] == "TX_ID_REUSED"


def test_commit_requires_written_batch(client):
    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    sc, err = client.commit(name, tx, 1, expect_error=True)
    assert sc == 409 and err["code"] == "BATCH_NOT_WRITTEN"


def test_commit_abort_race_single_terminal_winner(client):
    import concurrent.futures
    from tests.conftest import Client

    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    client.write(name, tx, 1, 1, [ev("s1", "k", {})])

    outcomes = []

    def do_commit():
        c = Client()
        try:
            sc, b = c.commit(name, tx, 1)
            outcomes.append(("commit", sc, b.get("status"), b.get("commitPosition")))
        except Exception as e:  # pragma: no cover
            outcomes.append(("commit-exc", repr(e)))
        finally:
            c.http.close()

    def do_abort():
        c = Client()
        try:
            sc, b = c.abort(name, tx, 1)
            outcomes.append(("abort", sc, b.get("status"), b.get("commitPosition")))
        except Exception as e:  # pragma: no cover
            outcomes.append(("abort-exc", repr(e)))
        finally:
            c.http.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda f: f(), [do_commit, do_abort]))

    _, g = client.get_tx(name, tx)
    assert g["status"] in ("committed", "aborted")
    winner = g["status"]
    if winner == "committed":
        # repeated commit returns original; abort reports stable conflict
        _, cb = client.commit(name, tx, 1)
        assert cb["status"] == "committed" and cb["commitPosition"] == g["commitPosition"]
        sc, err = client.abort(name, tx, 1, expect_error=True)
        assert sc == 409 and err["code"] == "TERMINAL_CONFLICT"
        assert err["details"]["status"] == "committed"
        assert err["details"]["commitPosition"] == g["commitPosition"]
    else:
        _, ab = client.abort(name, tx, 1)
        assert ab["status"] == "aborted"
        sc, err = client.commit(name, tx, 1, expect_error=True)
        assert sc == 409 and err["code"] == "TERMINAL_CONFLICT"
        assert err["details"]["status"] == "aborted"


def test_committed_tx_remains_identifiable_after_epoch_bump(client):
    """Critical: a successful commit must never be reported as FENCED."""
    name = _setup(client)
    tx, _ = client.create_tx(name, epoch=1)
    client.write(name, tx, 1, 1, [ev("s1", "k", {})])
    _, cb = client.commit(name, tx, 1)
    pos = cb["commitPosition"]
    client.register_producer(name, 2)  # new agent takes over

    # Retried commit by the old agent returns the original result.
    _, retry = client.commit(name, tx, 1)
    assert retry["status"] == "committed"
    assert retry["commitPosition"] == pos
    # GET by txId likewise shows committed, not fenced.
    _, g = client.get_tx(name, tx)
    assert g["status"] == "committed" and g["commitPosition"] == pos


def test_commit_positions_are_strictly_increasing_and_unique(client):
    name = _setup(client)
    positions = []
    for i in range(5):
        tx, _ = client.create_tx(name, epoch=1)
        client.write(name, tx, 1, i + 1, [ev("s1", f"k{i}", {})])
        _, b = client.commit(name, tx, 1)
        positions.append(b["commitPosition"])
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)


def test_uncommitted_and_aborted_never_visible(client):
    name = _setup(client)
    tx_open, _ = client.create_tx(name, epoch=1)
    client.write(name, tx_open, 1, 1, [ev("s.secret", "k", {})])
    # A second producer with an aborted batch on the same stream.
    name2, _ = client.register_producer(client.unique("prod"), 1)
    tx2, _ = client.create_tx(name2, epoch=1)
    client.write(name2, tx2, 1, 1, [ev("s.secret", "k2", {})])
    client.abort(name2, tx2, 1)
    _, snap = client.snapshot(["s.secret"])
    txs, _ = client.read_all(snap)
    ids = {(t["producer"], t["txId"]) for t in txs}
    assert (name, tx_open) not in ids
    assert (name2, tx2) not in ids
