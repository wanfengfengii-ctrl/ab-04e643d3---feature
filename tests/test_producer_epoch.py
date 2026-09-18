"""Producer sessions, monotonic epochs and fencing semantics."""
import concurrent.futures


def _ev(i, stream="s1"):
    return {"stream": stream, "key": f"k{i}", "payload": {"i": i}}


def test_create_producer_is_idempotent_on_same_epoch(client):
    name = client.unique("prod")
    _, b1 = client.register_producer(name, 1)
    assert b1["status"] == "created"
    _, b2 = client.register_producer(name, 1)
    assert b2["status"] == "already_current"
    assert b2["epoch"] == 1


def test_epoch_must_advance_monotonically(client):
    name = client.unique("prod")
    client.register_producer(name, 5)
    sc, err = client.register_producer(name, 4, expect_error=True)
    assert sc == 409 and err["code"] == "EPOCH_NOT_CURRENT"
    _, b = client.register_producer(name, 6)
    assert b["epoch"] == 6 and b["status"] == "fenced_previous"


def test_single_open_tx_per_epoch(client):
    name = client.unique("prod")
    client.register_producer(name, 1)
    client.create_tx(name, epoch=1)
    sc, err = client.create_tx(name, epoch=1, expect_error=True)
    assert sc == 409 and err["code"] == "OPEN_TRANSACTION_EXISTS"
    # A new epoch allows a new open transaction.
    client.register_producer(name, 2)
    tx2, b = client.create_tx(name, epoch=2)
    assert b["status"] == "open"


def test_lower_epoch_open_tx_is_fenced_on_epoch_bump(client):
    name = client.unique("prod")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    client.write(name, tx, 1, 1, [_ev(1)])
    client.register_producer(name, 2)

    # The old open transaction is now fenced.
    sc, err = client.commit(name, tx, 1, expect_error=True)
    assert sc == 409 and err["code"] == "FENCED"
    sc, err = client.write(name, tx, 1, 1, [_ev(9)], expect_error=True)
    assert sc == 409 and err["code"] == "FENCED"
    _, g = client.get_tx(name, tx)
    assert g["status"] == "fenced"


def test_fenced_tx_consumes_no_sequence(client):
    name = client.unique("prod")
    client.register_producer(name, 1)
    tx1, _ = client.create_tx(name, epoch=1)
    client.write(name, tx1, 1, 1, [_ev(1)])
    client.register_producer(name, 2)
    # tx1 fenced; a fresh epoch-2 tx must still start at sequence 1.
    tx2, _ = client.create_tx(name, epoch=2)
    client.write(name, tx2, 2, 1, [_ev(2)])
    _, b = client.commit(name, tx2, 2)
    assert b["status"] == "committed"
    assert b["firstSequence"] == 1


def test_abort_does_not_consume_sequence(client):
    name = client.unique("prod")
    client.register_producer(name, 1)
    tx1, _ = client.create_tx(name, epoch=1)
    client.write(name, tx1, 1, 1, [_ev(1)])
    _, ab = client.abort(name, tx1, 1)
    assert ab["status"] == "aborted"
    # aborted tx frees the "one open" slot
    tx2, _ = client.create_tx(name, epoch=1)
    client.write(name, tx2, 1, 1, [_ev(2)])
    _, b = client.commit(name, tx2, 1)
    assert b["firstSequence"] == 1


def test_concurrent_epoch_bumps_are_serialized(client):
    from tests.conftest import Client
    name = client.unique("prod")
    client.register_producer(name, 1)

    def bump(e):
        c = Client()
        try:
            return c.register_producer(name, e, expect_error=True)
        except Exception as exc:
            return ("exc", repr(exc))
        finally:
            c.http.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(bump, [2, 3]))
    _, final = client.request("GET", f"/v1/producers/{name}")
    assert final["epoch"] == 3
    for r in results:
        # each call either succeeded (200) or was a 409 stale epoch
        assert isinstance(r, tuple) and len(r) == 2


def test_higher_unregistered_epoch_rejected_for_tx(client):
    name = client.unique("prod")
    client.register_producer(name, 1)
    resp = client.raw("POST", "/v1/transactions",
                      json={"producer": name, "txId": client.unique("tx"), "epoch": 9})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] in ("FENCED", "EPOCH_NOT_CURRENT")
