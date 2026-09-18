"""Strict input validation contract."""
import httpx
import pytest

from tests.conftest import BASE_URL, CLOCK_TOKEN


def _post(client, path, payload, raw=False):
    headers = {"X-Now": "1900000000", "X-Now-Token": CLOCK_TOKEN,
               "Content-Type": "application/json"}
    kwargs = {"content": payload, "headers": headers} if raw else {"json": payload, "headers": headers}
    return client.http.request("POST", path, **kwargs)


@pytest.mark.parametrize("bad_name", [
    "", "a" * 129, "bad name", "bad/name", "bad:name", "héllo", "-dash",
    ".dot", "_under", "a b", "naïve",
])
def test_bad_producer_names(client, bad_name):
    r = _post(client, "/v1/producers", {"name": bad_name, "epoch": 1})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "VALIDATION_ERROR"


def test_epoch_must_be_positive_integer(client):
    for bad in [0, -1, 2**63, True, 1.5, "1", 1e3]:
        r = _post(client, "/v1/producers", {"name": client.unique("p"), "epoch": bad})
        assert r.status_code == 400, (bad, r.text)


def test_boolean_is_not_integer(client):
    name = client.unique("p")
    r = _post(client, "/v1/producers", {"name": name, "epoch": True})
    assert r.status_code == 400


def test_exponent_and_decimal_rejected(client):
    name = client.unique("p")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    # raw JSON body with exponential firstSequence
    body = b'{"epoch":1,"firstSequence":1e0,"events":[{"stream":"s1","key":"k","payload":{}}]}'
    r = _post_raw_put(client, f"/v1/transactions/{name}/{tx}/batch", body)
    assert r.status_code == 400
    body = b'{"epoch":1,"firstSequence":1.0,"events":[{"stream":"s1","key":"k","payload":{}}]}'
    r = _post_raw_put(client, f"/v1/transactions/{name}/{tx}/batch", body)
    assert r.status_code == 400


def _post_raw_put(client, path, body):
    headers = {"X-Now": "1900000000", "X-Now-Token": CLOCK_TOKEN,
               "Content-Type": "application/json"}
    return client.http.request("PUT", path, content=body, headers=headers)


def test_duplicate_object_keys_rejected(client):
    name = client.unique("p")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    body = (b'{"epoch":1,"firstSequence":1,"events":['
            b'{"stream":"s1","key":"k","payload":{"a":1,"a":2}}]}')
    r = _post_raw_put(client, f"/v1/transactions/{name}/{tx}/batch", body)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "INVALID_JSON"


def test_non_finite_numbers_rejected(client):
    name = client.unique("p")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    for token in (b"NaN", b"Infinity", b"-Infinity"):
        body = (b'{"epoch":1,"firstSequence":1,"events":['
                b'{"stream":"s1","key":"k","payload":' + token + b'}}]}')
        r = _post_raw_put(client, f"/v1/transactions/{name}/{tx}/batch", body)
        assert r.status_code == 400


def test_malformed_json_creates_nothing(client):
    r = _post(client, "/v1/producers", b'{not json', raw=True)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "INVALID_JSON"


def test_batch_size_limits(client):
    name = client.unique("p")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    events = [{"stream": "s1", "key": f"k{i}", "payload": {}} for i in range(101)]
    resp = client.raw("PUT", f"/v1/transactions/{name}/{tx}/batch",
                      json={"epoch": 1, "firstSequence": 1, "events": events})
    assert resp.status_code == 400


def test_empty_batch_rejected(client):
    name = client.unique("p")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    resp = client.raw("PUT", f"/v1/transactions/{name}/{tx}/batch",
                      json={"epoch": 1, "firstSequence": 1, "events": []})
    assert resp.status_code == 400


def test_validation_failure_creates_no_records(client):
    # Invalid group ack against missing group must not invent the group.
    resp = client.raw("POST", "/v1/consumer-groups/ghost-group/ack",
                      json={"position": 5})
    assert resp.status_code in (400, 404)
    sc, _ = client.request("GET", "/v1/consumer-groups/ghost-group", expect_error=True)
    assert sc == 404


def test_oversize_payload_rejected_with_413(client):
    name = client.unique("p")
    client.register_producer(name, 1)
    tx, _ = client.create_tx(name, epoch=1)
    big = "x" * 70000
    resp = client.raw("PUT", f"/v1/transactions/{name}/{tx}/batch",
                      json={"epoch": 1, "firstSequence": 1,
                            "events": [{"stream": "s1", "key": "k",
                                        "payload": {"data": big}}]})
    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "BATCH_TOO_LARGE"
    # Transaction remains open/unwritten: a later valid batch is accepted.
    sc, _ = client.write(name, tx, 1, 1,
                         [{"stream": "s1", "key": "k", "payload": {}}])
    assert sc == 200


def test_bad_names_for_streams_and_groups(client):
    sc, err = client.request("POST", "/v1/snapshots",
                             {"streams": ["bad/stream"]}, expect_error=True)
    assert sc == 400 and err["code"] == "VALIDATION_ERROR"
    sc, err = client.request("POST", "/v1/consumer-groups",
                             {"name": "grp with space"}, expect_error=True)
    assert sc == 400 and err["code"] == "VALIDATION_ERROR"
