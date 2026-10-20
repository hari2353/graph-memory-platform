"""Envelope validation tests — the ledger's front door contract."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from graph_memory.core.errors import EnvelopeError
from graph_memory.signals.envelope import CLOUDEVENTS_SPECVERSION, ENVELOPE_VERSION, Envelope

_SHIFTED = timezone(timedelta(hours=2))


def _minimal(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "specversion": "1.0",
        "id": "evt-0001",
        "source": "https://app.example.com/signals",
        "type": "itf.skill_assessed.v1",
        "time": "2026-06-01T02:00:00+02:00",
        "partitionkey": "user-17",
        "data": {"skill": "rust", "score": 0.8},
    }
    payload.update(overrides)
    for key, value in list(payload.items()):
        if value is None:
            del payload[key]
    return payload


def _parse(payload: dict[str, object]) -> Envelope:
    return Envelope.parse(payload)


def test_minimal_event_parses_all_fields() -> None:
    envelope = _parse(_minimal())
    assert envelope.specversion == "1.0"
    assert envelope.id == "evt-0001"
    assert envelope.type == "itf.skill_assessed.v1"
    assert envelope.signal == "itf.skill_assessed.v1"
    assert envelope.occurred_at == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    assert envelope.partitionkey == "user-17"
    assert envelope.data == {"skill": "rust", "score": 0.8}


def test_time_is_normalized_to_utc() -> None:
    envelope = _parse(_minimal())
    assert envelope.occurred_at.utcoffset() == timedelta(0)


def test_dedup_key_is_stable_hash_of_source_and_id() -> None:
    envelope = _parse(_minimal())
    expected = hashlib.sha256(f"{envelope.source}\x1f{envelope.id}".encode()).hexdigest()
    other_partition = _parse(_minimal(partitionkey="user-99"))
    assert envelope.dedup_key == expected
    assert envelope.dedup_key == other_partition.dedup_key  # same signal, different user


@pytest.mark.parametrize(
    "override",
    [
        {"specversion": "0.3"},
        {"id": None},
        {"source": None},
        {"type": None},
        {"time": None},
        {"partitionkey": None},
        {"data": None},
    ],
)
def test_missing_required_attribute_rejected(override: dict[str, object]) -> None:
    with pytest.raises(EnvelopeError) as err:
        _parse(_minimal(**override))
    payload = err.value.to_payload()
    assert payload["error"] == "envelope_invalid"
    details = payload["details"]
    assert isinstance(details, dict)
    assert "issues" in details
    assert details["envelope_version"] == ENVELOPE_VERSION


def test_namespaced_versioned_type_enforced() -> None:
    for bad in ["skill_assessed", "itf.skill_assessed", "itf.skill_assessed.1"]:
        with pytest.raises(EnvelopeError, match="validation"):
            _parse(_minimal(type=bad))


def test_naive_time_rejected() -> None:
    with pytest.raises(EnvelopeError):
        _parse(_minimal(time="2026-06-01T00:00:00"))


@pytest.mark.parametrize(
    "bad_partition",
    ["", "  ", " user-17", "user-17 ", "yo user", "u" * 257],
)
def test_partitionkey_is_user_key_validated(bad_partition: str) -> None:
    with pytest.raises(EnvelopeError):
        _parse(_minimal(partitionkey=bad_partition))


@pytest.mark.parametrize("bad_data", [["list"], [1, 2], "string", 42, 3.5, True])
def test_data_must_be_a_json_object(bad_data: object) -> None:
    payload = _minimal()
    payload["data"] = bad_data
    with pytest.raises(EnvelopeError):
        _parse(payload)


def test_id_boundaries_enforced() -> None:
    with pytest.raises(EnvelopeError):
        _parse(_minimal(id=" evt-0001"))
    with pytest.raises(EnvelopeError):
        _parse(_minimal(id="evt-0001\n"))
    with pytest.raises(EnvelopeError):
        _parse(_minimal(id="x" * 257))


def test_source_rejects_whitespace_and_controls() -> None:
    with pytest.raises(EnvelopeError):
        _parse(_minimal(source="https://app.example.com/signals x"))
    with pytest.raises(EnvelopeError):
        _parse(_minimal(source="https://app.example.com/\x01"))


def test_optional_attributes_when_valid() -> None:
    envelope = _parse(
        _minimal(
            dataschema="https://schema.example.com/skill_assessed.v1.json",
            subject="user-17",
            tenantid="acme",
        )
    )
    assert envelope.dataschema is not None
    assert envelope.subject == "user-17"
    assert envelope.tenantid == "acme"


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("dataschema", "has space"),
        ("subject", " padded"),
        ("tenantid", "acme "),
    ],
)
def test_optional_attribute_validation(field: str, bad: str) -> None:
    with pytest.raises(EnvelopeError):
        _parse(_minimal(**{field: bad}))


def test_unknown_extension_attributes_are_preserved() -> None:
    envelope = _parse(_minimal(traceparent="00-abcdef-01"))
    assert envelope.extensions["traceparent"] == "00-abcdef-01"
    dumped: dict[str, object] = envelope.model_dump()
    assert dumped["traceparent"] == "00-abcdef-01"  # round-trip preserved


def test_envelope_is_frozen() -> None:
    envelope = _parse(_minimal())
    with pytest.raises(ValidationError):
        setattr(envelope, "id", "mutated")  # noqa: B010 — frozen-ness is the point


def test_parse_reports_envelope_version_in_details() -> None:
    with pytest.raises(EnvelopeError) as err:
        _parse(_minimal(specversion="1.1"))
    payload = err.value.to_payload()
    details = payload["details"]
    assert isinstance(details, dict)
    assert details["envelope_version"] == ENVELOPE_VERSION


def test_specversion_constant_is_the_ce_pin() -> None:
    assert CLOUDEVENTS_SPECVERSION == "1.0"
    envelope = _parse(_minimal())
    assert envelope.model_dump(exclude_none=True)["specversion"] == "1.0"
    assert envelope.occurred_at == datetime(2026, 6, 1, tzinfo=UTC)
