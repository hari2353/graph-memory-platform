"""Unit tests for the core domain foundation (task: core/ ports)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta, timezone

import pytest

from graph_memory.core import (
    Capability,
    EnvelopeError,
    FixedClock,
    IngestResult,
    PlatformError,
    Principal,
    QuotaExceeded,
    Scope,
    ScopeDenied,
    SignalSink,
    SignalType,
    SystemClock,
    TenantId,
    TenantMismatch,
    UserContext,
    UserKey,
    UserRef,
    log_safe,
    parse_utc,
    require_tenant_match,
    signal_type,
    tenant_id,
    user_key,
)

_CODE_OWNERS: tuple[type[PlatformError], ...] = (
    PlatformError,
    EnvelopeError,
    TenantMismatch,
    ScopeDenied,
    QuotaExceeded,
)


class TestLogsafe:
    def test_log_forging_is_flattened(self) -> None:
        forged = "tenant=acme\n2026-01-01T00:00:00Z WARN injected line"
        out = log_safe(forged)
        assert "\n" not in out
        assert out == "tenant=acme?2026-01-01T00:00:00Z WARN injected line"
        assert "injected line" in out  # content preserved, structure destroyed

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("a\rb", "a?b"),
            ("a\tb", "a?b"),
            ("a\x00b", "a?b"),
            ("a\u0085b", "a?b"),  # C1 NEL
            ("a\u009fb", "a?b"),  # C1 APC
            ("a\x7fb", "a?b"),  # DEL
        ],
    )
    def test_controls_replaced_with_placeholder(self, raw: str, expected: str) -> None:
        assert log_safe(raw) == expected

    def test_ansi_escape_neutralized_but_kept_readable(self) -> None:
        assert log_safe("\x1b[31mRED\x1b[0m") == "?[31mRED?[0m"

    def test_long_values_truncate_with_ellipsis(self) -> None:
        out = log_safe("x" * 200, max_len=50)
        assert len(out) == 50
        assert out.endswith("...")
        assert out[:47] == "x" * 47

    def test_short_values_pass_untouched(self) -> None:
        assert log_safe("clean value") == "clean value"

    def test_max_len_must_fit_the_ellipsis(self) -> None:
        with pytest.raises(ValueError, match="max_len"):
            log_safe("abc", max_len=2)


class TestClocks:
    def test_system_clock_returns_aware_utc(self) -> None:
        moment = SystemClock().now()
        assert moment.tzinfo is not None
        assert moment.utcoffset() == timedelta(0)

    def test_fixed_clock_is_frozen_until_ticked(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        clock = FixedClock(start)
        assert clock.now() == start
        assert clock.tick(90) == datetime(2026, 1, 1, 0, 1, 30, tzinfo=UTC)
        assert clock.now() == datetime(2026, 1, 1, 0, 1, 30, tzinfo=UTC)

    def test_fixed_clock_rewinds_for_determinism_checks(self) -> None:
        start = datetime(2026, 2, 2, tzinfo=UTC)
        clock = FixedClock(start)
        clock.tick(30)
        clock.tick(-30)
        assert clock.now() == start

    def test_fixed_clock_set_jumps_and_normalizes_to_utc(self) -> None:
        clock = FixedClock(datetime(2026, 1, 1, tzinfo=UTC))
        shifted = timezone(timedelta(hours=2))
        landed = clock.set(datetime(2026, 6, 1, 2, 0, tzinfo=shifted))
        assert landed.utcoffset() == timedelta(0)
        assert landed == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)

    def test_fixed_clock_rejects_naive_instants(self) -> None:
        with pytest.raises(ValueError, match="aware"):
            FixedClock(datetime(2026, 1, 1))
        clock = FixedClock(datetime(2026, 1, 1, tzinfo=UTC))
        with pytest.raises(ValueError, match="aware"):
            clock.set(datetime(2026, 1, 1))


class TestValidators:
    @pytest.mark.parametrize("bad", ["", "   ", " acme", "acme ", "with space", "x" * 129])
    def test_tenant_id_rejections(self, bad: str) -> None:
        with pytest.raises(ValueError, match="tenant id"):
            tenant_id(bad)

    def test_tenant_id_accepts_a_normal_id(self) -> None:
        built = tenant_id("acme-corp")
        assert isinstance(built, str) and built == "acme-corp"

    @pytest.mark.parametrize("bad", ["", " ", " itf", "u" * 257])
    def test_user_key_rejections(self, bad: str) -> None:
        with pytest.raises(ValueError, match="user key"):
            user_key(bad)

    def test_user_key_accepts_a_normal_key(self) -> None:
        assert user_key("u-42") == UserKey("u-42")

    def test_signal_type_requires_namespaced_version(self) -> None:
        assert signal_type("itf.skill_assessed.v1") == SignalType("itf.skill_assessed.v1")
        with pytest.raises(ValueError, match="vN"):
            signal_type("skill")
        with pytest.raises(ValueError, match="vN"):
            signal_type("itf.skill_assessed")
        with pytest.raises(ValueError, match="vN"):
            signal_type("itf.skill_assessed.1")
        with pytest.raises(ValueError, match="vN"):
            signal_type("itf.v1.skill")


class TestPrincipalAndAccess:
    def test_scope_wire_values_are_a_public_contract(self) -> None:
        assert Scope.SIGNALS_WRITE.value == "signals:write"
        assert Scope.CONTEXT_READ.value == "context:read"
        assert Scope.MEMORY_SEARCH.value == "memory:search"
        assert Scope.ADMIN.value == "platform:admin"

    def test_principal_allows_its_scopes_only(self) -> None:
        principal = Principal(tenant_id("acme"), frozenset({Scope.CONTEXT_READ}), "subj")
        assert principal.allows(Scope.CONTEXT_READ)
        assert not principal.allows(Scope.ADMIN)
        assert principal.tenant == TenantId("acme")


class TestTenantMatch:
    def test_cross_tenant_reference_raises(self) -> None:
        principal = Principal(tenant_id("acme"), frozenset({Scope.CONTEXT_READ}), "subj")
        intruder = UserRef(tenant_id("other"), user_key("u1"))
        with pytest.raises(TenantMismatch, match="acme"):
            require_tenant_match(principal, intruder)

    def test_same_tenant_passes_silently(self) -> None:
        principal = Principal(tenant_id("acme"), frozenset(), "subj")
        own = UserRef(tenant_id("acme"), user_key("u1"))
        require_tenant_match(principal, own)  # does not raise


class TestErrors:
    def test_every_code_is_unique(self) -> None:
        codes = [kind.code for kind in _CODE_OWNERS]
        assert len(codes) == len(set(codes))

    def test_base_error_payload_shape(self) -> None:
        assert PlatformError("boom").to_payload() == {
            "error": "platform_error",
            "message": "boom",
        }

    def test_payload_messages_are_log_safe(self) -> None:
        err = EnvelopeError("bad event\nX-Injected: forged header", details={"index": 3})
        payload = err.to_payload()
        assert payload["error"] == "envelope_invalid"
        assert "\n" not in str(payload["message"])
        assert payload["details"] == {"index": 3}

    def test_quota_error_carries_retry_hint(self) -> None:
        err = QuotaExceeded("over batch budget", details={"retry_after_seconds": 30})
        assert err.to_payload()["details"] == {"retry_after_seconds": 30}


class TestUserContext:
    def test_staleness_uses_the_declared_max_age(self) -> None:
        generated = datetime(2026, 1, 1, tzinfo=UTC)
        ctx = UserContext(
            user=UserRef(tenant_id("acme"), user_key("u1")),
            capability=Capability("profile"),
            generated_at=generated,
            max_age_ms=1_000,
            cache_scope="user",
            payload={},
        )
        assert not ctx.is_stale(generated + timedelta(milliseconds=999))
        assert ctx.is_stale(generated + timedelta(milliseconds=1_001))

    def test_staleness_requires_aware_instant(self) -> None:
        ctx = UserContext(
            user=UserRef(tenant_id("acme"), user_key("u1")),
            capability=Capability("profile"),
            generated_at=datetime(2026, 1, 1, tzinfo=UTC),
            max_age_ms=1,
            cache_scope="user",
            payload={},
        )
        with pytest.raises(ValueError, match="aware"):
            ctx.is_stale(datetime(2026, 1, 1))


class TestIngestResult:
    def test_counts_must_be_non_negative(self) -> None:
        assert IngestResult(accepted=2, duplicates=1).accepted == 2
        with pytest.raises(ValueError, match="non-negative"):
            IngestResult(accepted=-1, duplicates=0)


class _CountingSink:
    """Structural SignalSink implementation — proves the Protocol is implementable."""

    def __init__(self) -> None:
        self.seen: list[list[str]] = []

    async def submit(self, events: Sequence[str]) -> IngestResult:
        self.seen.append(list(events))
        return IngestResult(accepted=len(events), duplicates=0)


async def test_signal_sink_protocol_is_implementable() -> None:
    stub = _CountingSink()
    sink: SignalSink[str] = stub
    result = await sink.submit(["a", "b", "c"])
    assert (result.accepted, result.duplicates) == (3, 0)
    assert stub.seen == [["a", "b", "c"]]


def test_parse_utc_normalizes_to_utc_or_rejects_naive() -> None:
    assert parse_utc("2026-06-01T02:00:00+02:00") == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="UTC offset"):
        parse_utc("2026-06-01T00:00:00")
