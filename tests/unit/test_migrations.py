from graph_memory.migrations import Migration, plan_migrations, validate_chain


def test_plan_returns_pending_sorted():
    migrations = [
        Migration(2, "b", "SELECT 2"),
        Migration(1, "a", "SELECT 1"),
        Migration(3, "c", "SELECT 3"),
    ]
    out = plan_migrations(migrations, applied={1})
    assert [m.version for m in out] == [2, 3]


def test_plan_rejects_duplicate_versions():
    try:
        plan_migrations([Migration(1, "a", "SELECT 1"), Migration(1, "b", "SELECT 2")])
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate versions accepted")


def test_validate_chain_detects_gap_and_drift():
    migrations = [Migration(1, "a", "SELECT 1"), Migration(3, "c", "SELECT 3")]
    problems = validate_chain(migrations, applied_checksums={1: "deadbeef"})
    assert any("missing migration version 2" in p for p in problems)
    assert any("checksum drift on v1" in p for p in problems)


def test_validate_chain_clean():
    m = Migration(1, "a", "SELECT 1")
    assert validate_chain([m], applied_checksums={1: m.checksum}) == []


def test_migration_rejects_invalid():
    for kwargs in ({"version": 0, "name": "a", "sql": "SELECT 1"},
                   {"version": 1, "name": " ", "sql": "SELECT 1"},
                   {"version": 1, "name": "a", "sql": " "}):
        try:
            Migration(**kwargs)
        except ValueError:
            continue
        raise AssertionError(f"accepted invalid migration: {kwargs}")


def test_checksum_is_deterministic():
    a = Migration(1, "a", "SELECT 1")
    b = Migration(1, "a", "SELECT 1")
    assert a.checksum == b.checksum
