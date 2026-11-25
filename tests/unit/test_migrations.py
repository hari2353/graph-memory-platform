from graph_memory.migrations import Migration, plan_migrations, validate_chain


def test_plan_returns_pending_sorted() -> None:
    migrations = [
        Migration(2, "b", "SELECT 2"),
        Migration(1, "a", "SELECT 1"),
        Migration(3, "c", "SELECT 3"),
    ]
    out = plan_migrations(migrations, applied={1})
    assert [m.version for m in out] == [2, 3]


def test_plan_rejects_duplicate_versions() -> None:
    try:
        plan_migrations([Migration(1, "a", "SELECT 1"), Migration(1, "b", "SELECT 2")])
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate versions accepted")


def test_validate_chain_detects_gap_and_drift() -> None:
    migrations = [Migration(1, "a", "SELECT 1"), Migration(3, "c", "SELECT 3")]
    problems = validate_chain(migrations, applied_checksums={1: "deadbeef"})
    assert any("missing migration version 2" in p for p in problems)
    assert any("checksum drift on v1" in p for p in problems)


def test_validate_chain_clean() -> None:
    m = Migration(1, "a", "SELECT 1")
    assert validate_chain([m], applied_checksums={1: m.checksum}) == []


def test_migration_rejects_invalid() -> None:
    cases = [
        (0, "a", "SELECT 1"),
        (1, " ", "SELECT 1"),
        (1, "a", " "),
    ]
    for version, name, sql in cases:
        try:
            Migration(version=version, name=name, sql=sql)
        except ValueError:
            continue
        raise AssertionError(f"accepted invalid migration: {version, name, sql}")


def test_checksum_is_deterministic() -> None:
    a = Migration(1, "a", "SELECT 1")
    b = Migration(1, "a", "SELECT 1")
    assert a.checksum == b.checksum
