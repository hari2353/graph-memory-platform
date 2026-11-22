"""Schema migration planning (cross-pollinated from snowflake-labs/schemachange).

A migration is a versioned SQL statement. The planner returns the pending ones
in version order and validates that versions and checksums are consistent, so
a deploy can be reviewed before anything runs. No database dependency.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValueError("version must be >= 1")
        if not self.name.strip():
            raise ValueError("name must be non-empty")
        if not self.sql.strip():
            raise ValueError("sql must be non-empty")

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()[:12]


def plan_migrations(
    migrations: list[Migration], applied: set[int] | None = None
) -> list[Migration]:
    """Return pending migrations sorted by version; reject duplicate versions."""
    applied = applied or set()
    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise ValueError("duplicate migration versions")
    pending = [m for m in migrations if m.version not in applied]
    return sorted(pending, key=lambda m: m.version)


def validate_chain(
    migrations: list[Migration], applied_checksums: dict[int, str] | None = None
) -> list[str]:
    """Return a list of problems: missing migrations, checksum drift."""
    applied_checksums = applied_checksums or {}
    problems: list[str] = []
    versions = sorted(m.version for m in migrations)
    for version in range(1, (versions[-1] if versions else 0) + 1):
        if version not in versions:
            problems.append(f"missing migration version {version}")
    for migration in migrations:
        recorded = applied_checksums.get(migration.version)
        if recorded is not None and recorded != migration.checksum:
            problems.append(
                f"checksum drift on v{migration.version}: "
                f"recorded {recorded}, current {migration.checksum}"
            )
    return problems
