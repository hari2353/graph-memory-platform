"""The quiet chore: credential -> principal, tenant derived from claims only.

M0 slice: a static bearer-token map for local/dev. The contract is
``core.ports.TenantGuard``; production adapters (verified JWT/OIDC claim sets)
replace THIS class without touching core. Tokens are secrets — they are never
logged, echoed, or embedded in error messages.
"""

from __future__ import annotations

from collections.abc import Mapping

from graph_memory.core.errors import ScopeDenied
from graph_memory.core.types import Principal, Scope, TenantId, tenant_id


class StaticBearerGuard:
    """Dev-grade guard: each static token is bound to exactly one tenant."""

    def __init__(
        self,
        tokens: Mapping[str, str],
        *,
        scopes: frozenset[Scope] | None = None,
    ) -> None:
        self._tokens: dict[str, TenantId] = {
            token: tenant_id(name) for token, name in tokens.items()
        }
        self._scopes: frozenset[Scope] = scopes or frozenset(
            {Scope.SIGNALS_WRITE, Scope.CONTEXT_READ, Scope.MEMORY_SEARCH}
        )

    def require_scope(self, credential: str, needed: Scope) -> Principal:
        tenant = self._tenant_for(credential)
        principal = Principal(
            tenant=tenant,
            scopes=self._scopes,
            subject=f"static:{tenant}",
        )
        if not principal.allows(needed):
            raise ScopeDenied(
                "credential does not carry the required scope",
                details={"needed_scope": needed.value},
            )
        return principal

    def _tenant_for(self, credential: str) -> TenantId:
        token = credential.strip()
        lowered = token.casefold()
        if lowered.startswith("bearer "):
            token = token[len("bearer ") :].strip()
        tenant = self._tokens.get(token)
        if tenant is None:
            raise ScopeDenied(
                "credential rejected",
                details={"reason": "unknown_credential"},
            )
        return tenant
