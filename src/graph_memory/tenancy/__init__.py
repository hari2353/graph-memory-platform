"""The tenancy module's first slice: credential -> principal."""

from graph_memory.tenancy.guard import StaticBearerGuard

__all__ = ["StaticBearerGuard"]
