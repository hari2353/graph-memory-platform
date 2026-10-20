"""MCP output channel (SPEC §9 primary)."""

from graph_memory.channels.mcp.server import (
    PROTOCOL_REVISION,
    SERVER_VERSION,
    BearerHeaderTransport,
    build_mcp_server,
    mcp_streamable_app,
)

__all__ = [
    "PROTOCOL_REVISION",
    "SERVER_VERSION",
    "BearerHeaderTransport",
    "build_mcp_server",
    "mcp_streamable_app",
]
