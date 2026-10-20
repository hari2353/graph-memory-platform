"""Output channels: MCP (primary), REST, webhooks, SDK-thin client.

Every channel renders from the same pure ``ContextProvider`` (SPEC §9/§15):
channels add, core never changes. Channel error payloads are exactly
``core.errors.PlatformError.to_payload()``.
"""
