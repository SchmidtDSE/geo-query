"""DNS-rebinding protection from deployment config (#435).

The MCP SDK can check each request's Host and Origin headers against allowed
lists. The public deployments leave that off: they are meant to be reached from
any origin, under hostnames the ingress chooses. A server on someone's own
machine is different. In a DNS-rebinding attack a web page re-points its own
hostname at 127.0.0.1, and the browser then lets it call localhost services and
read the replies, so any page open in the browser could query whatever data the
local stack holds.

MCP_ALLOWED_HOSTS and MCP_ALLOWED_ORIGINS (comma-separated) turn the check on.
"host:*" matches any port. A request with no Origin header (a non-browser
client) passes the Origin check. With neither variable set the server behaves as
before, with the check off.
"""
import os

from mcp.server.transport_security import TransportSecuritySettings


def _split(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def transport_security_from_env(env=None) -> TransportSecuritySettings:
    """Settings for FastMCP's transport_security, from MCP_ALLOWED_HOSTS/ORIGINS.

    Raises ValueError when origins are set without hosts: the SDK would then
    refuse every request's Host header, which is never what was meant.
    """
    env = os.environ if env is None else env
    hosts = _split(env.get("MCP_ALLOWED_HOSTS", ""))
    origins = _split(env.get("MCP_ALLOWED_ORIGINS", ""))
    if not hosts and not origins:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    if not hosts:
        raise ValueError(
            "MCP_ALLOWED_ORIGINS is set but MCP_ALLOWED_HOSTS is not; with "
            "DNS-rebinding protection on, every request's Host would be refused. "
            "Set MCP_ALLOWED_HOSTS too (e.g. localhost,127.0.0.1)."
        )
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=origins,
    )


def describe(settings: TransportSecuritySettings) -> str:
    """One boot-log line saying what the check allows."""
    if not settings.enable_dns_rebinding_protection:
        return "🌍 DNS-rebinding protection off (no MCP_ALLOWED_HOSTS)"
    origins = ", ".join(settings.allowed_origins) or "none (only requests without Origin)"
    return (f"🛡️ DNS-rebinding protection on: hosts {', '.join(settings.allowed_hosts)}; "
            f"origins {origins}")
