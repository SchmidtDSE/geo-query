"""DNS-rebinding protection from MCP_ALLOWED_HOSTS / MCP_ALLOWED_ORIGINS (#435)."""
import pytest
from mcp.server.fastmcp import FastMCP
from starlette.testclient import TestClient

from rebinding import describe, transport_security_from_env

_INIT = {
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "t", "version": "0"}},
}
_HEADERS = {"Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"}


def test_off_when_neither_is_set():
    s = transport_security_from_env({})
    assert not s.enable_dns_rebinding_protection
    assert "off" in describe(s)


def test_lists_are_split_and_trimmed():
    s = transport_security_from_env({
        "MCP_ALLOWED_HOSTS": " localhost, 127.0.0.1:* ,",
        "MCP_ALLOWED_ORIGINS": "http://localhost:8080,,http://127.0.0.1:8080",
    })
    assert s.enable_dns_rebinding_protection
    assert s.allowed_hosts == ["localhost", "127.0.0.1:*"]
    assert s.allowed_origins == ["http://localhost:8080", "http://127.0.0.1:8080"]
    assert "on" in describe(s)


def test_hosts_alone_turns_it_on_with_no_origins():
    s = transport_security_from_env({"MCP_ALLOWED_HOSTS": "localhost"})
    assert s.enable_dns_rebinding_protection and s.allowed_origins == []
    assert "only requests without Origin" in describe(s)


def test_origins_without_hosts_is_refused():
    with pytest.raises(ValueError, match="MCP_ALLOWED_HOSTS"):
        transport_security_from_env({"MCP_ALLOWED_ORIGINS": "http://localhost:8080"})


def _client(env):
    mcp = FastMCP("t", stateless_http=True,
                  transport_security=transport_security_from_env(env))
    return TestClient(mcp.streamable_http_app())


def _post(client, host, origin=None):
    headers = dict(_HEADERS, Host=host)
    if origin:
        headers["Origin"] = origin
    return client.post("/mcp", json=_INIT, headers=headers)


def test_the_sdk_enforces_the_lists_on_mcp():
    env = {"MCP_ALLOWED_HOSTS": "localhost",
           "MCP_ALLOWED_ORIGINS": "http://localhost:8080"}
    with _client(env) as c:
        assert _post(c, "localhost").status_code == 200  # no Origin: headless client
        assert _post(c, "localhost", "http://localhost:8080").status_code == 200
        # A rebound page: its own hostname now points at 127.0.0.1.
        assert _post(c, "evil.example:8080", "http://evil.example:8080").status_code == 421
        # The right Host but a foreign page's Origin.
        assert _post(c, "localhost", "https://evil.example").status_code == 403


def test_unset_keeps_todays_behaviour():
    with _client({}) as c:
        assert _post(c, "duckdb-mcp.nrp-nautilus.io", "https://anything.example").status_code == 200
