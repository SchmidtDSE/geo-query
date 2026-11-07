"""Named bearer tokens for the MCP server (#454).

Each caller (in practice one per Kubernetes namespace of apps) holds its own
token, so one can be revoked without rotating the others, and logs can say who
sent a request. Tokens come from a directory with one file per token, named for
the caller: a Kubernetes Secret mounted as a volume has exactly that shape. The
older single MCP_AUTH_TOKEN still works, under the name "default".
"""
import hmac
import os
import sys
from contextvars import ContextVar

# Name of the token that authorised the current request ("-" when auth is off
# or the route is exempt). Set by the auth middleware; read for log lines.
CALLER: ContextVar[str] = ContextVar("mcp_caller", default="-")


def load_tokens(tokens_dir: str = "", single: str = "") -> dict[str, str]:
    """Return {name: token} from `tokens_dir` plus the legacy single token.

    Skips hidden entries, which covers the `..data` links Kubernetes puts in a
    mounted Secret, and empty files. Never prints a token.
    """
    tokens: dict[str, str] = {}
    if tokens_dir:
        if not os.path.isdir(tokens_dir):
            print(f"⚠️ MCP_AUTH_TOKENS_DIR {tokens_dir!r} is not a directory", file=sys.stderr)
        else:
            for name in sorted(os.listdir(tokens_dir)):
                path = os.path.join(tokens_dir, name)
                if name.startswith(".") or not os.path.isfile(path):
                    continue
                try:
                    with open(path) as f:
                        token = f.read().strip()
                except OSError as e:
                    print(f"⚠️ MCP token {name!r} unreadable: {e.strerror}", file=sys.stderr)
                    continue
                if token:
                    tokens[name] = token
    single = (single or "").strip()
    if single and "default" not in tokens:
        tokens["default"] = single
    seen: dict[str, str] = {}
    for name, token in tokens.items():
        if token in seen:
            print(f"⚠️ MCP tokens {seen[token]!r} and {name!r} are identical; "
                  f"requests will be attributed to one of them", file=sys.stderr)
        seen[token] = name
    return tokens


def match_token(supplied: str, tokens: dict[str, str]) -> str | None:
    """Return the name of the token equal to `supplied`, or None.

    Compares against every entry in constant time per entry, without stopping
    at the first match, so timing does not reveal which entry matched.
    """
    if not supplied:
        return None
    found = None
    for name, token in tokens.items():
        if hmac.compare_digest(supplied.encode(), token.encode()):
            found = name
    return found
