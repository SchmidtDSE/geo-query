"""Named MCP bearer tokens (#454)."""
import os

import pytest

from auth import caller_label, load_tokens, match_token


def _secret_dir(tmp_path, entries):
    """Lay out a directory the way Kubernetes mounts a Secret: real files under
    a hidden ..data dir, with visible symlinks named for each key."""
    data = tmp_path / "..2026_11_07" 
    data.mkdir()
    (tmp_path / "..data").symlink_to(data.name)
    for name, value in entries.items():
        (data / name).write_text(value + "\n")
        (tmp_path / name).symlink_to(os.path.join("..data", name))
    return str(tmp_path)


def test_load_tokens_reads_mounted_secret(tmp_path):
    d = _secret_dir(tmp_path, {"biodiversity": "tok-b", "schmidtdse": "tok-s"})
    assert load_tokens(d) == {"biodiversity": "tok-b", "schmidtdse": "tok-s"}


def test_load_tokens_skips_empty_and_adds_legacy_default(tmp_path):
    d = _secret_dir(tmp_path, {"a": "tok-a", "empty": "  "})
    assert load_tokens(d, single="tok-legacy") == {"a": "tok-a", "default": "tok-legacy"}


def test_load_tokens_missing_dir_keeps_legacy(tmp_path):
    assert load_tokens(str(tmp_path / "nope"), single="t") == {"default": "t"}
    assert load_tokens("", single="") == {}


def test_match_token_names_the_caller():
    tokens = {"biodiversity": "tok-b", "schmidtdse": "tok-s"}
    assert match_token("tok-s", tokens) == "schmidtdse"
    assert match_token("tok-b", tokens) == "biodiversity"
    assert match_token("tok-x", tokens) is None
    assert match_token("", tokens) is None
    assert match_token("", {"x": ""}) is None


def test_load_tokens_skips_unreadable_file(tmp_path):
    d = _secret_dir(tmp_path, {"ok": "tok-ok", "locked": "tok-locked"})
    os.chmod(os.path.realpath(os.path.join(d, "locked")), 0)
    if os.access(os.path.join(d, "locked"), os.R_OK):  # running as root
        return
    assert load_tokens(d) == {"ok": "tok-ok"}


def test_caller_label_adds_the_app_name():
    assert caller_label("public", "padus") == "public/padus"
    assert caller_label("public", "bosl-high-seas") == "public/bosl-high-seas"
    assert caller_label("public", "  tpl-ca ") == "public/tpl-ca"


def test_caller_label_without_the_header_is_the_token_name():
    assert caller_label("coasts", None) == "coasts"
    assert caller_label("coasts", "") == "coasts"
    assert caller_label("coasts", "   ") == "coasts"


@pytest.mark.parametrize("hostile", [
    "padus\n🔍 Executing [admin]: DROP",
    "padus\r\nX: y",
    "a" * 64,
    "-padus",
    "padus/../x",
    "pad us",
    "padus]",
    "\x1b[31mred",
])
def test_caller_label_never_echoes_a_hostile_value(hostile):
    assert caller_label("public", hostile) == "public/?"
