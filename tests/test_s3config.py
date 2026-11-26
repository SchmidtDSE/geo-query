"""Tests for the s3config source registry (#264): data-driven href rewriting,
metadata rerouting, per-source secrets, and route hints for unknown hosts."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import s3config


class TestGetSources:
    def test_builtins_present(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        names = [s["name"] for s in s3config.get_sources()]
        assert "nrp_ceph" in names
        assert "source_coop" in names

    def test_env_appends_new_source(self, monkeypatch):
        monkeypatch.setenv(
            "S3_SOURCES",
            '[{"name": "campus_minio", "https_prefix": "https://minio.example.edu/",'
            ' "s3_prefix": "s3://", "secret": {"endpoint": "minio.example.edu",'
            ' "scope": "s3://mirror-data"}}]',
        )
        sources = {s["name"]: s for s in s3config.get_sources()}
        assert "campus_minio" in sources
        assert "source_coop" in sources  # built-ins retained

    def test_env_merges_over_builtin_by_name(self, monkeypatch):
        monkeypatch.setenv(
            "S3_SOURCES",
            '[{"name": "source_coop", "s3_prefix": "s3://other-mirror/"}]',
        )
        src = {s["name"]: s for s in s3config.get_sources()}["source_coop"]
        assert src["s3_prefix"] == "s3://other-mirror/"
        # Untouched keys survive the merge.
        assert src["https_prefix"] == "https://data.source.coop/"

    def test_env_disabled_removes_builtin(self, monkeypatch):
        monkeypatch.setenv("S3_SOURCES", '[{"name": "source_coop", "disabled": true}]')
        names = [s["name"] for s in s3config.get_sources()]
        assert "source_coop" not in names

    def test_malformed_env_ignored_with_warning(self, monkeypatch, capsys):
        monkeypatch.setenv("S3_SOURCES", "not json")
        names = [s["name"] for s in s3config.get_sources()]
        assert "nrp_ceph" in names  # built-ins still served
        assert "S3_SOURCES" in capsys.readouterr().err


class TestRewriteHref:
    def test_ceph_strip(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        assert (
            s3config.rewrite_href("https://s3-west.nrp-nautilus.io/public-carbon/x.parquet")
            == "s3://public-carbon/x.parquet"
        )

    def test_source_coop_gateway(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        assert (
            s3config.rewrite_href("https://data.source.coop/cboettig/carbon/h0=*/d.parquet")
            == "s3://us-west-2.opendata.source.coop/cboettig/carbon/h0=*/d.parquet"
        )

    def test_unknown_host_passthrough(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        href = "https://example.com/other.parquet"
        assert s3config.rewrite_href(href) == href

    def test_env_source_rewrites(self, monkeypatch):
        monkeypatch.setenv(
            "S3_SOURCES",
            '[{"name": "campus_minio", "https_prefix": "https://minio.example.edu/",'
            ' "s3_prefix": "s3://"}]',
        )
        assert (
            s3config.rewrite_href("https://minio.example.edu/mirror-x/y.parquet")
            == "s3://mirror-x/y.parquet"
        )


class TestSourceSecretSql:
    def test_source_coop_secret_generated(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        stmts = s3config.source_secret_sql()
        coop = [s for s in stmts if "SECRET source_coop" in s]
        assert len(coop) == 1
        assert "SCOPE 's3://us-west-2.opendata.source.coop'" in coop[0]
        assert "REGION 'us-west-2'" in coop[0]
        assert "ENDPOINT 's3.us-west-2.amazonaws.com'" in coop[0]

    def test_nrp_has_no_secret(self, monkeypatch):
        """NRP paths route via the deployment default `s3` secret — the registry
        entry must not create a competing one."""
        monkeypatch.delenv("S3_SOURCES", raising=False)
        assert not any("nrp" in s for s in s3config.source_secret_sql())

    def test_env_source_secret_with_ssl_inference(self, monkeypatch):
        monkeypatch.setenv(
            "S3_SOURCES",
            '[{"name": "my-mirror!", "secret": {"endpoint": "minio.example.edu",'
            ' "scope": "s3://mirror-data"}}]',
        )
        stmt = [s for s in s3config.source_secret_sql() if "minio.example.edu" in s][0]
        assert "SECRET my_mirror_ " in stmt  # name sanitized to an identifier
        assert "USE_SSL 'true'" in stmt  # inferred: non-rook = https

    def test_values_are_quote_escaped(self, monkeypatch):
        monkeypatch.setenv(
            "S3_SOURCES",
            '[{"name": "q", "secret": {"endpoint": "e.example", "scope": "s3://a\'b"}}]',
        )
        stmt = [s for s in s3config.source_secret_sql() if "e.example" in s][0]
        assert "s3://a''b" in stmt


# --- s3_endpoint allow-list -------------------------------------------------
import pytest as _pytest
from s3config import allowed_client_endpoints, check_client_endpoint


@_pytest.mark.parametrize("host", [
    "s3-west.nrp-nautilus.io", "s3.carlboettiger.info", "minio.carlboettiger.info", "rook-ceph-rgw-nautiluss3.rook",
    "s3.us-west-2.amazonaws.com", "s3.amazonaws.com", "S3-West.NRP-Nautilus.io",
])
def test_known_endpoints_accepted(host):
    check_client_endpoint(host)


@_pytest.mark.parametrize("host", [
    "open-llm-proxy.biodiversity.svc.cluster.local", "10.96.0.1", "localhost",
    "s3-west.nrp-nautilus.io:8080", "https://s3-west.nrp-nautilus.io",
    "s3-west.nrp-nautilus.io/x", "evil.amazonaws.com.attacker.io", "kubernetes.default", "",
])
def test_other_endpoints_rejected(host):
    with _pytest.raises(ValueError):
        check_client_endpoint(host)


def test_allowed_endpoints_extend_from_env_and_registry(monkeypatch):
    monkeypatch.setenv("S3_ALLOWED_ENDPOINTS", "minio.example.org, other.example.edu")
    monkeypatch.setenv("S3_SOURCES", '[{"name": "x", "s3_prefix": "s3://x/", "secret": {"endpoint": "reg.example.net", "scope": "s3://x"}}]')
    hosts = allowed_client_endpoints()
    assert {"minio.example.org", "other.example.edu", "reg.example.net"} <= hosts
    check_client_endpoint("reg.example.net")


class TestRewriteSqlHrefs:
    """https paths from any STAC client run as-is in `query` (#528)."""

    def test_rewrites_registered_prefixes_in_literals(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        sql = (
            "SELECT COUNT(*) FROM read_parquet('https://s3-west.nrp-nautilus.io/public-wdpa/wdpa/hex/h0=*/data_0.parquet') a "
            "JOIN read_parquet(['https://data.source.coop/cboettig/carbon/h0=*/d.parquet']) b USING (h8, h0)"
        )
        out, n = s3config.rewrite_sql_hrefs(sql)
        assert n == 2
        assert "'s3://public-wdpa/wdpa/hex/h0=*/data_0.parquet'" in out
        assert "'s3://us-west-2.opendata.source.coop/cboettig/carbon/h0=*/d.parquet'" in out
        assert "https://" not in out

    def test_leaves_other_text_alone(self, monkeypatch):
        monkeypatch.delenv("S3_SOURCES", raising=False)
        sql = (
            "SELECT 'see https://example.com/x' AS note, "
            "'https://example.com/other.parquet' AS unknown_host, "
            "'s3://public-carbon/x.parquet' AS already_s3 "
            "-- from https://s3-west.nrp-nautilus.io/public-carbon/stac.json"
        )
        out, n = s3config.rewrite_sql_hrefs(sql)
        assert n == 0
        assert out == sql

    def test_env_source(self, monkeypatch):
        monkeypatch.setenv(
            "S3_SOURCES",
            '[{"name": "campus_minio", "https_prefix": "https://minio.example.edu/",'
            ' "s3_prefix": "s3://"}]',
        )
        out, n = s3config.rewrite_sql_hrefs("FROM 'https://minio.example.edu/b/k.parquet'")
        assert (out, n) == ("FROM 's3://b/k.parquet'", 1)
