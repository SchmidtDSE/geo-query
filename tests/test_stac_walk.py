"""The STAC catalog walk on real files (#433), without mocks.

tests/fixtures/portolan is the Portolan-shaped tree from geo-query-wasm: relative
hrefs and no self links, theme Catalogs between the root and its Collections,
child links without ids, one Collection linked from two Catalogs, and two ids
ending in "wards". The deeper tree is written per test.
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import stac

PORTOLAN = (Path(__file__).parent / "fixtures" / "portolan" / "catalog.json").as_uri()


@pytest.fixture(autouse=True)
def _keep_module_caches():
    """Tests that load the default catalog must not leak it into other tests."""
    saved = (dict(stac.STAC_DATASETS), dict(stac._STAC_RAW),
             list(stac._STAC_INDEX), dict(stac.STAC_LOAD_ERRORS))
    yield
    for cache, old in zip((stac.STAC_DATASETS, stac._STAC_RAW, stac.STAC_LOAD_ERRORS),
                          (saved[0], saved[1], saved[3])):
        cache.clear()
        cache.update(old)
    stac._STAC_INDEX[:] = saved[2]


def _walk(url=PORTOLAN):
    datasets, raw, index, errors = stac._walk_catalog(url, None)
    return datasets, raw, index, errors


# --- Portolan ----------------------------------------------------------------

def test_portolan_loads_every_collection_through_catalog_nodes():
    datasets, _raw, index, _errors = _walk()
    leaves = {cid for _, cid, group in index if not group}
    groups = {cid for _, cid, group in index if group}
    assert leaves == {"government/wards", "government/wards-2010", "elections/wards"}
    assert groups == {"government", "elections"}
    assert set(datasets) == leaves | groups


def test_collection_linked_from_two_catalogs_is_listed_once():
    _datasets, _raw, index, _errors = _walk()
    assert [cid for _, cid, _ in index].count("government/wards-2010") == 1


def test_missing_child_is_an_error_not_a_crash():
    _datasets, _raw, _index, errors = _walk()
    assert any("missing" in key for key in errors)


def test_relative_asset_href_resolves_against_the_collection_url():
    _datasets, raw, _index, _errors = _walk()
    expected = (Path(__file__).parent / "fixtures" / "portolan" / "government" / "wards"
                / "wards.parquet").as_uri()
    assert raw["government/wards"]["assets"]["wards"]["href"] == expected


def test_absolute_asset_href_is_kept_with_its_glob():
    datasets, _raw, _index, _errors = _walk()
    assert ("s3://us-west-2.opendata.source.coop/example/portolan/government/wards-2010/"
            "hex/h0=*/data_0.parquet") in datasets["government/wards-2010"]


def test_listing_shows_groups_as_headings_with_datasets_beneath():
    out = stac.list_datasets(catalog_url=PORTOLAN)
    lines = out.splitlines()
    assert lines[0] == "# Available Datasets (3 datasets in 2 groups)"
    gov = next(i for i, line in enumerate(lines) if "(group `government`)" in line)
    assert lines[gov].startswith("- ")
    assert lines[gov + 1].startswith("  - **government/wards**")


def test_full_id_and_unique_last_segment_both_resolve():
    assert "Ward Boundaries (2010)" in stac.get_dataset("government/wards-2010", catalog_url=PORTOLAN)
    assert "Ward Boundaries (2010)" in stac.get_dataset("wards-2010", catalog_url=PORTOLAN)


def test_ambiguous_last_segment_lists_the_full_ids():
    out = stac.get_dataset("wards", catalog_url=PORTOLAN)
    assert "matches more than one" in out
    assert "`government/wards`" in out and "`elections/wards`" in out
    err = stac.get_collection("wards", catalog_url=PORTOLAN)["error"]
    assert "`government/wards`" in err and "`elections/wards`" in err


def test_group_details_list_its_datasets():
    out = stac.get_dataset("elections", catalog_url=PORTOLAN)
    assert "Sub-datasets (2)" in out
    assert "`elections/wards`" in out and "`government/wards-2010`" in out


def test_group_collection_dict_is_a_catalog_with_children():
    d = stac.get_collection("government", catalog_url=PORTOLAN)
    assert d["type"] == "Catalog"
    assert [c["id"] for c in d["children"]] == ["government/wards", "government/wards-2010"]


def test_default_catalog_listing_uses_the_index(monkeypatch):
    monkeypatch.setattr(stac, "STAC_CATALOG_URL", PORTOLAN)
    monkeypatch.delenv("STAC_PUBLIC_CATALOG_URL", raising=False)
    stac.fetch_stac_catalog()
    out = stac.list_datasets()
    assert "(group `elections`)" in out
    assert "  - **elections/wards**" in out


# --- Depth -------------------------------------------------------------------

def _write(path: Path, doc: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))


def _catalog(cid, children, extra_links=()):
    return {
        "type": "Catalog", "stac_version": "1.1.0", "id": cid, "description": cid,
        "links": [{"rel": "child", "href": h} for h in children] + list(extra_links),
    }


def _collection(cid, children=(), assets=None):
    return {
        "type": "Collection", "stac_version": "1.1.0", "id": cid, "description": cid,
        "license": "other",
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]},
                   "temporal": {"interval": [[None, None]]}},
        "links": [{"rel": "child", "href": h} for h in children],
        "assets": assets or {},
    }


@pytest.fixture
def deep_tree(tmp_path):
    """root Catalog → Collection without assets → Catalog → Catalog → leaf Collection.

    The leaf is four levels below the root, and the innermost Catalog links back
    to the root, a cycle the walk must not follow.
    """
    _write(tmp_path / "catalog.json", _catalog("root", ["./a/collection.json"]))
    _write(tmp_path / "a" / "collection.json", _collection("a", ["./b/catalog.json"]))
    _write(tmp_path / "a" / "b" / "catalog.json", _catalog("b", ["./c/catalog.json"]))
    _write(tmp_path / "a" / "b" / "c" / "catalog.json",
           _catalog("c", ["./leaf/collection.json", "../../../catalog.json"]))
    _write(tmp_path / "a" / "b" / "c" / "leaf" / "collection.json",
           _collection("deep-leaf", assets={"data": {
               "href": "./data.parquet", "type": "application/vnd.apache.parquet"}}))
    return (tmp_path / "catalog.json").as_uri()


def test_leaf_four_levels_down_loads(deep_tree):
    datasets, _raw, index, errors = _walk(deep_tree)
    assert errors == {}
    assert "deep-leaf" in datasets
    assert [(depth, cid, group) for depth, cid, group in index] == [
        (0, "a", True), (1, "b", True), (2, "c", True), (3, "deep-leaf", False),
    ]


def test_group_details_reach_nested_datasets(deep_tree):
    datasets, _raw, _index, _errors = _walk(deep_tree)
    assert "`deep-leaf`" in datasets["a"]


def test_depth_cap_records_an_error(deep_tree, monkeypatch):
    monkeypatch.setattr(stac, "_STAC_MAX_DEPTH", 2)
    datasets, _raw, _index, errors = _walk(deep_tree)
    assert "deep-leaf" not in datasets
    assert any("DepthLimit" in reason for reason in errors.values())


# --- Inline catalogs ---------------------------------------------------------

def test_inline_catalog_nests_to_any_depth():
    catalog = {"id": "inline", "children": [
        {"type": "Catalog", "id": "theme", "children": [
            {"id": "sub", "children": [
                {"id": "leaf", "assets": {"d": {
                    "href": "s3://bucket/leaf.parquet", "type": "application/vnd.apache.parquet"}}},
            ]},
        ]},
    ]}
    out = stac.list_datasets(catalog=catalog)
    assert "# Available Datasets (1 dataset in 2 groups)" in out
    assert "    - **leaf**" in out



def test_duplicate_group_id_keeps_its_datasets():
    def leaf(i):
        return {"id": i, "title": i, "assets": {"d": {
            "href": f"https://x.org/{i}.parquet", "type": "application/vnd.apache.parquet"}}}
    catalog = {"id": "c", "children": [
        {"type": "Catalog", "id": "hawaii", "children": [leaf("coral-reefs")]},
        {"type": "Catalog", "id": "coral-reefs", "children": [leaf("reef-a"), leaf("reef-b")]},
    ]}
    out = stac.list_datasets(catalog=catalog)
    assert "# Available Datasets (3 datasets in 1 group)" in out
    assert "\n- **reef-a**" in out and "\n- **reef-b**" in out
    datasets, _index = stac._render_inline_catalog(catalog)
    for i in ("reef-a", "reef-b"):
        assert stac._lookup(datasets, i)[0] is not None

# --- No network beyond the documents themselves ---------------------------------

def test_root_link_to_another_host_is_never_fetched(tmp_path):
    """A mirror's documents keep `root` pointing at the original catalog; reading
    them must not depend on that host (the GLEN source.coop mirror points at NRP)."""
    away = [{"rel": "root", "href": "https://unreachable.invalid/catalog.json"}]
    _write(tmp_path / "catalog.json", _catalog("root", ["./g/catalog.json"], away))
    _write(tmp_path / "g" / "catalog.json", _catalog("g", ["./leaf/collection.json"], away))
    leaf = _collection("leaf", assets={"d": {"href": "./d.parquet",
                                             "type": "application/vnd.apache.parquet"}})
    leaf["links"] += away
    _write(tmp_path / "g" / "leaf" / "collection.json", leaf)
    datasets, _raw, _index, errors = _walk((tmp_path / "catalog.json").as_uri())
    assert errors == {}
    assert set(datasets) == {"g", "leaf"}


def test_collection_dict_links_are_absolute():
    d = stac.get_collection("government/wards", catalog_url=PORTOLAN)
    readme = next(link for link in d["links"] if link["rel"] == "describedby")
    assert readme["href"] == (Path(__file__).parent / "fixtures" / "portolan" / "government"
                              / "wards" / "README.md").as_uri()
