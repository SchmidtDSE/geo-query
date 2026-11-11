"""
STAC catalog integration — fetches dataset metadata at startup and exposes
it as MCP resources for client discovery.

The STAC catalog describes harmonized, cloud-native datasets that are
co-located with this MCP server on the same Kubernetes cluster, enabling
high-speed internal reads via the Ceph S3 endpoint.
"""

import os
import sys
import threading
import pystac
import requests
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from urllib.parse import urljoin
from pystac.stac_io import DefaultStacIO

import s3config


STAC_CATALOG_URL = os.environ.get(
    "STAC_CATALOG_URL",
    "https://s3-west.nrp-nautilus.io/public-data/stac/catalog.json",
)


def default_catalog_token() -> str | None:
    """Bearer token for *this deployment's* catalog, when that catalog is private.

    The env-side replacement for the old model-supplied `catalog_token` tool
    argument (#420). Catalog identity and its credential are deployment
    properties — read from the environment at call time, exactly as S3
    credentials are — never re-decided per call by the model.
    """
    return os.environ.get("STAC_CATALOG_TOKEN", "").strip() or None


def discovery_enabled() -> bool:
    """Whether this deployment exposes whole-catalog discovery (#420).

    `browse_stac_catalog` and the `catalog://list` resource enumerate the whole
    configured catalog. That is right for the shared public server — where the
    configured catalog *is* the public catalog — and wrong for a curated app
    deployment, which wants the model to see only the collections the app
    configures. Default on (back-compat for clients pinned too old to notice a
    tool disappearing); a deployment opts out with `STAC_DISCOVERY=0`.
    """
    return os.environ.get("STAC_DISCOVERY", "1").strip().lower() not in (
        "0", "false", "no", "off",
    )


def _discovery_hint(noun: str) -> str:
    """Trailing "use browse_stac_catalog" hint, omitted where it isn't registered."""
    return f" Use browse_stac_catalog to list available {noun}." if discovery_enabled() else ""


def _searched_catalog(catalog_url: str = None) -> str:
    """Name the catalog a lookup actually consulted, for a not-found message.

    Since #420 a caller cannot retarget the catalog per call, and an argument
    absent from the schema is dropped during validation rather than rejected —
    so a direct MCP consumer still sending `catalog_url` is answered from the
    deployment's catalog with no signal that its own was never consulted. Naming
    the catalog in the one response such a consumer will actually read turns that
    silence into something diagnosable. Quote the *client-reachable* URL (#346):
    on a mirror head the address this process reads may not resolve for them.
    """
    return catalog_url or f"this server's catalog ({public_catalog_url()})"


def public_catalog_url() -> str:
    """The catalog URL a *client* should put in its own config.

    `STAC_CATALOG_URL` is the address this server reads, which on some
    deployments is in-cluster only (cirrus reads the MinIO mirror at
    `minio-svc.minio.svc.cluster.local`). Advertising that verbatim leaves a
    client unable to self-configure from the server (#346). Same pattern as
    `MCP_PUBLIC_BASE_URL` in `tiles/pyramid.py`: read at call time, and default
    to the internal URL so NRP prod — where the two are identical — is
    unchanged.
    """
    return os.environ.get("STAC_PUBLIC_CATALOG_URL", "").strip() or STAC_CATALOG_URL

# Backwards-compatible: STAC_TIMEOUT alone still works as a single knob; the two
# new vars override it when set. Root is a hard prerequisite (generous timeout);
# children are individually skippable (tight timeout). See
# docs/superpowers/specs/2026-04-16-stac-catalog-resilience-design.md.
_STAC_ROOT_TIMEOUT = int(
    os.environ.get("STAC_ROOT_TIMEOUT", os.environ.get("STAC_TIMEOUT", "15"))
)
_STAC_CHILD_TIMEOUT = int(
    os.environ.get("STAC_CHILD_TIMEOUT", os.environ.get("STAC_TIMEOUT", "5"))
)
_STAC_FETCH_CONCURRENCY = int(os.environ.get("STAC_FETCH_CONCURRENCY", "16"))

# Child fetches that time out on the first pass are retried once, each with a
# longer per-child timeout. Rescues tail-latency failures without leaving
# otherwise-healthy collections out of the catalog. A persistent failure
# (both attempts time out) stays in STAC_LOAD_ERRORS.
_STAC_CHILD_RETRY_TIMEOUT = int(os.environ.get("STAC_CHILD_RETRY_TIMEOUT", "8"))

# Legacy alias retained so external callers (if any) that read the old name still work.
_STAC_TIMEOUT = _STAC_CHILD_TIMEOUT

# Seconds between background re-walks of the default catalog (see #337). The
# startup snapshot goes stale the moment a new asset/dataset is published to S3;
# the on-miss re-fetch (#11) only rescues *new* dataset ids, never an updated
# asset on an already-cached collection (a cache HIT). A per-pod timer keeps
# every replica converged without an external trigger or a rollout. `0` (or
# negative) disables the thread — used by tests and one-shot tooling.
_STAC_REFRESH_INTERVAL = int(os.environ.get("STAC_REFRESH_INTERVAL", "900"))

# Guards the atomic swap of the shared module caches (STAC_DATASETS / _STAC_RAW /
# STAC_LOAD_ERRORS) against concurrent readers. The clear()+update() swap has a
# transient-empty window a concurrent request could otherwise observe as a
# spurious "not found"; the background refresh (#337) makes that race routine.
# Only the fast in-memory swap and the shared-dict reads take the lock — the slow
# HTTP walk in fetch_stac_catalog runs lock-free.
_STAC_LOCK = threading.RLock()

# Navigational STAC link rel types excluded from _collection_to_dict output.
_NAV_RELS = {"root", "parent", "self", "child", "item"}


def _fuzzy_lookup(mapping: dict, key: str):
    """Look up *key* in *mapping*: exact → prefix → substring. Returns None if no match."""
    if key in mapping:
        return mapping[key]
    low = key.lower()
    # Prefix match (e.g. "census" matches "census-2024-state")
    for k, v in mapping.items():
        if k.lower().startswith(low):
            return v
    # Substring match
    for k, v in mapping.items():
        if low in k.lower():
            return v
    return None


def _child_identifier(href: str, title_hint: str = None, fetched_id: str = None) -> str:
    """Best-effort identifier for a STAC child — used for error reporting when the
    real collection `id` may not be available (fetch failed before JSON parse).

    Precedence:
    1. `fetched_id` if given — the real collection id
    2. The last path segment of the href (directory name for `.../dir/stac-collection.json`,
       or the file stem for `.../dir/name.json` / `.../dir/name-stac.json`)
    3. Optionally augmented with `title_hint` when present and non-redundant
    """
    if fetched_id:
        return fetched_id
    # Strip trailing slash, then take the last non-empty segment.
    path = href.rstrip("/")
    segments = [s for s in path.split("/") if s]
    if not segments:
        return title_hint or href
    tail = segments[-1]
    # If the tail is a generic collection-json filename, use the parent directory.
    if tail in ("stac-collection.json", "catalog.json"):
        tail = segments[-2] if len(segments) >= 2 else tail
    else:
        # Strip common suffixes so "foo-stac.json" / "foo.json" become "foo-stac" / "foo".
        for suffix in (".json",):
            if tail.endswith(suffix):
                tail = tail[: -len(suffix)]
                break
    if title_hint and title_hint.lower() not in tail.lower():
        return f"{tail} ({title_hint})"
    return tail


class _TimeoutStacIO(DefaultStacIO):
    def __init__(self, token: str = None, timeout: int = None):
        self._token = token
        self._timeout = timeout if timeout is not None else _STAC_CHILD_TIMEOUT

    def read_text_from_href(self, href: str) -> str:
        # Registry-driven reroute (s3config): e.g. NRP external SSL hrefs are
        # fetched via the in-cluster endpoint for speed.
        href = s3config.metadata_href(href)
        if href.startswith("http"):
            headers = {"Authorization": f"Bearer {self._token}"} if self._token else {}
            resp = requests.get(href, timeout=self._timeout, headers=headers)
            resp.raise_for_status()
            return resp.text
        return super().read_text_from_href(href)


pystac.StacIO.set_default(_TimeoutStacIO)


def _href_to_s3(href: str) -> str:
    """Convert an HTTPS S3 URL to an s3:// path for DuckDB read_parquet().

    Registry-driven (s3config, #264): built-ins cover the primary NRP Ceph
    endpoint and the source.coop mirror (whose `data.source.coop` HTTPS gateway
    cannot list/glob — the AWS-bucket form can, see #260); deployments add
    sources via S3_SOURCES. Hrefs on unknown hosts pass through unchanged —
    the renderers surface a per-request routing hint for those instead
    (s3config.route_hint) rather than guessing a rewrite.
    """
    return s3config.rewrite_href(href)


def _format_columns(table_cols: list) -> list[str]:
    """Format a list of table:columns dicts into markdown lines."""
    if not table_cols:
        return []
    display_cols = [
        c for c in table_cols
        if c.get("name", "").lower() not in ("geometry", "geom", "bbox")
    ]

    lines = []
    for c in display_cols:
        desc = f" — {c['description']}" if c.get("description") else ""
        lines.append(f"    - `{c['name']}` ({c.get('type', '?')}){desc}")
        values = c.get("values")
        if values:
            lines.append(f"      values: {list(values)}")
    return lines


def _is_queryable_asset(href: str, atype: str) -> bool:
    """True for assets the `query` tool reads via read_parquet()."""
    if "pmtiles" in atype or href.endswith(".pmtiles"):
        return False
    if "tif" in atype or href.endswith(".tif") or href.endswith(".tiff"):
        return False
    return (
        "parquet" in atype or href.endswith(".parquet")
        or href.endswith("/") or "/hex/" in href
    )


def _asset_column_summary(table_cols: list) -> str:
    """One-line `` `name` (type) `` list of an asset's columns (geometry included).

    The per-asset delta view (#303): it makes each queryable asset's column set
    unambiguous — which asset carries the hex's `h0/h8/h10`, which one keeps
    `geometry` — without repeating every column's prose. The full descriptions
    and categorical `values` live once in the shared block appended by
    `_extract_parquet_assets`.
    """
    parts = []
    for c in table_cols:
        name = c.get("name")
        if not name:
            continue
        t = c.get("type")
        parts.append(f"`{name}` ({t})" if t else f"`{name}`")
    return ", ".join(parts)


def _extract_parquet_assets(col, compact: bool = False) -> list[str]:
    """Extract parquet/hex asset lines from a collection's assets.

    Column *descriptions* are deduplicated (#303): rather than reprinting every
    column's prose once per asset — 2-3× on the common flat-GeoParquet + hex
    dataset, which pushed the largest schemas past geo-agent's 16k tool-result
    cap and truncated them — each asset line lists only its own column
    names/types, and every column's description + categorical `values` is
    rendered exactly once in a shared block appended after the asset lines.

    *compact* (#305): emit only each asset's ``read_parquet(...)`` path, routing
    hint, description, and extension fields — NO per-asset column names and NO
    shared description block. Used by the parent/sub-dataset index, where the
    columns would re-dump every child's schema (20-50k parent payloads over the
    16k cap) and duplicate what the per-child ``get_stac_details`` call returns.
    The caller appends an explicit per-child "call get_stac_details(<id>)"
    pointer instead.
    """
    assets = []
    shared_cols: dict = {}  # name -> col dict, in first-seen order (union of all assets)
    for asset_id, asset in (getattr(col, "assets", None) or {}).items():
        href = asset.href
        atype = asset.media_type or ""
        title = asset.title or asset_id

        if _is_queryable_asset(href, atype):
            s3 = _href_to_s3(href)
            # Unknown host (no registry rewrite): render the derived s3:// form —
            # the only form that can glob — with the per-request routing params
            # the caller must pass to `query` (see s3config.route_hint, #264).
            hint = s3config.route_hint(s3) if s3.startswith("http") else None
            if hint:
                s3 = hint["path"]
            if s3.endswith("/"):
                s3 = s3.rstrip("/") + "/**"
            size = asset.extra_fields.get("file:size")
            size_note = f" ({size/1024**3:.2f} GiB)" if size and size > 1024**2 else ""
            assets.append(f"  - {title}{size_note}: `read_parquet('{s3}')`")
            if hint:
                assets.append(
                    f"    - ⚠️ non-default endpoint (derived from `{href}` assuming "
                    f"path-style S3): call `query` with s3_endpoint='{hint['endpoint']}', "
                    f"s3_scope='{hint['scope']}' (anonymous; a private bucket's credentials come from the deployment)"
                )
            # Asset-level description — carries per-asset facts (e.g. "this file has
            # duplicate rows per feature; dedup by <id>") that the model needs at
            # column-choice time. Without this line they never reach the prompt.
            if asset.description:
                assets.append(f"    - {asset.description}")
            # Asset-level STAC extension fields (h3, raster, vector)
            for ext_key in (
                "h3:native_resolution",
                "h3:parent_resolutions",
                "raster:bands",
                "vector:layers",
            ):
                ext_val = asset.extra_fields.get(ext_key)
                if ext_val is not None:
                    assets.append(f"    - {ext_key}: {ext_val}")
            # Per-asset columns are omitted in compact mode (parent index, #305):
            # the caller adds a per-child get_stac_details pointer instead.
            if compact:
                continue
            # Per-asset column *set* only (names/types); descriptions are shared below.
            # An asset without its own list is described by the collection's:
            # under Portolan the collection holds the one column dictionary
            # (PORTO-FMT-046) and an asset lists its own columns only where its
            # schema differs (PORTO-FMT-048).
            asset_cols = asset.extra_fields.get("table:columns") or col.extra_fields.get("table:columns") or []
            summary = _asset_column_summary(asset_cols)
            if summary:
                assets.append(f"    - columns: {summary}")
            # Accumulate the union for the single shared description block. First
            # asset to define a name wins; later assets backfill any field it was
            # missing (description/values/type) so the block is maximally complete.
            for c in asset_cols:
                name = c.get("name")
                if not name:
                    continue
                if name not in shared_cols:
                    shared_cols[name] = dict(c)
                else:
                    existing = shared_cols[name]
                    for k in ("description", "values", "type"):
                        if not existing.get(k) and c.get(k):
                            existing[k] = c[k]

    if not compact:
        # Fill what the assets leave out (description, values, type) from the
        # collection's dictionary, matching by name: a per-asset list may carry
        # only names and types (PORTO-FMT-048).
        for c in col.extra_fields.get("table:columns") or []:
            existing = shared_cols.get(c.get("name"))
            if existing is None:
                continue
            for k in ("description", "values", "type"):
                if not existing.get(k) and c.get(k):
                    existing[k] = c[k]
        block = _format_columns(list(shared_cols.values()))
        if block:
            assets.append("\nColumn descriptions (shared across the assets above):")
            assets.extend(block)
    return assets


def _format_collection(col, sub_children: list = None) -> str:
    """Build a compact markdown summary of one STAC collection.

    If the collection has sub-datasets (e.g. wyoming-wildlife-lands has
    per-species sub-collections), list them as a compact chooser. The catalog
    walk passes every dataset beneath a group, however deep (#433).

    *sub_children* may be passed to avoid a duplicate ``get_children()``
    network call when the caller already fetched them.
    """
    lines = []
    lines.append(f"**{col.title or col.id}**")
    lines.append(f"Collection ID: `{col.id}`")
    self_href = col.get_self_href()
    if self_href:
        lines.append(f"collection_url: `{self_href}`")
    if col.description:
        lines.append(col.description)

    # Collect parquet assets from this level
    parquet_assets = _extract_parquet_assets(col)

    # No separate collection-level "Key columns" trailer (#303): every
    # queryable asset's columns are rendered once by `_extract_parquet_assets`,
    # which takes the collection's dictionary for an asset without its own
    # list (Portolan PORTO-FMT-046/048), so a trailer would only repeat it.

    # Check for sub-children — some collections (wyoming-wildlife-lands,
    # pad-us, census) group sub-datasets as child collections, each with
    # their own assets and column schemas.
    if sub_children is None:
        sub_children = list(col.get_children())
    if sub_children:
        lines.append(f"\n**Sub-datasets ({len(sub_children)}):**\n")
        for sc in sub_children:
            sc_title = sc.title or sc.id
            # Compact render (#305): paths + extensions only, no columns. The
            # parent index is a chooser — inlining every child's schema re-dumped
            # the whole bucket (20-50k payloads over the 16k cap) and duplicated
            # what the per-child get_stac_details call returns anyway.
            sc_parquet = _extract_parquet_assets(sc, compact=True)
            if sc_parquet:
                lines.append(f"*{sc_title}* (`{sc.id}`):")
                lines.extend(sc_parquet)
                lines.append(f"  - columns: call get_stac_details('{sc.id}')")
            else:
                lines.append(f"*{sc_title}* (`{sc.id}`): call get_stac_details('{sc.id}')")
        lines.append(f"\nCall get_stac_details with a sub-dataset ID (e.g. `{sub_children[0].id}`) for column schemas and details.")
    elif parquet_assets:
        lines.append("\nSQL data (use with `query` tool):")
        lines.extend(parquet_assets)

    return "\n".join(lines)


def _collection_to_dict(col, sub_children=None) -> dict:
    """Convert a pystac Collection to a structured dict for programmatic use.

    All asset hrefs are converted from HTTPS to s3:// form.
    Pass sub_children when already fetched to avoid extra network calls.
    """
    result: dict = {
        "id": col.id,
        "title": col.title,
        "description": col.description,
        "type": "Catalog" if not isinstance(col, pystac.Collection) and isinstance(col, pystac.Catalog) else None,
        "license": getattr(col, "license", None),
        "keywords": list(getattr(col, "keywords", None) or []),
    }

    # Providers
    result["providers"] = [
        {k: v for k, v in {
            "name": p.name,
            "description": getattr(p, "description", None),
            "roles": list(p.roles) if getattr(p, "roles", None) else None,
            "url": getattr(p, "url", None),
        }.items() if v is not None}
        for p in (getattr(col, "providers", None) or [])
    ]

    # Extent
    ext = getattr(col, "extent", None)
    result["extent"] = None if ext is None else {
        "spatial": {"bbox": ext.spatial.bboxes if ext and ext.spatial else []},
        "temporal": {
            "interval": [
                [dt.isoformat() if dt else None for dt in interval]
                for interval in (ext.temporal.intervals if ext and ext.temporal else [])
            ]
        },
    }

    # External links (exclude navigational rel types). Use get_target_str()
    # rather than lnk.href: the latter calls get_root(), which resolves the root
    # link over the network — so rendering an inline collection whose root points
    # at an unreachable catalog (e.g. the source.coop mirror path during a Ceph
    # outage, #260) would crash. get_target_str() returns the raw href, no I/O.
    result["links"] = [
        {k: v for k, v in {
            "rel": lnk.rel,
            "href": _resolve_href(col, lnk.get_target_str()),
            "title": getattr(lnk, "title", None),
        }.items() if v is not None}
        for lnk in (col.links or [])
        if lnk.rel not in _NAV_RELS
    ]

    # Summaries
    if getattr(col, "summaries", None):
        try:
            result["summaries"] = col.summaries.to_dict()
        except Exception:
            result["summaries"] = {}
    else:
        result["summaries"] = {}

    # All assets — every media type, with S3 path conversion
    assets = {}
    for asset_id, asset in (getattr(col, "assets", None) or {}).items():
        a: dict = {
            "href": _href_to_s3(asset.href),
            "type": asset.media_type,
            "title": asset.title,
            "description": asset.description,
        }
        # Queryable asset on a host outside the registry: keep the original href
        # (still fetchable over HTTPS by map clients) and attach the derived
        # per-request routing advisory — s3_href + the s3_endpoint/s3_scope to
        # pass to `query` (s3config.route_hint, #264). Additive only, so inline
        # round-trips and non-DuckDB consumers are unaffected.
        if a["href"].startswith("http") and _is_queryable_asset(asset.href, asset.media_type or ""):
            hint = s3config.route_hint(a["href"])
            if hint:
                a["s3_href"] = hint["path"]
                a["s3_endpoint"] = hint["endpoint"]
                a["s3_scope"] = hint["scope"]
        # Merge extra_fields (table:columns, raster:bands, vector:layers, file:size, …)
        a.update(asset.extra_fields)
        assets[asset_id] = {k: v for k, v in a.items() if v is not None}
    result["assets"] = assets

    # Collection-level STAC extension fields
    for ext_key in ("table:columns", "vector:layers", "raster:bands"):
        val = col.extra_fields.get(ext_key)
        if val is not None:
            result[ext_key] = val

    # Child collections — only populated when sub_children are provided. Emitted
    # as a list of nested dicts (not just IDs) so the output round-trips back
    # into the inline `collection=` parameter; see issue #105.
    if sub_children is not None:
        result["children"] = [_collection_to_dict(sc) for sc in sub_children]

    return {k: v for k, v in result.items() if v is not None}


# Safety net for the recursive walk (#433): far deeper than any catalog we
# publish. The visited set already stops cycles; this stops a runaway chain.
_STAC_MAX_DEPTH = int(os.environ.get("STAC_MAX_DEPTH", "6"))


def _is_absolute_href(href: str) -> bool:
    return "://" in href or href.startswith("/")


def _resolve_href(obj, href: str) -> str:
    """*href* made absolute against *obj*'s own URL, with no network access.

    pystac's Link.href and get_absolute_href can fetch the root catalog to decide
    how to render a path, and a mirror's documents keep `root` pointing at the
    original catalog (data.source.coop/cboettig/glen points at NRP), so every read
    would also depend on that host. A plain urljoin against the self href avoids it.
    """
    if not href or _is_absolute_href(href):
        return href
    base = obj.get_self_href()
    return urljoin(base, href) if base else href


def _read_stac_object(href: str, stac_io):
    """Fetch one STAC document and parse it as a Catalog or a Collection (#433).

    Dispatches on the document's "type": a Catalog is a group node, which STAC
    allows anywhere above a Collection and Portolan requires for grouping. The
    object's self href is set to *href*, the URL it was fetched from, so relative
    link and asset hrefs resolve against it; Portolan catalogs carry relative
    hrefs and no self link. Relative asset hrefs are made absolute here, so every
    renderer sees a full URL. Absolute hrefs are kept as written (globs survive).
    """
    d = stac_io.read_json(href)
    kind = d.get("type")
    if kind == "Collection":
        obj = pystac.Collection.from_dict(d, href=href)
    elif kind == "Catalog":
        obj = pystac.Catalog.from_dict(d, href=href)
    else:
        raise pystac.STACTypeError(f"{href} is a {kind!r}, not a Catalog or Collection")
    obj.set_self_href(href)
    for asset in (getattr(obj, "assets", None) or {}).values():
        asset.href = _resolve_href(obj, asset.href)
    return obj


def _child_links(obj) -> list[tuple[str, str | None]]:
    """(absolute href, title) of each child link, in document order."""
    out = []
    for link in obj.links or []:
        if link.rel != "child":
            continue
        out.append((_resolve_href(obj, link.get_target_str()), getattr(link, "title", None)))
    return out


def _fetch_node(href: str, title: str | None, token: str | None, timeout: int = None):
    """Thread-worker: fetch one catalog node, a Catalog or a Collection.

    Returns (obj, child_links, error): obj is None on failure, child_links lists
    (href, title) of its children, and error is None or {identifier: reason}.
    Never raises. `timeout` overrides the default child timeout (retry pass).
    """
    child_io = _TimeoutStacIO(
        token=token,
        timeout=timeout if timeout is not None else _STAC_CHILD_TIMEOUT,
    )
    try:
        obj = _read_stac_object(href, child_io)
        return obj, _child_links(obj), None
    except Exception as e:
        ident = _child_identifier(href, title_hint=title)
        return None, [], {ident: f"{type(e).__name__}: {e}"}


def _is_group(obj, child_count: int) -> bool:
    """A group node holds no data: a Catalog, or a Collection with children and
    no queryable asset. Groups are headings in the listing, never datasets."""
    if isinstance(obj, pystac.Catalog) and not isinstance(obj, pystac.Collection):
        return True
    if not child_count:
        return False
    return not any(
        _is_queryable_asset(a.href or "", a.media_type or "")
        for a in (getattr(obj, "assets", None) or {}).values()
    )


def _assemble(root_key: str, nodes: dict, kids_of: dict):
    """Render a fetched tree into (datasets, raw, index).

    *nodes* maps a node key (its href) to its pystac object, and *kids_of* maps a
    key to its children's keys in link order. Every node is indexed once, under
    the first parent that reaches it, so a node linked from two parents is not
    listed twice. *index* is [(depth, id, is_group)] in depth-first order, which
    list_datasets renders as headings with their datasets indented beneath.
    A group's markdown lists every dataset beneath it, however deep; its dict
    nests its direct children, recursively.
    """
    def kids(key):
        return [k for k in kids_of.get(key, []) if k in nodes]

    def leaves_under(key, out, seen):
        for k in kids(key):
            if k in seen:
                continue
            seen.add(k)
            if not _is_group(nodes[k], len(kids(k))):
                out.append(nodes[k])
            leaves_under(k, out, seen)
        return out

    def tree_dict(key, stack):
        d = _collection_to_dict(nodes[key], sub_children=None)
        sub = [k for k in kids(key) if k not in stack]
        if sub:
            d["children"] = [tree_dict(k, stack | {k}) for k in sub]
        return d

    datasets: dict = {}
    raw: dict = {}
    index: list = []
    placed = {root_key}

    def visit(key, depth):
        for k in kids(key):
            if k in placed:
                continue
            placed.add(k)
            obj = nodes[k]
            if obj.id in datasets:
                print(f"⚠️ Duplicate STAC id {obj.id!r} at {k}; keeping the first", file=sys.stderr)
                # Skip the node, not its subtree: a duplicate group's datasets
                # still list, at the depth the group would have had.
                visit(k, depth)
                continue
            sub = kids(k)
            if sub:
                datasets[obj.id] = _format_collection(obj, sub_children=leaves_under(k, [], {k}))
                raw[obj.id] = tree_dict(k, {k})
            else:
                datasets[obj.id] = _format_collection(obj, sub_children=[])
                raw[obj.id] = _collection_to_dict(obj, sub_children=None)
            index.append((depth, obj.id, _is_group(obj, len(sub))))
            visit(k, depth + 1)

    visit(root_key, 0)
    return datasets, raw, index


def _walk_catalog(url: str, token: str | None):
    """Fetch a STAC tree to any depth (#433) and render it.

    Returns (datasets, raw, index, errors), or None plus the root's error string
    when the root itself cannot be read. Children are fetched in a bounded
    ThreadPoolExecutor with the tight child timeout; individual failures are
    isolated in *errors*, and failed nodes are retried once with a longer timeout
    (a rescued node's children are then fetched too). Each href is fetched once.
    """
    root_io = _TimeoutStacIO(token=token, timeout=_STAC_ROOT_TIMEOUT)
    try:
        root = _read_stac_object(url, root_io)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

    root_kids = _child_links(root)
    nodes: dict = {}
    kids_of: dict = {url: [h for h, _ in root_kids]}
    visited = {url} | {h for h, _ in root_kids}
    errors: dict = {}

    def drain(pool, seeds, timeout, failed, retrying=False):
        pending: dict = {}

        def submit(href, title, depth):
            fut = pool.submit(_fetch_node, href, title, token, timeout)
            pending[fut] = (href, title, depth)

        for seed in seeds:
            submit(*seed)
        while pending:
            done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                href, title, depth = pending.pop(fut)
                obj, kids, error = fut.result()
                if obj is None:
                    errors.update(error)
                    failed.append((href, title, depth))
                    continue
                if retrying:
                    errors.pop(_child_identifier(href, title_hint=title), None)
                    print(f"🔁 Retry succeeded: {obj.id}", file=sys.stderr)
                nodes[href] = obj
                kids_of[href] = [h for h, _ in kids]
                for kid_href, kid_title in kids:
                    if kid_href in visited:
                        continue
                    visited.add(kid_href)
                    if depth + 1 > _STAC_MAX_DEPTH:
                        errors[_child_identifier(kid_href, title_hint=kid_title)] = (
                            f"DepthLimit: more than {_STAC_MAX_DEPTH} levels below the root"
                        )
                        continue
                    submit(kid_href, kid_title, depth + 1)

    failed: list = []
    with ThreadPoolExecutor(max_workers=_STAC_FETCH_CONCURRENCY) as pool:
        drain(pool, [(h, t, 1) for h, t in root_kids], None, failed)
    if failed:
        # Rescues tail-latency failures; a retry that also fails keeps its error.
        with ThreadPoolExecutor(max_workers=_STAC_FETCH_CONCURRENCY) as pool:
            drain(pool, failed, _STAC_CHILD_RETRY_TIMEOUT, [], retrying=True)

    datasets, raw, index = _assemble(url, nodes, kids_of)
    return datasets, raw, index, errors


# Parallel cache of structured dicts, populated alongside STAC_DATASETS at startup.
_STAC_RAW: dict[str, dict] = {}

# Listing order and nesting of the default catalog: [(depth, id, is_group)].
_STAC_INDEX: list = []

# Populated by fetch_stac_catalog on the default-catalog path; keys are the best
# available identifier (real collection id when parse succeeded, else href tail),
# values are short reason strings. Cleared on each successful default-catalog load.
STAC_LOAD_ERRORS: dict[str, str] = {}


def fetch_stac_catalog(catalog_url: str = None, catalog_token: str = None) -> dict[str, str]:
    """Fetch the STAC catalog and return {collection_id: markdown_summary}.

    Walks the tree to any depth through Catalog and Collection nodes (#433); see
    _walk_catalog. A root failure returns {} and records a __root__ error. For the
    default catalog (no catalog_url), module-level state (STAC_DATASETS, _STAC_RAW,
    _STAC_INDEX, STAC_LOAD_ERRORS) is replaced once the walk finishes.
    """
    url = catalog_url or STAC_CATALOG_URL
    # The deployment's own catalog carries its credential in the environment
    # (#420). An explicit catalog_url is a library call and keeps whatever token
    # it was handed.
    if not catalog_url:
        catalog_token = catalog_token or default_catalog_token()

    walked = _walk_catalog(url, catalog_token)
    if walked[0] is None:
        reason = walked[1]
        print(f"⚠️ Failed to load STAC root catalog: {reason}", file=sys.stderr)
        if not catalog_url:
            STAC_LOAD_ERRORS.clear()
            STAC_LOAD_ERRORS["__root__"] = reason
        return {}
    datasets, raw, index, errors = walked

    print(
        f"📂 Loaded {len(datasets)} collections "
        f"({len(errors)} failed) from STAC: {url}",
        file=sys.stderr,
    )
    for ident, reason in errors.items():
        print(f"⚠️ Child fetch failed: {ident} — {reason}", file=sys.stderr)

    if not catalog_url:
        # Only replace successful-load state when the new load produced something:
        # if every child failed, keep the previous snapshot rather than wiping a
        # working cache. Errors are always refreshed so operators and the
        # list_datasets footer see the current failure state. Held under
        # _STAC_LOCK so a concurrent reader never sees a half-swapped cache (#337).
        with _STAC_LOCK:
            if datasets:
                STAC_DATASETS.clear()
                STAC_DATASETS.update(datasets)
                _STAC_RAW.clear()
                _STAC_RAW.update(raw)
                _STAC_INDEX[:] = index
            STAC_LOAD_ERRORS.clear()
            STAC_LOAD_ERRORS.update(errors)

    return datasets


# Module-level caches — declared before the startup load so the loader's
# clear()/update() pattern works on first call.
STAC_DATASETS: dict[str, str] = {}

# Kick off the initial load at import. Populates STAC_DATASETS, _STAC_RAW,
# STAC_LOAD_ERRORS in place.
fetch_stac_catalog()

# Set once start_periodic_refresh() spawns the daemon, so repeat calls are no-ops.
_refresh_thread: threading.Thread | None = None


def _refresh_loop(interval: int, stop_event: threading.Event) -> None:
    """Re-walk the default catalog every *interval* seconds until *stop_event*.

    A publish to S3 (new dataset OR a new asset on an existing collection) is
    invisible to get_stac_details/get_collection/list_datasets until the in-process
    snapshot is rebuilt — the on-miss re-fetch (#11) can't help the updated-asset
    case because it's a cache HIT. This per-pod timer rebuilds the snapshot so every
    replica converges without an external trigger or a rollout (#337).

    Failures are swallowed: fetch_stac_catalog already keeps the previous snapshot
    when a walk yields nothing, so a transient S3 blip just means a stale-but-working
    cache until the next tick — never a crashed thread or a wiped catalog.
    """
    while not stop_event.wait(interval):
        try:
            fetch_stac_catalog()
        except Exception as e:  # never let the daemon die on a transient error
            print(
                f"⚠️ STAC periodic refresh failed (keeping previous snapshot): "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )


def start_periodic_refresh(interval: int = None) -> threading.Thread | None:
    """Spawn the background refresh daemon (idempotent). Returns the thread, or
    None when disabled (interval <= 0).

    Started explicitly from server.py's __main__ rather than at import, so tests
    and one-shot tooling that import `stac` don't spawn a background thread.
    """
    global _refresh_thread
    if interval is None:
        interval = _STAC_REFRESH_INTERVAL
    if interval <= 0:
        print(
            "⏸️ STAC periodic refresh disabled (STAC_REFRESH_INTERVAL <= 0)",
            file=sys.stderr,
        )
        return None
    if _refresh_thread is not None and _refresh_thread.is_alive():
        return _refresh_thread
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_refresh_loop,
        args=(interval, stop_event),
        name="stac-refresh",
        daemon=True,
    )
    thread._stop_event = stop_event  # handle for tests / future shutdown
    thread.start()
    _refresh_thread = thread
    print(
        f"🔄 STAC periodic refresh started (every {interval}s)",
        file=sys.stderr,
    )
    return thread


def _coerce_inline_node(d: dict):
    """Parse an inline catalog node: a Catalog when its type says so, else a Collection."""
    if d.get("type") == "Catalog":
        payload = {k: v for k, v in d.items() if k != "children"}
        payload.setdefault("stac_version", "1.0.0")
        payload.setdefault("links", [])
        payload.setdefault("description", "")
        try:
            return pystac.Catalog.from_dict(payload)
        except Exception as e:
            raise ValueError(f"Invalid inline catalog: {type(e).__name__}: {e}") from e
    return _coerce_inline_collection(d)


def _render_inline_catalog(catalog: dict):
    """Build (datasets, index) from an inline catalog dict.

    Mirrors fetch_stac_catalog: nested `children` lists may go to any depth and
    hold Catalog or Collection nodes (#433), rendered by the same _assemble.
    """
    nodes: dict = {}
    kids_of: dict = {"": []}

    def add(d, parent_key):
        obj = _coerce_inline_node(d)
        key = f"{parent_key}/{obj.id}"
        kids_of[parent_key].append(key)
        nodes[key] = obj
        kids_of[key] = []
        for child in d.get("children") or []:
            add(child, key)

    for child in catalog.get("children", []) or []:
        add(child, "")
    datasets, _raw, index = _assemble("", nodes, kids_of)
    return datasets, index


def _render_listing(datasets: dict, index: list) -> tuple[list[str], int, int]:
    """Markdown lines for browse_stac_catalog, plus the dataset and group counts.

    Groups (#433) are headings with their datasets indented beneath; only datasets
    are things to call get_stac_details on. Ids missing from *index* are listed
    flat at the end, so a cache populated without one still lists everything.
    """
    lines: list[str] = []
    listed: set = set()
    groups = 0
    for depth, cid, is_group in index:
        if cid not in datasets or cid in listed:
            continue
        listed.add(cid)
        pad = "  " * depth
        first_line = datasets[cid].split("\n")[0]
        if is_group:
            groups += 1
            lines.append(f"{pad}- {first_line} (group `{cid}`):")
        else:
            lines.append(f"{pad}- **{cid}**: {first_line}")
    for cid, summary in datasets.items():
        if cid not in listed:
            lines.append(f"- **{cid}**: {summary.split(chr(10))[0]}")
    return lines, len(datasets) - groups, groups


def _listing_header(n_datasets: int, n_groups: int) -> str:
    groups = f" in {n_groups} group{'s' if n_groups != 1 else ''}" if n_groups else ""
    return f"# Available Datasets ({n_datasets} dataset{'s' if n_datasets != 1 else ''}{groups})\n"


def _lookup(mapping: dict, key: str):
    """Find *key* in *mapping*: exact id, then a unique last path segment, then
    _fuzzy_lookup's prefix and substring matches.

    Returns (value, candidates). Portolan ids are paths ("government/wards"), so
    "wards" finds that id, but only when exactly one id ends in it: when several
    do, value is None and *candidates* lists their full ids, rather than picking one.
    """
    if key in mapping:
        return mapping[key], []
    low = key.lower()
    if "/" not in key:
        tails = [k for k in mapping if "/" in k and k.rsplit("/", 1)[1].lower() == low]
        if len(tails) == 1:
            return mapping[tails[0]], []
        if len(tails) > 1:
            return None, tails
    return _fuzzy_lookup(mapping, key), []


def _ambiguous(noun: str, key: str, candidates: list, catalog_url: str = None) -> str:
    ids = ", ".join(f"`{c}`" for c in candidates)
    return (
        f"{noun} '{key}' not found in {_searched_catalog(catalog_url)}: it matches "
        f"more than one. Use one of the full ids: {ids}."
    )


# ---------------------------------------------------------------------------
# Library entry points.
#
# The `catalog_url` / `catalog_token` parameters below are a *library* affordance
# — for tests and for in-process callers that already know which catalog they
# mean. They are deliberately NOT exposed in the MCP tool schemas: catalog scope
# is ambient deployment config (`STAC_CATALOG_URL` + `STAC_CATALOG_TOKEN`), never
# an argument the model supplies per call (#420). Do not plumb them back out.
# ---------------------------------------------------------------------------
def list_datasets(
    catalog_url: str = None,
    catalog_token: str = None,
    catalog: dict = None,
) -> str:
    """List all available datasets from the STAC catalog.

    Appends a warning footer when `STAC_LOAD_ERRORS` is non-empty, so callers
    can distinguish "not in catalog" from "failed to load this time."

    Resolution order: `catalog` (inline, no fetch) → `catalog_url` (fetch) →
    server default. See issue #105.
    """
    if catalog is not None:
        datasets, index = _render_inline_catalog(catalog)
        url = catalog.get("id") or "<inline>"
        body, n_datasets, n_groups = _render_listing(datasets, index)
        lines = [_listing_header(n_datasets, n_groups)]
        lines.append(f"STAC catalog: `{url}` (provided inline)\n")
        lines.extend(body)
        return "\n".join(lines)

    # Set only when the server reads the catalog somewhere a client can't, so the
    # advertised URL needs disambiguating (#346).
    internal_note = ""
    if catalog_url:
        walked = _walk_catalog(catalog_url, catalog_token)
        datasets, index = (walked[0], walked[2]) if walked[0] is not None else ({}, [])
        url = catalog_url
        # Errors for custom catalogs are not tracked in module state; caller
        # can detect failure via returned dict being empty or partial.
        footer_errors: dict = {}
    else:
        # Snapshot the shared caches under the lock so a concurrent background
        # refresh (#337) can't mutate them mid-iteration below.
        with _STAC_LOCK:
            datasets = dict(STAC_DATASETS)
            index = list(_STAC_INDEX)
            footer_errors = dict(STAC_LOAD_ERRORS)
        # Report the client-usable URL, not the one this process reads.
        url = public_catalog_url()
        if url != STAC_CATALOG_URL:
            internal_note = (
                f"(server reads it at {STAC_CATALOG_URL} — "
                "use the first URL in client configs)"
            )
    if not datasets and not footer_errors:
        msg = f"No datasets loaded. STAC catalog: {url}"
        return f"{msg}\n{internal_note}" if internal_note else msg
    body, n_datasets, n_groups = _render_listing(datasets, index)
    lines = [_listing_header(n_datasets, n_groups)]
    lines.append(f"STAC catalog: `{url}`\n")
    if internal_note:
        lines.append(f"{internal_note}\n")
    lines.extend(body)
    if footer_errors:
        lines.append("")
        err_pairs = ", ".join(f"{k} ({v.split(':', 1)[0]})" for k, v in footer_errors.items())
        lines.append(
            f"⚠️ {len(footer_errors)} collection"
            f"{'s' if len(footer_errors) != 1 else ''} could not be loaded: {err_pairs}"
        )
    return "\n".join(lines)


def get_dataset(
    dataset_id: str,
    catalog_url: str = None,
    catalog_token: str = None,
    collection: dict = None,
) -> str:
    """Get detailed metadata for a specific dataset.

    Resolution order: `collection` (inline, no fetch) → `catalog_url` (fetch) →
    server default. Embedded `children: [<dict>, ...]` in the inline dict are
    rendered as sub-datasets. See issue #105.
    """
    if collection is not None:
        col = _coerce_inline_collection(collection)
        child_dicts = collection.get("children") or []
        sub_children = [_coerce_inline_collection(c) for c in child_dicts]
        return _format_collection(col, sub_children=sub_children)

    if catalog_url:
        # Local (unshared) dict — no lock needed.
        datasets = fetch_stac_catalog(catalog_url, catalog_token=catalog_token)
        result, candidates = _lookup(datasets, dataset_id)
    else:
        # Shared cache — read under the lock (guards against a concurrent refresh #337).
        with _STAC_LOCK:
            result, candidates = _lookup(STAC_DATASETS, dataset_id)
    if result is not None:
        return result
    if candidates:
        return _ambiguous("Dataset", dataset_id, candidates, catalog_url)

    # Cache miss (default catalog only): re-fetch in case datasets were added since startup
    if not catalog_url:
        fetch_stac_catalog()  # populates STAC_DATASETS in place if successful
        with _STAC_LOCK:
            result, candidates = _lookup(STAC_DATASETS, dataset_id)
        if result is not None:
            return result
        if candidates:
            return _ambiguous("Dataset", dataset_id, candidates)
    return (
        f"Dataset '{dataset_id}' not found in {_searched_catalog(catalog_url)}."
        f"{_discovery_hint('datasets')}"
    )


def _coerce_inline_collection(d: dict):
    """Parse an inline STAC collection dict into a pystac.Collection.

    Tolerates dicts that omit STAC envelope fields (`type`, `stac_version`,
    `links`, `description`) so that both `_collection_to_dict` output AND
    hand-built/client-cached dicts (#105) can pass through the inline
    `collection=` parameter. The `children` key is consumed by the caller and
    must be stripped before pystac sees the dict.
    """
    payload = {k: v for k, v in d.items() if k != "children"}
    payload.setdefault("type", "Collection")
    payload.setdefault("stac_version", "1.0.0")
    # STAC marks `license` as required, but `_collection_to_dict` strips None
    # values — so a license-less collection round-tripped through this path
    # would crash pystac's parser. "various" is the STAC-recommended placeholder.
    payload.setdefault("license", "various")
    # pystac raises a bare KeyError for these two (unlike `extent`, which it
    # defaults with a warning) — a hand-built dict shouldn't need them (#278).
    payload.setdefault("links", [])
    payload.setdefault("description", "")
    try:
        return pystac.Collection.from_dict(payload)
    except Exception as e:
        # Surface remaining parse failures (e.g. a missing `id`) as a readable
        # message instead of pystac's bare KeyError reaching the tool response.
        raise ValueError(f"Invalid inline collection: {type(e).__name__}: {e}") from e


def get_collection(
    collection_id: str,
    catalog_url: str = None,
    catalog_token: str = None,
    collection: dict = None,
) -> dict:
    """Return structured STAC collection metadata for programmatic use.

    Unlike get_stac_details (which returns markdown for LLM consumption),
    this returns the raw collection dict with:
    - All assets (parquet, PMTiles, COG, GeoJSON) with hrefs converted to s3://
    - Per-asset STAC extension fields (table:columns, raster:bands, vector:layers)
    - Full collection metadata (providers, extent, license, keywords, links)
    - Child collections (nested dicts) for parent containers

    Resolution order:
      1. If `collection` is provided, render directly (no fetch). Embedded
         `children: [<dict>, ...]` are surfaced as nested child dicts.
      2. Else if `catalog_url` is provided, walk that catalog as today.
      3. Else use the server's default STAC_CATALOG_URL.

    Intended for app code (e.g. geo-agent) that builds map layers and system
    prompts programmatically from structured data.
    """
    if collection is not None:
        col = _coerce_inline_collection(collection)
        child_dicts = collection.get("children") or []
        sub_children = [_coerce_inline_collection(c) for c in child_dicts] or None
        return _collection_to_dict(col, sub_children=sub_children)

    if catalog_url:
        # Non-default catalog (library call): walk it the same way as the default.
        try:
            walked = _walk_catalog(catalog_url, catalog_token)
        except Exception as e:
            return {"error": f"Failed to fetch catalog: {e}"}
        if walked[0] is None:
            return {"error": f"Failed to fetch catalog: {walked[1]}"}
        result, candidates = _lookup(walked[1], collection_id)
        if result is not None:
            return result
        if candidates:
            return {"error": _ambiguous("Collection", collection_id, candidates, catalog_url)}
        return {"error": f"Collection '{collection_id}' not found in {_searched_catalog(catalog_url)}."}

    # Default catalog: look up from pre-populated cache (read under the lock so a
    # concurrent background refresh #337 can't mutate _STAC_RAW mid-lookup).
    with _STAC_LOCK:
        result, candidates = _lookup(_STAC_RAW, collection_id)
    if result is not None:
        return result
    if candidates:
        return {"error": _ambiguous("Collection", collection_id, candidates)}

    # Cache miss: re-fetch.
    # fetch_stac_catalog() updates _STAC_RAW when catalog_url is None.
    fetch_stac_catalog()
    with _STAC_LOCK:
        result, candidates = _lookup(_STAC_RAW, collection_id)
    if result is not None:
        return result
    if candidates:
        return {"error": _ambiguous("Collection", collection_id, candidates)}

    return {"error": (
        f"Collection '{collection_id}' not found in {_searched_catalog()}."
        f"{_discovery_hint('collections')}"
    )}
