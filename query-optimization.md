# Query Optimization Essentials

## 1. Always include h0 in joins
<!-- prov: issue=#40 models=unrecorded added=2026-03-31 cell=hex-join-include-h0 tier=core -->

Most datasets are hive-partitioned by h0. When both sides of a join have h0, always include it in the join condition:

```sql
JOIN table2 ON table1.hX = table2.hX AND table1.h0 = table2.h0
```

where `hX` is the finest resolution shared by both datasets (h8, h9, etc. — check the
schema). Omitting `AND t1.h0 = t2.h0` causes DuckDB to open every partition file on S3
instead of only the matching ones (10-100x slower).

Except after `h3_grid_disk`: ring cells can fall in another partition, so take each
ring cell's h0 from `h3_cell_to_parent(cell, 0)` (H3 guide, *Distance Between Hexes*).

## 2. Start with a small geographic reference dataset
<!-- prov: issue=#83,#87 models=unrecorded added=2026-08-12 cell=mask-before-aggregate tier=core -->

Use `regions/hex/**` or `countries.parquet` as the first CTE to establish geographic
scope before joining large thematic datasets (PADUS, carbon, wetlands, species).

```sql
WITH scope AS (
  SELECT DISTINCT h8, h0
  FROM read_parquet('<STAC_REGIONS_HEX_PATH>')
  WHERE region = 'US-CA'
),
parks AS (
  SELECT DISTINCT p.h8, p.h0
  FROM scope s
  JOIN read_parquet('<STAC_PADUS_HEX_PATH>') p
    ON s.h8 = p.h8 AND s.h0 = p.h0
  WHERE p.Des_Tp = 'NP'
)
SELECT SUM(c.carbon)/1e6
FROM parks p
JOIN read_parquet('<STAC_CARBON_HEX_PATH>') c
  ON p.h8 = c.h8 AND p.h0 = c.h0
```

### Scoping by name or feature id (no region mask)
<!-- prov: issue=#163,#563 models=gpt-oss,gemma added=2026-11-28 cell=bosl-pri-eez-area,glob-brazil-first-protected tier=core -->

To filter a global `…/hex/h0=*/…` dataset by a name or `_cng_fid` with no region-mask hex to join, filter on the dataset's own attribute. Never derive an `h0` set from typed coordinates or a hand-written list; when you want an `h0` prefilter, take it from the data with the same attribute filter.

```sql
SELECT SUM(h3_cell_area(h8, 'km^2')) AS area_km2
FROM (
  SELECT DISTINCT h8, h0
  FROM read_parquet('s3://<bucket>/<dataset>/hex/h0=*/data_00.parquet')
  WHERE _cng_fid IN (201, 246, 243, 142, 90)
);
```

## 3. Trust your schema — don't grep with DESCRIBE+LIKE
<!-- prov: issue=#108,#113 models=qwen3 added=2026-05-03 cell=none tier=extra -->

For datasets already loaded in your app, the column list returned by `get_schema`
(or `get_stac_details`) is canonical: every column, with type and description.
Running follow-up queries like

```sql
SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('…') LIMIT 1)
WHERE column_name LIKE '%corridor%'
```

to grep for specific columns is wasteful — the same data is already in your
context. Search the existing schema response instead. `DESCRIBE` is appropriate
only for arbitrary parquet files not represented in any STAC collection.

## 4. Text matching: fuzzy search vs exact keys
<!-- prov: issue=#209,#220,#528 models=unrecorded added=2026-06-20 cell=feature_type-exact,county-name-filter,program-like-lwcf tier=core -->

DuckDB `LIKE` is case-sensitive by default. For **fuzzy substring search** on a
free-text label the user typed (site, owner, program names), normalize both
sides:

```sql
WHERE lower(site) LIKE '%' || lower('user input') || '%'
```

For an **exact match on a known key** (`sci_name`, `name_en`, a country or
category code), match the stored case exactly instead of wrapping the column in
`lower()`. Where the file is sorted or clustered by that key, exact-case lets
DuckDB skip row groups by their stored min/max stats; `lower()` forces a full
column scan. Take the spelling from an example in the STAC metadata or a
`SELECT DISTINCT` probe:

```sql
WHERE sci_name = 'Rana draytonii'
```

## 5. GeoParquet geometry columns
<!-- prov: issue=#48,#522 models=unrecorded added=2026-11-20 cell=none tier=core -->

A GeoParquet file has one row per feature and a geometry column (`geom`, `geometry`
or `SHAPE`) that the server drops from output. Read feature attributes from it (names,
ids, measured area or length); do spatial work on the hex asset. Select only the
columns you need, not `SELECT *`. For coordinates use `h3_cell_to_lat` /
`h3_cell_to_lng` on the hex (H3 guide).

## 6. Apostrophes in string literals
<!-- prov: issue=unrecorded models=unrecorded added=2026-06-24 cell=tpl-tates-hell-acres tier=core -->

Site names and owner names can contain apostrophes (e.g. `O'Brien Ranch`). Double any
single quote inside a SQL string literal — do not use a backslash:

```sql
WHERE site = 'O''Brien Ranch'   -- correct
WHERE site = 'O'Brien Ranch'    -- parse error
```

## 7. Filter no-data values before SUM / AVG
<!-- prov: issue=#243,#244,#549 models=gemma,glm-5,kimi added=2026-08-12 cell=sum-amount-safe tier=core -->

A single `NaN` turns the entire aggregate into `NaN` — one no-data cell poisons a
whole `SUM` or `AVG`. No-data shows up as literal `NaN`, or as sentinel codes a
dataset reserves for "no data" (e.g. a land-cover fill class 0). Exclude both
before aggregating:

```sql
SELECT SUM(<value>) AS total
FROM read_parquet('<hex>')
WHERE <value> IS NOT NULL AND NOT isnan(<value>)
  AND lc_class <> 0;   -- the dataset's fill value
```

Take the sentinel codes from the dataset's STAC description, and keep classes it
names as real (water, open sea) unless the question excludes them. A `NaN` total or a
total far smaller than expected is the signature of this trap.

## 8. A single feature's own size comes from its measured column
<!-- prov: issue=#289,#563 models=claude-sonnet-5,glm-5.2,kimi-k3,qwen,qwen3-small,deepseek-v4-flash added=2026-11-28 cell=acres-from-geoparquet-not-hex tier=core -->

When the question asks for the size of named features themselves, take each
feature's area or length from its **own** column (`acres`, `length_km`), summed over
`DISTINCT _cng_fid` — tiling repeats that value on every cell. The area covered by
several features or by a layer is the **distinct-cell union** (`h3_cell_area` over
distinct cells, or `cover_frac` per H3 guide Problem 2), never a sum of an attribute
area column, which double-counts overlaps. Take a ratio's numerator and denominator
from the same cells: `SEMI JOIN` the numerator to the denominator's cells. For the conserved or overlaid **share**,
weight by the coverage fraction — area for polygons and rasters, length for lines —
inside the same `SUM()` as the measure. (See the overlay rules in the H3 guide.)

## 9. When a no-data filter drops a material share of the data, say so
<!-- prov: issue=#359,#563 models=deepseek-v4-flash added=2026-11-28 cell=instance-only:streamorder-use-nhdplus-hr-not-base-nhd tier=core -->

*(Skip unless excluding NULLs or sentinels removes a material share of the rows or
of the measure.)* The surviving rows are then rarely a random sample: incomplete
columns track geography, feature type or source vintage. State the covered fraction
alongside the number. If coverage is concentrated in part of the study area, say the
breakdown is unavailable for the whole rather than giving a number with a caveat.

## 10. One-line rules
<!-- prov: issue=#563 models=qwen3.6-35b-a3b,inkling-2-mini,deepseek-nimbus,glm-5.3-flash,mimo-v2.6-flash,gpt-oss,gemma-small,gemma,qwen3-small added=2026-11-28 cell=orig-vents-near-ebsa,car-20-native-bird-hotspots,car-21-native-reptile-hotspots,glob-brazil-first-protected tier=core -->

- Select by the dataset's own attribute and exclude the few codes that don't belong; never type long code lists or `h0` sets.
- `h3_cell_to_parent(cell, res)` takes the target resolution (`h3_cell_to_parent(h10, 8)`), not a number of steps.
- A cell has `7^(fine − coarse)` children at a finer resolution (49 from res 8 to res 10).
- Between OGC:CRS84 and EPSG:4326 use `ST_SetCRS`, never `ST_Transform`: the coordinates are the same, and `ST_Transform` swaps the axes.
- Top X% by value: cut at `quantile_cont(value, 1 - X/100)` over distinct cells.
- Never cross-join two layers to compute distances; use `h3_grid_disk` (H3 guide, *Distance Between Hexes*).
