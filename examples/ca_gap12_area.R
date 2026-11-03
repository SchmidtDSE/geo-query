#!/usr/bin/env Rscript
# Total GAP 1 + GAP 2 conserved area in California (the share California's
# 30x30 program counts as conserved), computed from the H3 grid.
#
# Runs entirely on YOUR machine: a local DuckDB streams Parquet from NRP's
# public S3. No MCP server, no API key, no data downloaded to disk.

library(duckdbfs)
library(dplyr)
library(units)

# Anonymous reads from NRP's public S3. (The 2025 layer is not yet on the
# source.coop mirror.)
duckdb_s3_config(
  s3_endpoint  = "s3-west.nrp-nautilus.io",
  s3_url_style = "path",
  s3_use_ssl   = TRUE,
  anonymous    = TRUE
)

# One row per res-10 H3 cell, with the fraction of that cell in each GAP status
# (w1..w4). A conserved-area unit is split across statuses, so its cells are
# fractions too -- never round a cell to conserved / not conserved.
weights <- open_dataset("s3://public-ca30x30/conserved-areas-terrestrial-2025/hex-weights")

# H3 cells are not all the same size, so weight each cell's GAP 1+2 fraction by
# its true area. h3_cell_area() runs inside DuckDB.
area <- weights |>
  summarise(km2 = sum((w1 + w2) * h3_cell_area(h10, "km^2"), na.rm = TRUE)) |>
  pull(km2)

set_units(area, km^2) |> set_units(acres)
