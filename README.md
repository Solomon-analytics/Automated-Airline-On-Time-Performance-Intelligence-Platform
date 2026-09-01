# Automated-Airline-On-Time-Performance-Intelligence-Platform
Airlines, airports and travel platforms need current, trustworthy visibility into flight reliability to make route, compensation and capacity decisions. Source data arrives monthly, raw and inconsistent (missing delay-cause codes, cancelled vs diverted flights, shifting airport/carrier reference data). Manual reprocessing doesn't scale, and silent failures erode trust in the numbers.

# Aeropulse: ADLS → Landing → Bronze Ingestion

Part of the **Aeropulse** project — a Microsoft Fabric rebuild of DP-700 Domain 1 fundamentals using US Bureau of Transportation Statistics (BTS) flight data, replacing an earlier Airbnb-based build once it became clear that dataset had no bookings/transaction records to build a real fact table on.

## Workspace & lakehouse layout

Environment: `aeropulse_dev` workspace (Dev stage), with separate Test/Prod workspaces to follow via Fabric deployment pipelines.

One lakehouse per medallion layer, rather than a single lakehouse split into schemas — this keeps each layer independently permissioned and independently promotable through deployment pipelines, so gold can eventually be shared with report consumers without exposing raw or unvalidated data underneath it.

| Lakehouse | Purpose |
|---|---|
| `aeropulse_landing_lh` | Raw file copies, tagged with batch and lineage metadata |
| `aeropulse_bronze_lh` | First structured Delta layer, schema-enabled |
| `aeropulse_silver_lh` | Cleaned, deduplicated, DQ-validated (in progress) |
| `aeropulse_gold_lh` / Warehouse | Business/dimensional model for reporting (in progress) |

## Objective 1 — Data ingestion: ADLS to landing lakehouse

A dedicated lakehouse (`aeropulse_landing_lh`) holds the landing layer. Ingestion is parameterised by batch, so a single notebook design handles any month's data without code changes.

A shared `landing-environment` notebook centralises configuration: the ADLS account and container, and the source paths for all three datasets (airports, carriers, flights). The flights path is built dynamically from the batch year and batch ID rather than hardcoded, so it always resolves to the correct month's file for whichever batch is currently running.

A shared `write_to_landing()` helper handles the actual write, reused across all three sources. Every row is tagged with the batch it belongs to, an ingestion timestamp, and the file it came from, giving each record traceable lineage back to its source file. The helper branches by load type: the incremental source (flights) lands into a folder named after its batch, so re-running a given month only ever touches that batch's own folder and never disturbs previously landed data; the static reference sources (airports, carriers) are fully refreshed into a fixed folder on every run. Data is written in its original CSV format — landing performs no transformation, only a raw, auditable copy from ADLS into the lakehouse.

Each ingestion notebook follows the same shape: set the batch parameters, pull in the shared environment and helper notebooks, read the relevant ADLS source, and write it to landing. Airport and carrier ingestion follow the identical pattern, differing only in source path and load type.

## Objective 2 (phase 1) — Landing to bronze

A second, separate lakehouse (`aeropulse_bronze_lh`) holds the bronze layer, continuing the one-lakehouse-per-layer structure and created with schema support enabled.

A matching `bronze-environment` notebook holds the landing lakehouse's path and the per-source folder locations to read back from. A shared `write_to_bronze()` helper writes managed Delta tables, following the same incremental/full split established at landing: incremental batches use a targeted replace-and-partition write so re-running a batch replaces only that batch's rows rather than duplicating them, while static sources are fully overwritten each run.

For flights specifically, an explicit schema is defined and enforced on read — every column typed as a string at this stage, with strict, fail-fast handling of any row that doesn't conform — rather than letting Spark infer types from the CSV. Real type conversion and cleanup is deferred to the silver layer; bronze's job is a faithful, schema-validated structural copy.

The flights bronze notebook sets its batch ID, pulls in the bronze environment and helper, resolves the correct landing files for that batch, reads and schema-validates them, and writes the result into a bronze Delta table. Airports and carriers follow the same shape as full loads.

## Lessons learned

A few real issues surfaced while building this, worth documenting for anyone extending the pipeline:

- `replaceWhere` only takes effect under an overwrite write mode — paired with append mode it's silently ignored, so a re-run of the same batch can duplicate rows rather than replacing them. Caught by testing: land the same batch twice and confirm row counts stay flat.
- Spark's hidden per-file lineage column breaks non-Delta writes in this environment — attempting to derive lineage from it while writing to CSV threw an unhelpful, unexplained error. Since each landing call already reads from one known path, lineage is captured as an explicit parameter instead.
- OneLake write paths must use the `abfss://` scheme, not `https://` — the latter looks like a valid path but isn't a Spark-writable filesystem scheme, and fails on the write's commit step. A relative path (resolved against the notebook's own attached lakehouse) avoids the issue entirely for same-lakehouse writes.
- Writing to a lakehouse's managed Tables area and its raw Files area are two different mechanisms — passing a file path where a table name is expected fails with a parse error, since it's interpreted as a SQL identifier rather than a location.

## Next steps

- Silver layer: deduplication, data quality rules, SCD Type 2 on carrier or airport reference data
- Gold layer: star schema (fact_flights plus conformed dimensions), evaluating Lakehouse vs. Warehouse for native RLS/CLS support
- Orchestration: control-table-driven batch loop, wrapped in a Fabric Data pipeline
- CI/CD: Git integration on the Dev workspace, deployment pipeline rules for environment-specific lakehouse bindings
