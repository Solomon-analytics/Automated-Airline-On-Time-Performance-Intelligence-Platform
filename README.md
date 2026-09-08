# Automated-Airline-On-Time-Performance-Intelligence-Platform
Airlines, airports and travel platforms need current, trustworthy visibility into flight reliability to make route, compensation and capacity decisions. Source data arrives monthly, raw and inconsistent (missing delay-cause codes, cancelled vs diverted flights, shifting airport/carrier reference data). Manual reprocessing doesn't scale, and silent failures erode trust in the numbers.

# Aeropulse

A Microsoft Fabric build covering DP-700 Domain 1 (Implement and Manage an Analytics Solution) and Domain 3 (Monitor and Optimize), using US Bureau of Transportation Statistics (BTS) flight data.

Part of a two-project portfolio series:

- **Project 1** ("Aeropulse: ADLS to Gold"): a single working Fabric workspace, medallion architecture, dimensional model.
- **Project 2** (this repository): taking that solution from a single workspace to something that could plausibly run in a team, with environment separation, access control, CI/CD, orchestration and operational visibility.

## What this project sets out to do

| # | Stage | What it covers |
|---|-------|-----------------|
| 1 | Project environment setup | Create the Azure tenant account, grant Fabric access (licence/capacity) and storage access (ADLS Gen2), create and configure the Fabric workspace |
| 2 | Data ingestion | Raw source files into the Landing layer, parameterised by batch |
| 3 | Data transformation (medallion architecture) | Landing to Bronze to Silver: schema enforcement, cleaning, casting, deduplication, data quality flags |
| 4 | Reporting and analytics | Silver to Gold, dimensional model (star schema: fact plus dimensions), feeding a semantic model |
| 5 | Automated orchestration | Control table plus pipeline automation, replacing the manual run-by-hand chain |
| 6 | Monitoring | Monitoring Hub, pipeline run history |
| 7 | Alerting | Data Activator alert on pipeline failure (or a threshold breach) |
| 8 | CI/CD | Dev/Test/Prod workspaces, Git integration, deployment pipeline with environment-specific deployment rules |
| 9 | Data warehouse objects | Analytics views, stored procedures and functions in the Warehouse, answering specific business questions, promoted Dev to Prod via the CI/CD pipeline |
| 10 | Security | OLS, RLS, CLS and data masking, layered onto the warehouse objects, with environment-specific rules handled as deployment parameters |
| 11 | Testing | Reconciliation and validation checks that fail a batch on a data correctness issue, not just a technical exception |
| 12 | Data dictionary | Full documentation of every gold and warehouse table, column and business rule |

---

# Stage 1: Project Environment Setup

- **Microsoft Entra ID**: created a dedicated project user, granted Owner and a Fabric role.
- **Resource group**: created to hold Fabric and any other Azure resources for this project.
- **Fabric capacity**: provisioned, F4 SKU.
- **Fabric workspace**: `aeropulse-dev`, attached to the F4 capacity.
- **Lakehouses**: four created in `aeropulse-dev`, one per medallion layer:
  - `aeropulse_landing_lh`
  - `aeropulse_bronze_lh`
  - `aeropulse_silver_lh`
  - `aeropulse_gold_lh`

---

# Stage 2: Data Ingestion (ADLS to Landing)

- **`landing-environment`**: shared config notebook, holds the ADLS account/container and source paths for airport, carrier and flight. Flight path is built dynamically from `batch_year`/`batch_id`, so it always resolves to the right month's file.
- **`landing-helper`**: defines `write_to_landing()`, the one write function all three ingestion notebooks call. Tags every row with `batch_id`, `ingested_timestamp` and `source_path` for lineage. No transformation happens here, raw CSV in, raw CSV out.
- **Load type per source**:
  - `airport-landing` and `carrier-landing`: **full load**, small reference tables, entire dataset overwritten on every run.
  - `flight-landing`: **incremental**, one month per run, written into its own `batch_id`-named subfolder so re-running a batch never touches another batch's data.

**Why:**

- **`landing-environment`** separates configuration from logic, a standard data engineering practice. Paths and account names live in one place, not copied into three notebooks, so an environment change (e.g. moving to a Test/Prod ADLS account later) is a one-line edit, not a find-and-replace across the codebase.
- **`landing-helper`** centralises the write logic into one reusable, tested function instead of three near-identical copies. This is the DRY principle in practice: one place to fix a bug, one place to add a feature, and every notebook that calls it behaves consistently. Stamping `batch_id`, `ingested_timestamp` and `source_path` on every row also builds lineage in from the start, so any row in Landing can be traced back to exactly which run and file produced it, a basic auditability requirement in any production pipeline.
- Reference data is small and changes rarely, so a full refresh is simpler and cheap. Flight data is the actual fact source and grows every month, so it's processed incrementally and isolated by batch, which also means a failed or re-run batch can't corrupt another batch's data.
- Landing does no cleaning or casting by design. Keeping the raw copy untouched and auditable, and pushing all transformation downstream to Bronze/Silver, is standard medallion practice: it means the original source can always be replayed if a transformation rule turns out to be wrong.

---

# Stage 3a: Data Transformation — Bronze (Landing to Bronze)

- **`bronze-environment`**: shared config, holds the landing lakehouse path and the per-source landing folder locations that every bronze notebook reads from.
- **`bronze-helper`**: defines `write_to_bronze()`, the one write function used by all three notebooks. Writes as a managed Delta table with `mergeSchema` enabled. Full load overwrites the whole table; incremental uses `replaceWhere` plus `partitionBy` on `batch_id`, so re-running a batch only replaces that batch's own rows.
- **`airport-landing-to-bronze`** and **`carrier-landing-to-bronze`**: explicit schema enforced on read, `FAILFAST` on any non-conforming row, full load into `bronze_airport` / `bronze_carrier`.
- **`flight-landing-to-bronze`**: same explicit-schema-plus-`FAILFAST` pattern, every column read as a string, incremental load into `bronze_flight` for the current `batch_id` only.

**Why:**

- **Config/logic separation and a single reusable write function**, the same DRY and single-source-of-truth reasoning as the landing layer, kept consistent through every layer of the pipeline.
- **Explicit schema plus `FAILFAST`** is a fail-fast control: a malformed or unexpected row is caught the moment it enters Bronze, not silently let through to poison Silver or Gold. It also means the schema is documented in code, not inferred and guessed at.
- **Every column typed as a string at this layer** is deliberate. Real type casting is deferred to Silver, so a source value that doesn't convert cleanly never gets lost or nulled out by an over-eager cast happening too early, the raw value is preserved until it's actually validated.
- **`replaceWhere` plus `partitionBy` on `batch_id`** makes the incremental flight load idempotent: re-running a failed or corrected batch replaces only that batch's partition, not the whole table, which is both safer and cheaper than a full overwrite.
- **Full load for airport/carrier** is proportionate to their size, they're small reference tables, so the simplicity of a full overwrite outweighs any benefit of incremental handling.

---

# Stage 3b: Data Transformation — Silver (Bronze to Silver)

- **`silver-transformation-exploration`**: scratch profiling notebook, not part of the production run. Used to actually check the data before writing any rule: row counts, null checks, grain uniqueness, how `CANCELLED`/`DIVERTED` are encoded (1/0), and quantified real inconsistencies (345 cancelled flights still carrying a departure or arrival time). Every decision in the three production notebooks traces back to a finding here.
- **`silver-environment`**: shared config, holds the bronze lakehouse path and per-source table paths.
- **`silver-helper`**: shared, dataset-agnostic functions used by all three notebooks: `remove_duplicates`, `remove_nulls`, `rename_column`, `trim_whitespaces`, `cast_columns`, `add_sk_key` (SHA-256 surrogate key), `add_dq_flag`, and `write_to_silver` (creates the schema/table if needed, otherwise merges, guarded so only a row from an equal-or-newer batch can overwrite an existing one).
- **`airport-bronze-to-silver`**: full refresh. Renames columns, splits `airport_description` into `airport_name` / `airport_city` / `airport_state`, drops null/duplicate `airport_code`, adds `airport_sk`, merges into `silver.airport`.
- **`carrier-bronze-to-silver`**: full refresh. Renames columns, drops null/duplicate `carrier_code`, adds `carrier_sk`, merges into `silver.carrier`.
- **`flight-bronze-to-silver`**: incremental, current `batch_id` only. Renames to descriptive column names, casts delay/time/numeric columns, parses `flight_date` (source format confirmed as `M/d/yyyy h:mm:ss a`), derives `is_cancelled`/`is_diverted` booleans and `flight_date_id`, applies two data quality flags for cancellation consistency, deduplicates on the flight grain, adds `flight_sk`, merges into `silver.flight`.

**Why:**

- **Profiling before ruling**, the exploration notebook's job was to quantify the data before any cleaning rule was written, confirming the join grain, the boolean encoding, and the real scale of each inconsistency, rather than assuming them. This is standard data quality practice: understand the data before you write rules to fix it.
- **Shared helper functions** keep the mechanics (rename, trim, cast, dedupe, surrogate key, data quality flag, merge-write) in one place, so all three notebooks behave consistently and a fix or improvement only needs making once, the same DRY principle carried through from Landing and Bronze.
- **Merge instead of overwrite**, and specifically a batch-guarded merge (`s.batch_id >= t.batch_id`), makes Silver safe to reprocess: a late or corrected batch updates existing rows, but an out-of-order older batch can't clobber newer data.
- **Surrogate keys generated here, not later**, is standard dimensional modelling practice. Gold then always joins on a stable, deterministic key rather than a natural key that could change format or meaning at the source.
- **Data Quality issues are flagged, not silently dropped.** A cancelled flight with a departure time, for example, is kept and marked, so downstream consumers can decide how to treat it rather than losing visibility of a real data quality problem.

---

# Stage 3c: Data Transformation — Gold (Silver to Gold)

- **`gold-environment`**: shared config, holds the silver source paths and the gold dimension paths `fact-flight` reads back once the dimensions exist.
- **`gold-helper`**: `add_sk_key` (SHA-256 surrogate key) and `write_to_gold` (create-or-merge write, stamping `created_timestamp`/`updated_timestamp`).
- **`dim_date`**: generated calendar table (2018-01-01 to 2027-12-31), not sourced from a table. `date_id` (`yyyyMMdd`) is the key `fact_flight` joins on. Full overwrite, not batch-parameterised, it's deterministic and independent of any source data.
- **`dim_origin_airport`**: from silver `airport`, enriched with `origin_city_name` looked up from silver `flight`, since the airport reference table alone doesn't carry the city name as recorded on a flight.
- **`dim_destination_airport`**: built directly from silver `flight`, deduplicated on `destination_airport_code`, rather than from silver `airport`. It only needs the destination code/city pair the fact table joins back to, so going straight to flight is simpler and avoids an unneeded join.
- **`dim_carrier`**: from silver `carrier`, lineage columns (`batch_id`, `ingested_timestamp`, `source_path`) dropped, they don't belong in a reporting-facing dimension.
- **`fact_flight`**: joins the current batch of silver `flight` to the three gold dimensions for their surrogate keys, then derives the reporting measures: `total_delay_minutes` (null-safe sum of the five delay-cause columns), `primary_delay_cause`, `is_delayed` (arrival delay ≥ 15 minutes, the standard FAA/BTS definition), `delay_category` (bucketed severity), `flight_status`, and `route`.

**Why:**

- **Star schema over a flat table** is the standard shape for a reporting/semantic model: a narrow fact table of measures and keys, joined out to conformed dimensions, which is what Power BI (and any BI tool) is built to aggregate and slice efficiently.
- **Origin and destination handled as two separate dimensions**, built differently, rather than one shared airport dimension joined twice, resolves the role-playing dimension problem at build time instead of pushing two aliased joins onto every downstream report.
- **A generated, source-independent date dimension** is standard dimensional modelling practice: calendar attributes (year, quarter, weekday name, weekend flag) shouldn't depend on which transactional source happens to be loaded, and building the full range once avoids re-running this every batch.
- **Business logic derived once, in the fact table**, not left to be recalculated in DAX or SQL by every report. `is_delayed`, `delay_category` and `primary_delay_cause` are computed a single way here, so every consumer of `fact_flight` gets the same answer to "was this flight delayed" without re-implementing the rule.
- **The fact table only carries keys, not descriptive attributes**, from the dimensions, keeping it normalised and the join pattern predictable, another core star schema principle.

---
# Stage 5a: Automated Orchestration — Control Notebooks

- **`00 control-table`**: creates the `control` schema and `batch_control` table, seeds it with every expected `flight` batch as `PENDING`. Safe to re-run, a left-anti join against what's already tracked stops it duplicating rows or resetting a batch already in progress.
- **`01 identify-next-batch`**: finds the oldest `PENDING` batch for a source and hands it back to the pipeline as `has_next_batch` / `batch_id` / `batch_year`, via `mssparkutils.notebook.exit`.
- **`02 create-new-batch`**: marks a batch `IN_PROGRESS` and stamps its start time, run immediately before the landing to gold chain starts.
- **`03 complete-batch`**: marks a batch `COMPLETED` and stamps its end time, wired to the success path of the last transform activity.
- **`04 fail-batch`**: marks a batch `FAILED`, records the error message (escaped and length-capped) and increments `retry_count`, wired to the failure path of any transform activity.

Each notebook takes its `source_name`/`batch_id` (and `error_message` for fail-batch) as pipeline-injected parameters, with defaults only used for standalone testing.

**Why:**

- **A control table is a well established orchestration pattern**: one row per unit of work, moving through an explicit state machine (`PENDING` → `IN_PROGRESS` → `COMPLETED`/`FAILED`), rather than relying solely on the pipeline engine's own run history to know what happened.
- **It decouples batch state from the orchestration tool.** Whether a batch succeeded, is still running, or failed and why is a plain SQL query against `batch_control`, not something that only exists inside Fabric's own monitoring UI, so it can be queried, reported on, or alerted on independently.
- **Explicit state, not implicit success/failure**, gives a genuine audit trail and a retry mechanism for free: reprocessing a batch is as simple as resetting its row back to `PENDING`.
- **Splitting the four actions into their own notebooks** (rather than one notebook branching internally) keeps each one single-purpose and lets the pipeline wire success and failure paths to the right one directly, rather than adding conditional logic inside the notebook itself.

---

# Stage 5b: Automated Orchestration — The Pipeline

**Activities, in order:**

| Activity | Type | Depends on | Base parameters |
|---|---|---|---|
| `identify_next_batch` | Notebook | none | `source_name = "flight"` |
| **If Condition** | If Condition | `identify_next_batch` Succeeded | Expression: `@equals(json(activity('identify_next_batch').output.result.exitValue).has_next_batch, true)` |
| `create-new-batch` | Notebook (True branch) | none (branch root) | `source_name = "flight"`, `batch_id = @json(activity('identify_next_batch').output.result.exitValue).batch_id` |
| `ADLS-landing-flight` | Notebook (True branch) | `create-new-batch` Succeeded | `batch_year = @json(activity('identify_next_batch').output.result.exitValue).batch_year`, `batch_id = @json(activity('identify_next_batch').output.result.exitValue).batch_id` |
| `landing-bronze-flight` | Notebook (True branch) | `ADLS-landing-flight` Succeeded | same `batch_year`/`batch_id` expressions |
| `bronze-silver-flight` | Notebook (True branch) | `landing-bronze-flight` Succeeded | same `batch_year`/`batch_id` expressions |
| `silver-gold-origin-airport` | Notebook (True branch) | `bronze-silver-flight` Succeeded | same `batch_year`/`batch_id` expressions |
| `silver-gold-destination-airport` | Notebook (True branch) | `bronze-silver-flight` Succeeded | same `batch_year`/`batch_id` expressions |
| `silver-gold-flight` | Notebook (True branch) | `silver-gold-origin-airport` **and** `silver-gold-destination-airport` Succeeded | same `batch_year`/`batch_id` expressions |
| `complete-batch` | Notebook (True branch) | `silver-gold-flight` Succeeded | `source_name = "flight"`, `batch_id` expression as above |

**False branch:** empty. No pending batch, the run ends without doing anything further.

**No loop construct.** One pipeline run processes exactly one batch; a monthly schedule trigger fires the next run rather than an Until/For Each draining the whole backlog in one go.

**Why:**

- **Every downstream activity reads `batch_id`/`batch_year` off `identify_next_batch`'s own output**, via the same `json(activity('identify_next_batch').output.result.exitValue).<field>` expression, rather than being passed hand to hand. One source of truth for which batch the run is processing, and one place to change if that ever needs to.
- **Fan out where the DAG genuinely allows it.** Origin and destination airport both only depend on `bronze-silver-flight`, not on each other, so they run in parallel rather than an arbitrary sequence, then fan back into `silver-gold-flight`, which does need both surrogate keys before it can build the fact table.
- **One batch per run, on a monthly trigger**, matches the actual cadence of the source data and keeps each run's scope, and its control-table footprint, easy to reason about, rather than a loop that tries to process everything pending in one go.

**Design note: why airport, carrier, `dim_carrier` and `dim_date` aren't in this pipeline.** All four are full-refresh, source-independent of any batch, and already populated in Gold, so re-running them every month would just be wasted compute

---

# Stage 8: CI/CD

**Scope decision:** two environments, Dev and Prod. Test was deliberately dropped from the original Dev/Test/Prod plan to save time, this is a scoping choice, not a gap.

**Source control:**

- Azure DevOps project and Git repo created.
- `aeropulse-dev` connected to it via workspace Git integration, main branch.
- Branch policy on main: minimum 1 reviewer, no direct commits.
- CI workflow demonstrated end to end: branched out to a feature workspace, made a change, committed, raised a PR in Azure DevOps, reviewed and approved, merged to main, updated the Dev workspace from source control.

**Environment-specific data sources:**

- `flight-data-prod` container created in the same `aeropulse` storage account, mirroring the Dev container's folder layout.
- `landing-environment` parameterised: `source_adls_account_name`/`source_container_name` set as a tagged parameters cell, defaulting to Dev's values.
- Two batches of source files uploaded to `flight-data-prod`.

**Deployment pipeline:**

- `aeropulse-dev-to-prod`, two stages, `aeropulse-dev` assigned to Development and `aeropulse-prod` assigned to Production.
- All items (four lakehouses, every notebook, the orchestration pipeline) deployed Dev to Prod.
- Deployment rules set on the Production stage: a **parameter rule** on `landing-environment` pointing `source_container_name` at `flight-data-prod`, and a **default lakehouse rule** on every other notebook, rebinding each to its corresponding lakehouse inside `aeropulse-prod`.
- Redeployed after setting the rules, since rules only take effect at deployment time.

**Running Prod independently:**

- `00-control-table` run inside `aeropulse-prod` to seed its own `control.batch_control`, separate from Dev's.
- Orchestration pipeline run in `aeropulse-prod`, processing the two uploaded batches against `flight-data-prod` end to end, landing through to gold.

**Access control:**

- Blob Data Storage Contributor role granted on the ADLS Prod container, scoped to what the pipeline actually needs to read there.

**Dev vs Prod, what actually changed:**

| | Dev (`aeropulse-dev`) | Prod (`aeropulse-prod`) |
|---|---|---|
| Fabric workspace | `aeropulse-dev` | `aeropulse-prod` |
| ADLS source container | `flight-data` | `flight-data-prod` |
| `landing-environment` → `source_adls_account_name` | `aeropulse` | `aeropulse` (unchanged) |
| `landing-environment` → `source_container_name` | `flight-data` | `flight-data-prod` (parameter rule) |
| Default lakehouse on landing notebooks | `aeropulse_landing_lh` (Dev) | `aeropulse_landing_lh` (Prod copy, rebound by rule) |
| Default lakehouse on bronze/silver/gold/control notebooks | respective Dev lakehouses | respective Prod lakehouses (rebound by rule) |
| `control.batch_control` | Dev's own instance | Prod's own instance, seeded separately |
| Batches processed | 2, run independently in Dev | 2, run independently in Prod |
| Git integration | connected to Azure DevOps, main branch | not connected, receives changes only via the deployment pipeline |
| How it gets changes | direct commits (via PR) from feature branches | only ever via a deployment from Dev, never edited directly |
| ADLS access | dev-scoped access | Blob Data Storage Contributor, scoped to the Prod container |

Everything in that table other than the workspace name and the two container/parameter values is **code that's identical between the two**, the notebooks and pipeline definition don't fork or duplicate, only the environment-specific bindings differ, and those are set by the deployment rules rather than by hand.

**Why:**

- **Parameterisation over hardcoding.** Fabric's deployment rules can only rebind a notebook's default lakehouse automatically, not arbitrary variables inside it. Turning the ADLS account/container into notebook parameters is what makes them something a deployment rule (or a manual override) can actually target per environment, rather than editing code by hand in every stage.
- **Deployment rules over manual per-environment edits.** Set once on the Production stage, they reapply automatically on every future deployment, so promoting a change from Dev doesn't mean re-doing the environment-specific rebinding each time.
- **Deployment moves item definitions, not data.** Prod's lakehouses arrived empty and needed their own control table seeded and their own batches run, this is expected, not a fault, and is exactly why the two batches processed in Prod are a genuine end-to-end proof rather than a copy of Dev's results.
- **Git integration and a branch policy give this a real audit trail.** A change reaches main only through a reviewed pull request, which is what "changes committed and synced from the Fabric UI" is meant to demonstrate, not just that Git is connected.
- **A scoped RBAC grant on the Prod container**, rather than broad or inherited access, keeps the access control story consistent with the principle of least privilege, worth calling out explicitly rather than leaving implicit.

**Next:** Data Warehouse and analytics objects.





