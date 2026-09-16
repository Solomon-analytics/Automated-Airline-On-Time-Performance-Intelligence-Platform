# Aeropulse

Airline on-time performance, from raw monthly CSV files to a governed reporting layer, built on Microsoft Fabric.

Source data is US Bureau of Transportation Statistics flight records. It arrives monthly, messy, and inconsistent: missing delay-cause codes, cancelled flights that still carry departure times, airport and carrier reference data that shifts underneath you. Reprocessing it by hand does not scale, and silent failures are worse than loud ones because people keep trusting numbers that have quietly stopped being right.

This repository is the second of two projects. [Project 1](#project-1) built a working medallion pipeline in one workspace. This one takes it to something that could run in a team: separate environments, access control, CI/CD, orchestration and monitoring.

![Aeropulse architecture](docs/images/architecture-overview.png)

---

## Contents

- [What this shows](#what-this-shows)
- [Stack](#stack)
- [How the data flows](#how-the-data-flows)
- [Build stages](#build-stages)
  - [1. Environment setup](#1-environment-setup)
  - [2. Ingestion](#2-ingestion)
  - [3. Bronze](#3-bronze)
  - [4. Silver](#4-silver)
  - [5. Gold](#5-gold)
  - [6. Orchestration](#6-orchestration)
  - [7. CI/CD and Deployment Pipeline](#7-cicd)
  - [8. Warehouse and analytics](#8-warehouse-and-analytics)
  - [9. Security & Access Management](#9-security)
  - [10. Reconciliation tests and promotion](#10-reconciliation-tests-and-promotion
- [Two things that went wrong](#two-things-that-went-wrong)
- [Decisions worth explaining](#decisions-worth-explaining)
- [Repository layout](#repository-layout)
- [Screenshots](#screenshots)
- [Project 1](#project-1)

---

## What this shows

| Area | Evidence in this repo |
|---|---|
| Medallion architecture | Landing, Bronze, Silver, Gold as four lakehouses with distinct rules at each hop |
| Dimensional modelling | Star schema, role-playing dimensions resolved at build time, generated date dimension |
| Incremental processing | Batch-partitioned flight loads that are safe to re-run |
| Data quality | Profiling before rules, quality flags rather than silent drops |
| Orchestration | Control table state machine, parameterised pipeline, parallel fan-out |
| CI/CD & Deployment Pipeline | Git integration, branch policy, deployment pipeline with environment rules |
| SQL engineering | Views, stored procedures, scalar and table-valued functions, chosen deliberately |
| Security &  Access Management | Object, column and row-level security, plus dynamic data masking, tested as a real non-admin user |
| Operations | Monitoring, failure alerting, reconciliation tests that fail on bad data rather than only on exceptions |

## Stack

Microsoft Fabric (Lakehouse, Warehouse, Data Factory pipelines, Data Activator), PySpark, T-SQL, Delta Lake, Azure Data Lake Storage Gen2, Azure DevOps, Power BI.

---

## How the data flows

![Medallion layers](docs/images/architecture-medallion.png)

**Landing** holds the raw file exactly as it arrived, with three columns stamped on for lineage: `batch_id`, `ingested_timestamp`, `source_path`. Nothing is cleaned or cast here. Any row can be traced back to the run and file that produced it, and the original can always be replayed if a downstream rule turns out to be wrong.

**Bronze** enforces an explicit schema with `FAILFAST`, so a malformed row is caught at the door rather than poisoning Silver. Everything is read as a string on purpose. Casting happens one layer later, once the value has been validated, so nothing gets nulled out by a cast that fired too early.

**Silver** is where the cleaning lives: renaming, trimming, casting, deduplication, surrogate keys, data quality flags. Writes are a batch-guarded merge, so a corrected batch can update existing rows but an out-of-order older batch cannot overwrite newer data.

**Gold** is the star schema: `fact_flight` joined out to date, carrier, origin airport and destination airport. Reporting logic such as `is_delayed` and `delay_category` is derived once here so every consumer gets the same answer.

**Warehouse** sits on top of Gold and serves the business-facing layer through views, procedures and functions, with security applied.

---

## Build stages

<a id="1-environment-setup"></a>
<details>
<summary><b>1. Environment setup</b></summary>

- **Microsoft Entra ID**: created a dedicated project user, granted Owner and a Fabric role.
- **Resource group**: created to hold Fabric and any other Azure resources for this project.
- **Fabric capacity**: provisioned, F4 SKU.
- **Fabric workspace**: `aeropulse-dev`, attached to the F4 capacity.
- **Lakehouses**: four created in `aeropulse-dev`, one per medallion layer:
  - `aeropulse_landing_lh`
  - `aeropulse_bronze_lh`
  - `aeropulse_silver_lh`
  - `aeropulse_gold_lh`

![Workspace setup](docs/images/01-workspace-lakehouses.png)

</details>

<a id="2-ingestion"></a>
<details>
<summary><b>2. Ingestion</b></summary>

Three sources, two load patterns.

Airport and carrier are small reference tables, so they are fully refreshed on every run. Flight is the fact source and grows monthly, so it loads one month per run into its own `batch_id` folder. Re-running a batch cannot touch another batch's data.

Two shared notebooks do the heavy lifting. `landing-environment` holds paths and account names in one place, so moving to a different storage account later is a one-line change rather than a hunt through three notebooks. `landing-helper` holds the single write function all three ingestion notebooks call, which means one place to fix a bug and one place to add a feature.

![Landing layer](docs/images/02-landing-notebooks.png)

</details>

<a id="3-bronze"></a>
<details>
<summary><b>3. Bronze</b></summary>

Same config and helper split as Landing, carried through deliberately so every layer behaves the same way.

The incremental flight load uses `replaceWhere` with `partitionBy` on `batch_id`. Re-running a failed batch replaces only that batch's partition. That makes reprocessing safe and cheap rather than a full table rewrite.

- **`bronze-environment`**: shared config, holds the landing lakehouse path and the per-source landing folder locations that every bronze notebook reads from.
- **`bronze-helper`**: defines `write_to_bronze()`, the one write function used by all three notebooks. Writes as a managed Delta table with `mergeSchema` enabled. Full load overwrites the whole table; incremental uses `replaceWhere` plus `partitionBy` on `batch_id`, so re-running a batch only replaces that batch's own rows.
- **`airport-landing-to-bronze`** and **`carrier-landing-to-bronze`**: explicit schema enforced on read, `FAILFAST` on any non-conforming row, full load into `bronze_airport` / `bronze_carrier`.
- **`flight-landing-to-bronze`**: same explicit-schema-plus-`FAILFAST` pattern, every column read as a string, incremental load into `bronze_flight` for the current `batch_id` only.

![Bronze tables](docs/images/03-bronze-tables.png)

</details>

<a id="4-silver"></a>
<details>
<summary><b>4. Silver</b></summary>

- **`silver-transformation-exploration`**: Before writing a single cleaning rule, I profiled the data: row counts, null checks, grain uniqueness, how cancelled and diverted are actually encoded, and how big each inconsistency really was. That found 345 cancelled flights still carrying a departure or arrival time. Every rule in the production notebooks traces back to something measured in that profiling notebook rather than something assumed.

Those 345 rows are flagged, not deleted. A cancelled flight with a departure time is a real data quality problem, and dropping it hides the problem instead of surfacing it. Downstream consumers can then decide what to do with it.

- **`silver-environment`**: shared config, holds the bronze lakehouse path and per-source table paths.
- **`silver-helper`**: shared, dataset-agnostic functions used by all three notebooks: `remove_duplicates`, `remove_nulls`, `rename_column`, `trim_whitespaces`, `cast_columns`, `add_sk_key` (SHA-256 surrogate key), `add_dq_flag`, and `write_to_silver` (creates the schema/table if needed, otherwise merges, guarded so only a row from an equal-or-newer batch can overwrite an existing one).
- **`airport-bronze-to-silver`**: full refresh. Renames columns, splits `airport_description` into `airport_name` / `airport_city` / `airport_state`, drops null/duplicate `airport_code`, adds `airport_sk`, merges into `silver.airport`.
- **`carrier-bronze-to-silver`**: full refresh. Renames columns, drops null/duplicate `carrier_code`, adds `carrier_sk`, merges into `silver.carrier`.
- **`flight-bronze-to-silver`**: incremental, current `batch_id` only. Renames to descriptive column names, casts delay/time/numeric columns, parses `flight_date` (source format confirmed as `M/d/yyyy h:mm:ss a`), derives `is_cancelled`/`is_diverted` booleans and `flight_date_id`, applies two data quality flags for cancellation consistency, deduplicates on the flight grain, adds `flight_sk`, merges into `silver.flight`.

Surrogate keys are generated here using SHA-256, so Gold joins on a stable key rather than a natural key whose format or meaning might change at the source.


![Silver profiling](docs/images/04-silver-profiling.png)

</details>

<a id="5-gold"></a>
<details>
<summary><b>5. Gold</b></summary>

A star schema: one narrow fact table of keys and measures, joined out to conformed dimensions. That is the shape Power BI is built to aggregate efficiently.

Origin and destination are built as two separate dimensions rather than one airport dimension joined twice.

- **`gold-environment`**: shared config, holds the silver source paths and the gold dimension paths `fact-flight` reads back once the dimensions exist.
- **`gold-helper`**: `add_sk_key` (SHA-256 surrogate key) and `write_to_gold` (create-or-merge write, stamping `created_timestamp`/`updated_timestamp`).
- **`dim_date`**: generated calendar table (2018-01-01 to 2027-12-31), not sourced from a table. `date_id` (`yyyyMMdd`) is the key `fact_flight` joins on. Full overwrite, not batch-parameterised, it's deterministic and independent of any source data.
- **`dim_origin_airport`**: from silver `airport`, enriched with `origin_city_name` looked up from silver `flight`, since the airport reference table alone doesn't carry the city name as recorded on a flight.
- **`dim_destination_airport`**: built directly from silver `flight`, deduplicated on `destination_airport_code`, rather than from silver `airport`. It only needs the destination code/city pair the fact table joins back to, so going straight to flight is simpler and avoids an unneeded join.
- **`dim_carrier`**: from silver `carrier`, lineage columns (`batch_id`, `ingested_timestamp`, `source_path`) dropped, they don't belong in a reporting-facing dimension.
- **`fact_flight`**: joins the current batch of silver `flight` to the three gold dimensions for their surrogate keys, then derives the reporting measures: `total_delay_minutes` (null-safe sum of the five delay-cause columns), `primary_delay_cause`, `is_delayed` (arrival delay ≥ 15 minutes, the standard FAA/BTS definition), `delay_category` (bucketed severity), `flight_status`, and `route`.

![Star schema](docs/images/05-star-schema.png)

</details>

<a id="6-orchestration"></a>
<details>
<summary><b>6. Orchestration</b></summary>

A `batch_control` table holds one row per unit of work, moving through an explicit state machine: `PENDING`, `IN_PROGRESS`, then `COMPLETED` or `FAILED`.

The point of this is that batch state lives in SQL rather than only inside Fabric's monitoring UI. Whether a batch succeeded, is still running, or failed and why is a query anyone can run, report on or alert from. Reprocessing a batch is as simple as resetting its row to `PENDING`.

- **`00 control-table`**: creates the `control` schema and `batch_control` table, seeds it with every expected `flight` batch as `PENDING`. Safe to re-run, a left-anti join against what's already tracked stops it duplicating rows or resetting a batch already in progress.
- **`01 identify-next-batch`**: finds the oldest `PENDING` batch for a source and hands it back to the pipeline as `has_next_batch` / `batch_id` / `batch_year`, via `mssparkutils.notebook.exit`.
- **`02 create-new-batch`**: marks a batch `IN_PROGRESS` and stamps its start time, run immediately before the landing to gold chain starts.
- **`03 complete-batch`**: marks a batch `COMPLETED` and stamps its end time, wired to the success path of the last transform activity.
- **`04 fail-batch`**: marks a batch `FAILED`, records the error message (escaped and length-capped) and increments `retry_count`, wired to the failure path of any transform activity.

The pipeline itself processes one batch per run on a monthly trigger, matching the cadence of the source. Origin and destination airport run in parallel because neither depends on the other, then fan back in to the fact table, which needs both sets of surrogate keys.

Each notebook takes its `source_name`/`batch_id` (and `error_message` for fail-batch) as pipeline-injected parameters, with defaults only used for standalone testing.

# 6b: Automated Orchestration — The Pipeline

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

![Orchestration pipeline](docs/images/06-pipeline-dag.png)
![Control table](docs/images/06-control-table.png)

</details>

<a id="7-cicd & deployment pipeline"></a>
<details>
<summary><b>7. CI/CD & deployment pipeline</b></summary>

Two environments, Dev and Prod. Test was dropped to keep the scope sensible, which was a choice rather than an oversight.

`aeropulse-dev` is connected to Azure DevOps with a branch policy on main requiring one reviewer, which is also what blocks direct pushes. Every change has to arrive through a pull request. The policy is proven rather than just configured: an attempted direct commit from the workspace was rejected with `Git_GitProviderCommitRejectedByPolicy`.

Promotion runs through a deployment pipeline with rules set on the Production stage. A parameter rule points the ADLS container at `flight-data-prod`, and default lakehouse rules rebind every notebook to its Prod counterpart. The notebooks and pipeline definitions are byte-identical between environments. Only the bindings differ, and those are set by rules rather than by hand.

Prod then seeds its own control table and runs its own batches. Deployment carries item definitions, not data, so a green Prod is real evidence the solution works there rather than a copy of Dev's results.

![Deployment pipeline](docs/images/07-deployment-pipeline.png)
![Branch policy](docs/images/07-branch-policy.png)

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
| How it gets changes | pull requests from feature branches, merged into main, then pulled down | only ever via a deployment from Dev, never edited directly |
| ADLS access | dev-scoped access | Blob Data Storage Contributor, scoped to the Prod container |

Everything in that table other than the workspace name and the two container/parameter values is **code that's identical between the two**, the notebooks and pipeline definition don't fork or duplicate, only the environment-specific bindings differ, and those are set by the deployment rules rather than by hand.

</details>

<a id="8-warehouse-and-analytics"></a>
<details>
<summary><b>8. Warehouse and analytics</b></summary>

## What was built

A Fabric Warehouse, `aeropulse_wh`, sitting on top of the Gold lakehouse and serving the business-facing analytics layer.

**Two schemas:**

| Schema | Contents | Reached by |
|---|---|---|
| `dbo` | Five base tables loaded from Gold | The load procedure only |
| `analytics` | Views, functions, procedures, summary table | Report consumers |

**Base tables:** `dim_date`, `dim_origin_airport`, `dim_destination_airport`, `dim_carrier`, `fact_flight`.

**Analytics objects, and the business question each answers:**

| Object | Type | Business question |
|---|---|---|
| `vw_daily_flight_performance` | View | How many flights ran on a given day, what share arrived on time, and which origin airports performed worst |
| `vw_carrier_monthly_otp` | View | How each carrier is tracking month on month against an 80% on-time target, and how they rank against each other |
| `vw_route_performance` | View | Which routes are consistently late once volume is taken into account |
| `vw_cancellation_analysis` | View | What is actually driving cancellations, by carrier, airport and month |
| `usp_carrier_performance_summary` | Stored procedure | How a given carrier performed over any date range, and whether delay accumulates through the day |
| `usp_refresh_monthly_summary` | Stored procedure | Rebuilds the pre-aggregated monthly carrier summary for one batch |
| `usp_load_warehouse_from_gold` | Stored procedure | Reloads every Warehouse table from Gold |
| `fn_departure_time_band` | Scalar function | Which part of the day a flight departed in |
| `fn_flights_in_range` | Inline table-valued function | A joinable flight set for a given date range |

## How it was built

**Build scripts, numbered by run order.** The order is not arbitrary: functions must exist before the views and procedures that call them.

| Script | Creates |
|---|---|
| `01_create_schema_and_tables.sql` | `analytics` schema, five base tables in `dbo` |
| `02_create_and_load_stored_proc.sql` | `usp_load_warehouse_from_gold`, then runs it |
| `03_create_functions.sql` | Both functions |
| `04_create_views.sql` | Four analytics views |
| `05_create_summary_table_and_refresh.sql` | `monthly_carrier_summary` and its refresh procedure |
| `06_create_reporting_stored_proc.sql` | `usp_carrier_performance_summary` |

**Tables are created with explicit DDL** rather than CTAS, and loaded with TRUNCATE and INSERT. `cancellation_code` was added later with `ALTER TABLE` rather than by rebuilding.

**Cross-database loading.** The load procedure reaches the Gold lakehouse by three-part name, `aeropulse_gold_lh.dbo.<table>`. Because that lakehouse carries the same name in both Dev and Prod, the procedure needs no environment-specific variant.

**Refresh runs in two steps, in order**, after the gold notebook in the orchestration pipeline:

1. `usp_load_warehouse_from_gold`, no parameters.
2. `usp_refresh_monthly_summary`, taking the run's `batch_id`.

Both are Stored procedure activities in the pipeline, with `batch_id` bound to the same expression every other activity uses. The order matters because the summary reads from `fact_flight` in the Warehouse rather than from Gold, so the Warehouse has to be current before the summary is recalculated.

**Testing** covers row counts reconciled between Warehouse and Gold, orphaned surrogate key checks on carrier, origin airport and date, smoke tests on every view and function, and a cancellation consistency check confirming every cancelled flight carries a code and no uncancelled flight does.

![Warehouse objects](docs/images/08-warehouse-objects.png)

</details>

<a id="9-security"></a>
<details>
<summary><b>9. Security & Access Management</b></summary>
## What was built

Two test identities in Microsoft Entra ID, created with no directory role and no Azure RBAC, so that every permission they hold is one granted deliberately in Fabric or in T-SQL:

| User | Purpose |
|---|---|
| `aeropulse-bi-analyst` | Analyst persona used to test item sharing, workspace roles and granular SQL security |
| `aeropulse-operation-analyst` | Second identity for comparison testing |

Access was then exercised at every layer Fabric exposes, from tenant down to individual column and row.

**Administrative layers:**

| Layer | Scope | What it does not grant |
|---|---|---|
| Fabric Administrator | Tenant-wide. Automatically carries domain and capacity admin rights, and can self-elevate to any Fabric role | Workspace access is not automatic |
| Capacity Administrator | One compute resource. Controls which workspaces use the capacity and its workload settings | No domain rights, no workspace access |
| Domain Administrator | One domain. Administrative tasks within it | Cannot administer other domains, cannot add or remove domain admins, no workspace access without a separate role |
| Domain Contributor | Can add workspaces to a domain | Almost nothing else |

Fabric Administrator is assigned as an Entra ID directory role. Capacity Administrator is assigned either in the Fabric admin portal or on the capacity resource in Azure.

**Workspace roles, observed behaviour:**

| Role | What the user could actually do |
|---|---|
| Viewer | Read notebooks and pipeline activities. Could not view lakehouse tables or files |
| Contributor | Query the SQL endpoint, run notebooks, read tables and files, run the orchestration pipeline. Could not add other users |
| Member | Everything Contributor can do, plus adding other users |

**Item-level sharing on the gold lakehouse:**

| Permission | Effect |
|---|---|
| Share with no additions | Opens the lakehouse and SQL endpoint, connects via SSMS, reads the default semantic model. No file access. Granular SQL security then layers on top |
| Read all SQL endpoint data | Read on everything exposed through the T-SQL endpoint, equivalent to `db_datareader` |
| Read all Apache Spark and subscribe to events | Reads delta tables through Spark notebooks, grants raw file access, enables event subscriptions and shortcuts |
| Execute Apache Spark jobs | Does not enable querying. Appears to exist for scheduling jobs rather than reading data |

**Granular security, implemented and tested in a Warehouse:**

- **Object-level security** on schemas and tables, using `GRANT`, `DENY` and `REVOKE`.
- **Column-level security**, granting `SELECT` on a named column list so that a sensitive column is excluded.
- **Row-level security**, using a predicate function and a security policy to restrict a user to their own region.
- **Dynamic data masking**, using all four masking functions: `default()`, `email()`, `random()` and `partial()`.

## How it was built

**Object-level security.** Grant on the schema, or on individual tables where access should be narrower:

```sql
GRANT SELECT ON SCHEMA::<schema> TO [user];
GRANT SELECT ON <schema>.<table> TO [user];
DENY  SELECT ON <schema>.<table> TO [user];
REVOKE SELECT ON <schema>.<table> TO [user];
```

Sharing the Warehouse with no additional item permissions left the user unable to read any table. Access appeared only as each `GRANT` was issued.

**Column-level security.** Same statements, scoped to a column list:

```sql
GRANT SELECT ON <schema>.employees (emp_id, full_name, department) TO [user];
```

`salary` was deliberately omitted. Querying it returned:

```
Msg 230: The SELECT permission was denied on the column 'salary' of the object 'employees'
```

`REVOKE` on `department` then produced the same error for that column, confirming that revoking clears a permission rather than granting or denying one: the column returned to its default state, which is no access.

**Row-level security.** A predicate function matched against the signed-in user, then a security policy binding it to the table:

```sql
CREATE FUNCTION access_management.fn_sales_rls(@region VARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS result
WHERE @region IN (
    SELECT region FROM access_management.user_region
    WHERE user_email = USER_NAME()
);
GO

CREATE SECURITY POLICY access_management.sales_rls_policy
ADD FILTER PREDICATE access_management.fn_sales_rls(region)
ON access_management.sales
WITH (STATE = ON);
```

The mapping table tied the analyst to the north region. Querying `sales` as that user returned only north rows, with no filter in the query itself.

**Dynamic data masking.** Applied at table creation, then altered and dropped to test each operation:

```sql
-- at creation
email_address     VARCHAR(100) MASKED WITH (FUNCTION = 'email()'),
credit_card_number VARCHAR(20) MASKED WITH (FUNCTION = 'partial(0,"XXXX-XXXX-XXXX-",4)'),
random_number      INT         MASKED WITH (FUNCTION = 'random(1000, 9999)'),
salary             DECIMAL(10,2) MASKED WITH (FUNCTION = 'default()')

-- add, remove, and override
ALTER TABLE <schema>.<table> ALTER COLUMN <col> ADD MASKED WITH (FUNCTION = 'default()');
ALTER TABLE <schema>.<table> ALTER COLUMN <col> DROP MASKED;
GRANT  UNMASK ON <schema>.<table> TO [user];
REVOKE UNMASK ON <schema>.<table> TO [user];
```

**Testing method.** Every permission was verified by signing in as the test user and running the same query, rather than by inspecting the configuration. The admin account sees unmasked values throughout, so masking can only be confirmed from a non-privileged session. The `random()` function returned a different value on each execution, which is visible across repeated runs.

## Why

**Least privilege is the organising principle.** Both test users were created with no Entra directory role and no Azure RBAC, so nothing is inherited and every capability they have was granted on purpose. That makes the access model auditable: the answer to "why can this user see this" is always a specific grant, never an accident of inheritance. It also limits the blast radius if an account is compromised, and keeps role reviews meaningful.

**The layers are independent, and that is what makes testing meaningful.** A workspace role and an item share are alternative routes to the same data, not a sequence. Granting a workspace role of Contributor or above effectively bypasses granular SQL security, because workspace access to a Warehouse carries full read. Testing RLS or masking against such a user proves nothing. Everything here was therefore tested against a user with no workspace role at all.

**Grant upwards rather than deny downwards.** Sharing an item without additional permissions, then granting specific access in T-SQL, produces an access model that is easy to read and easy to reason about. Starting from broad access and carving exceptions with `DENY` gets harder to audit with every exception, and mixes two mechanisms so it stops being obvious which one is deciding the outcome.

**Separating `dbo` from `analytics` in the Warehouse exists precisely for this.** Base tables in one schema, consumer-facing views in another, so access can be granted on the governed layer and withheld from the raw fact table. Object-level security falls out of the structure rather than being retrofitted onto a flat schema.

**Masking is not a security boundary.** It prevents accidental exposure, not determined access. A user who can query a table can often infer or reconstruct masked values, and anyone with a privileged workspace role sees through it entirely. It belongs on top of object, column and row security, never instead of them.

**Row-level security belongs in the database, not the query.** Putting the predicate in a security policy means the restriction holds no matter how the user reaches the data: a SQL client, a notebook, a report. Filtering in a view or a report can always be worked around by querying the underlying table directly.

**Governance sits alongside access, not inside it.** Endorsement (promoted, certified, master data), tagging and sensitivity labels do not grant or restrict access. They tell people what an item is and how far it can be trusted, which is a different problem from who may read it. Certification is restricted to reviewers a Fabric administrator nominates, precisely so that "certified" keeps meaning something.

![Row level security](docs/images/09-rls-test.png)
![Dynamic data masking](docs/images/09-masking-test.png)

</details>

<a id="10-reconciliation-tests-and-promotion"></a>
<details>
<summary><b>10. Reconciliation tests and promotion</b></summary>

A single PySpark notebook, `reconciliation-validation-tests`, checks one run end to end
across landing, bronze, silver and gold. Built and proven in `aeropulse-dev`, committed
through Azure DevOps, then promoted to `aeropulse-prod` by the deployment pipeline.

### What it checks

Counts do not reconcile the same way at every hop, and pretending they do is how a test
ends up agreeing with whatever the data happens to be.

| Hop | Expected | Why |
|---|---|---|
| Landing to bronze | Exactly equal | Bronze does no filtering or dedup. `FAILFAST` either passed everything or threw |
| Bronze to silver | Silver equals the distinct non-null grain in bronze | Silver dedupes and drops null keys, so it can only shrink, and by a knowable amount |
| Silver to gold | Exactly equal | The dimension joins should not drop facts |

That middle one is the one usually fudged as "silver is smaller, looks about right". The
shrinkage is calculable, so it is checked exactly.

On top of the row counts: surrogate key uniqueness, referential integrity from the fact
to every dimension, date keys resolving against the generated calendar, lineage columns
surviving into bronze, and every batch appearing in every layer.

Two checks earn their place beyond the obvious. **Dimensions are not empty**, which is
the check that would have caught the empty `dim_carrier` that reached Production earlier
in this project. And **derived rules still agree with the data they came from**, testing
`is_delayed` against the 15 minute arrival delay rule it was built from. Row counts and
key integrity both pass happily while business logic has quietly drifted.

![Reconciliation test run](docs/images/10-reconciliation-test-run.png)

### It has to fail loudly

A failed check raises, which fails the notebook. A test that only writes results
somewhere needs a person to go and look, and nobody does. The point is that a data
correctness problem stops the run the same way a technical exception does.

### One change before it could be promoted

The notebook resolves its OneLake paths from a single `WORKSPACE` constant. Left as an
ordinary variable it would have travelled to Production still pointing at Dev, passed
every check against Dev data, and reported green.

Fabric deployment rules can rebind a default lakehouse and can override a parameter cell
variable. They cannot touch an arbitrary constant sitting in a normal code cell. So
`WORKSPACE` moved into a parameter cell with a deployment rule on the Production stage,
the same pattern already used for the ADLS container in Stage 7. There is also an assert
that all four layer paths sit in the same workspace, so a half-edited config fails
immediately rather than silently comparing Dev against Prod.

### Committing through Azure DevOps

The workspace is bound to `main`, which has a branch policy requiring a reviewer, so a
direct commit is rejected. Changes go out on a feature branch, through a pull request,
get approved, and merge to `main`. The workspace then updates from source control.

Two items in this change: the new notebook, and the warehouse it sits alongside.

![Commit and approval in Azure DevOps](docs/images/devops-CICD-commit.png)

### Deploying to Production

The deployment pipeline compares the two stages and shows the notebook as **Only in
source**, since Prod has never seen it. Deploying carries the item definition across.

Unlike the warehouse promotion in Stage 8b, this one needed no sequencing. The notebook
reads Delta paths at runtime rather than resolving columns at import time, so nothing
binds to another item during deployment and it goes over in a single pass.

![Deploying from dev to prod](docs/images/deploying-changes-from-dev-to-prod.png)

Prod then runs the tests against its own batches. That matters: the checks compare layers
within one workspace, so a green run in Prod is evidence the solution works there rather
than a copy of Dev's result.

</details>


---

## Two things that went wrong

These are the parts I would want to talk through in an interview.

### The Warehouse would not deploy

Promoting the Warehouse to Prod failed on import:

```
DmsImportDatabaseException ... File: dbo/StoredProcedures/usp_load_warehouse_from_gold.sql,
Error: Invalid column name 'cancellation_code'.
```

Fabric recreates each Warehouse object by executing its DDL. Creating the load procedure meant resolving every column it references, including one in the Gold lakehouse. Prod's Gold table existed but did not yet have `cancellation_code`, because the same release added the column upstream. Deferred name resolution does not save you here: it covers a missing table, not a missing column on a table that resolves.

The import also does not roll back cleanly. It left a half-built Warehouse in Prod with `dbo` present and no `analytics` schema, which had to be deleted before retrying.

The fix was to sequence the release: deploy the notebooks first, rebuild Prod's Gold using them, delete the partial Warehouse, then deploy the Warehouse. The lesson is that a Warehouse procedure reading a Lakehouse table binds those two items together at deployment time, not just at runtime. Any release touching both sides of that boundary needs ordering, which is ordinary release management rather than a Fabric quirk.

### An entire dimension was empty in Production

Verification found `dim_carrier` empty in Prod. The Warehouse load mirrors Gold, so an empty target meant an empty source.

The cause was upstream. Airport and carrier are full-refresh reference sources, and I had deliberately left them outside the orchestration pipeline to run by hand. That worked in Dev, where I had run them. It failed in Prod, where I had not. Worse, the Gold reference notebooks filtered their Silver source on `batch_id`, so the mismatch produced an empty dimension rather than an error.

Two things came out of this. Any step that exists only as something a person remembers to do will hold in the environment where it was done and quietly fail everywhere else, and the saving from keeping those small tables out of the pipeline was not worth a missing dimension in Production. And a `batch_id` filter on a full-refresh table returns an empty set rather than an error, which turns a bug into silence. Removing the filter converts that into either correct data or a visible failure, and a visible failure is the more useful of the two.

---

## Decisions worth explaining

**Configuration lives apart from logic.** Paths and account names sit in one shared notebook per layer, so an environment change is one edit rather than a find and replace across the codebase.

**One write function per layer.** Three near-identical copies become three places to fix the same bug. The helper notebooks exist so that every source behaves consistently and improvements only need making once.

**Landing stays raw.** No cleaning, no casting. If a transformation rule turns out to be wrong six months later, the original is still there to replay.

**Fail at the door.** Explicit schemas with `FAILFAST` in Bronze mean a bad row is rejected on entry rather than quietly corrupting a dimension three layers down.

**Re-running a batch is safe.** `replaceWhere` on a partitioned `batch_id` in Bronze, a batch-guarded merge in Silver, and a clear-then-insert in the Warehouse summary. Nothing in the chain duplicates or clobbers on a second run.

**Flag data quality problems, do not drop them.** A dropped row is an invisible problem. A flagged row is a decision someone can make.

**Derive business logic once.** `is_delayed` uses the standard 15 minute BTS definition and is computed in Gold, not reimplemented in DAX or in each view. The same rule in two places eventually becomes two different rules.

**Put logic where it belongs.** `primary_delay_cause` compares five columns against each other, so it is transformation and belongs upstream. `cancellation_code` is a four-value lookup on one column, so it is presentation and gets decoded in the view.

**Grant upwards rather than deny downwards.** Share an item with nothing attached, then grant specific access. Starting broad and carving exceptions with `DENY` gets harder to audit with every exception.

**Masking is not a security boundary.** It prevents accidental exposure. It does not stop determined access, and a privileged workspace role sees straight through it. It belongs on top of object, column and row security, never instead of them.

**Production is a destination, never a source.** No object in the Prod Warehouse was created by hand. Everything arrived by deployment, and the only SQL run there is the load, the refresh and the verification queries.

---

## Repository layout

```
notebooks/
  landing/          config, helper, three ingestion notebooks
  bronze/           config, helper, three landing to bronze notebooks
  silver/           config, helper, profiling, three bronze to silver notebooks
  gold/             config, helper, four dimensions, one fact
  control/          control table plus four state transition notebooks
warehouse/
  01_create_schema_and_tables.sql
  02_create_and_load_stored_proc.sql
  03_create_functions.sql
  04_create_views.sql
  05_create_summary_table_and_refresh.sql
  06_create_reporting_stored_proc.sql
  security/         object, column and row level security, masking
  tests/            reconciliation and validation queries
docs/
  images/           screenshots and diagrams
  data-dictionary.md
```

Warehouse scripts are numbered by run order. Functions have to exist before the views and procedures that call them.

---

## Screenshots

<details>
<summary><b>Environment and ingestion</b></summary>

![Entra user](docs/images/gallery/01-entra-user.png)
![Fabric capacity](docs/images/gallery/01-capacity.png)
![ADLS container](docs/images/gallery/02-adls-container.png)
![Landing output](docs/images/gallery/02-landing-output.png)

</details>

<details>
<summary><b>Transformation</b></summary>

![Bronze schema enforcement](docs/images/gallery/03-bronze-schema.png)
![Silver quality flags](docs/images/gallery/04-silver-flags.png)
![Gold dimensions](docs/images/gallery/05-gold-dims.png)
![Fact table](docs/images/gallery/05-fact-flight.png)

</details>

<details>
<summary><b>Orchestration and monitoring</b></summary>

![Pipeline success run](docs/images/gallery/06-pipeline-success.png)
![Pipeline failure path](docs/images/gallery/06-pipeline-failure.png)
![Monitoring hub](docs/images/gallery/06-monitoring-hub.png)
![Data Activator alert](docs/images/gallery/06-alert.png)

</details>

<details>
<summary><b>CI/CD</b></summary>

![Pull request](docs/images/gallery/07-pull-request.png)
![Commit rejected by policy](docs/images/gallery/07-commit-rejected.png)
![Deployment comparison](docs/images/gallery/07-deploy-compare.png)
![Deployment rules](docs/images/gallery/07-deploy-rules.png)

</details>

<details>
<summary><b>Warehouse and security</b></summary>

![Analytics views](docs/images/gallery/08-views.png)
![Reconciliation tests](docs/images/gallery/08-tests.png)
![Column level security denial](docs/images/gallery/09-cls-denied.png)
![Masked output as test user](docs/images/gallery/09-masked-output.png)

</details>

---

## Project 1

[Aeropulse: ADLS to Gold](#) builds the foundation this repository extends: one Fabric workspace, the medallion architecture, and the dimensional model.

Together the two cover DP-700 Domain 1 (Implement and Manage an Analytics Solution) and Domain 3 (Monitor and Optimize).

