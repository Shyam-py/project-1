# project-1
# NYC Yellow Taxi — End-to-End Data Engineering Pipeline

A production-style data engineering project built on **Databricks Free Edition** using the **medallion architecture** (Bronze → Silver → Gold). The pipeline ingests raw NYC Taxi & Limousine Commission (TLC) trip data, cleans and conforms it, and produces analytics-ready tables that answer concrete business questions.

**Stack:** Databricks (serverless), PySpark, Spark SQL, Delta Lake, Unity Catalog, Databricks Jobs, Great Expectations, Terraform, GitHub Actions.

---

## Business Questions (the reason this pipeline exists)

The Gold layer is built to answer three specific questions. Every upstream decision serves these.

**Q1 — Demand (Operations).**
What are the peak-demand hours and days of week, broken down by pickup borough?
*Stakeholder:* fleet operations deciding where and when to position drivers.

**Q2 — Revenue & Tipping (Finance).**
Which pickup zones generate the highest average revenue and tip rate per trip, and does payment type predict tipping?
*Stakeholder:* revenue analyst optimizing high-value zones.
*Known limitation:* cash tips are not recorded in the data; tip analysis is valid only for card payments. This is acknowledged, not hidden.

**Q3 — Congestion Pricing Impact (Strategy).**
After NYC congestion pricing took effect (January 2025), how did trip volume and average total cost change for trips into the Manhattan congestion zone versus outside it?
*Stakeholder:* policy/strategy team assessing a real regulatory change.
*Why it matters:* forces the pipeline to handle real schema drift — the `cbd_congestion_fee` column exists only from 2025 onward.

**Q4 — Year-over-Year Seasonal Comparison (Strategy).**
How did trip volume and average base fare change year-over-year for the same season — November–December 2024 versus November–December 2025?
*Stakeholder:* strategy team tracking demand growth/decline independent of pricing changes.
*Design guardrail:* compare **trip volume** and **base fare** only — these are clean YoY signals. Do NOT compare `total_amount` as a headline finding: 2025 trips carry `cbd_congestion_fee` that 2024 trips cannot, so any total-cost gap is largely the known pricing change, not organic demand. Treat that gap as expected, not discovered.

---

## Dataset

**Source:** NYC TLC Trip Record Data — Yellow Taxi
**Format:** Parquet, published monthly (~2-month lag)
**Download pattern:** `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
**Zone lookup:** TLC Taxi Zone lookup table (maps `LocationID` → zone → borough)

**Months selected:**

| Month     | Serves      | Status                          |
|-----------|-------------|---------------------------------|
| `2024-11` | Q3 + Q4     | core                            |
| `2024-12` | Q3 + Q4     | core                            |
| `2025-01` | Q3          | core                            |
| `2025-02` | Q3          | core                            |
| `2025-11` | Q4          | conditional on quota headroom   |
| `2025-12` | Q4          | conditional on quota headroom   |

> The 2024-11 and 2024-12 files do double duty (Q3 before-period *and* Q4 prior-year), which is efficient.
> The two 2025 months exist **only** to serve Q4. **Do not ingest them until Phase 3 has run on one month and quota burn is measured.** If quota is tight, drop Q4 rather than starve Q3.
> Rule that cannot break: Q3 requires data on **both sides of January 2025**; Q4 requires **matching Nov–Dec months one year apart**.

**Known data-quality issues (to be verified in profiling):** null passenger counts, negative/zero fares, impossible timestamps, zero-distance trips, schema drift across months (`cbd_congestion_fee` present 2025+ only).

---

## Architecture

```
TLC Parquet (source)
      │  ingest untouched + metadata
      ▼
┌─────────────┐   raw, immutable, replayable
│   BRONZE    │   (exact copy + ingestion_timestamp, source_file)
└─────────────┘
      │  schema enforcement, dedup, null handling,
      │  type standardization, schema-drift reconciliation
      ▼
┌─────────────┐   clean, conformed, idempotent (Delta MERGE)
│   SILVER    │
└─────────────┘
      │  business aggregations, zone/borough joins
      ▼
┌─────────────┐   analytics-ready tables answering Q1–Q4
│    GOLD     │
└─────────────┘
```

Quality gates run **between** layers (Bronze→Silver, Silver→Gold). The pipeline fails loudly on bad data rather than passing garbage downstream.

---

## Repository Structure

```
/notebooks
    01_bronze_ingest        # raw → Bronze Delta
    02_silver_clean         # Bronze → Silver (clean/conform)
    03_gold_aggregate       # Silver → Gold (business logic)
/config
    paths                   # source URLs, table locations
    schema                  # explicit schema definitions
/tests
    quality_checks          # Great Expectations suites
/docs
    decisions               # architecture decision notes
README.md                   # this file
databricks.yml              # (Phase 9) Asset Bundle for deployment
```

---

## Language Strategy

| Layer / Task            | Language          | Rationale                                             |
|-------------------------|-------------------|-------------------------------------------------------|
| Bronze ingest           | PySpark           | programmatic file handling + metadata                 |
| Silver clean/conform    | PySpark + Spark SQL | UDFs/type coercion (PySpark), filters/dedup (SQL)   |
| Gold aggregation        | Spark SQL         | aggregations are what SQL is built for                |
| Orchestration           | Databricks Jobs   | native, included in Free Edition                      |
| Quality gates           | Python + PySpark  | Great Expectations against Spark DataFrames           |
| Infrastructure / CI-CD  | HCL + YAML        | Terraform + GitHub Actions                            |

Target mix across Bronze→Gold: ~60% SQL / ~40% PySpark.

---

## Build Phases

1. **Scope** — lock the 3 business questions and month selection *(this document)*.
2. **Environment** — Databricks Free Edition, GitHub repo integration.
3. **Bronze** — ingest raw Parquet + metadata. Test quota on 1 month before scaling to 4.
4. **Profile** — nulls, duplicates, out-of-range values. Document defects as evidence.
5. **Silver** — clean/conform, reconcile schema drift, idempotent Delta MERGE.
6. **Gold** — aggregate to answer Q1–Q4.
7. **Orchestrate** — Databricks Jobs DAG with retries + failure alerts.
8. **Quality gates** — Great Expectations checks between layers; fail loud.
9. **Version & automate** — convert to Databricks Asset Bundle; Terraform IaC; GitHub Actions CI.
10. **Document decisions** — finalize the Architecture Decisions section below.

---

## Architecture Decisions (fill in as you build — this is the interview material)

- **Why medallion?** _[your reasoning]_
- **Partitioning strategy and why:** _[e.g., partition by pickup month]_
- **Idempotency:** _[why Delta MERGE over overwrite; behavior on Jobs retry]_
- **Schema drift handling:** _[how Silver reconciles pre-2025 vs 2025+ schemas]_
- **Late / duplicate data:** _[what happens if a file arrives late or twice]_
- **Scaling to production:** _[how this design would run at full multi-year volume]_

---

## Constraints & Notes

- **Free Edition is serverless and quota-limited.** If quota is exceeded, compute shuts down for the rest of the day. Dataset size is capped to fit & validated by testing ingestion on one month first.
- **Cash tips are unrecorded** — tipping analysis (Q2) covers card payments only.
- **Schema drift is intentional in scope**, not an accident — handling it is part of the demonstrated skill.