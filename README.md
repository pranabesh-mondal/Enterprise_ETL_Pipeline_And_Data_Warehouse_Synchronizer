# Enterprise ETL Pipeline & Data Warehouse Synchronizer

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache_Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Database](https://img.shields.io/badge/Warehouse-PostgreSQL_/_Snowflake-336791?logo=postgresql&logoColor=white)
![AWS](https://img.shields.io/badge/Data_Lake-AWS_S3-FF9900?logo=amazonwebservices&logoColor=white)
![Processing](https://img.shields.io/badge/Processing-Polars_/_Pandas-CD792C?logo=pandas&logoColor=white)
![Docker](https://img.shields.io/badge/Deployment-Docker-2496ED?logo=docker&logoColor=white)

> A resilient, automated Data Engineering pipeline that extracts business data from multiple disparate third-party APIs (**Salesforce**, **Stripe**, **Zendesk**), transforms and cleans it to fit internal schemas, and loads it securely into a centralized **Data Warehouse** — establishing a **single source of truth** for BI & analytics, refreshed on a reliable **24-hour cycle**.

---

##  Table of Contents

1. [Project Information](#1-project-information)
2. [Key Features](#2-key-features)
3. [Architecture](#3-architecture)
4. [Tech Stack](#4-tech-stack)
5. [Development Timeline (4 Weeks)](#5-development-timeline-4-weeks)
6. [Planned Repository Structure](#6-planned-repository-structure)
7. [Getting Started](#7-getting-started)
8. [Resilience and Error Handling](#8-resilience-and-error-handling)
9. [Testing Strategy](#9-testing-strategy)
10. [Deployment and CI/CD](#10-deployment-and-cicd)
11. [Expected Impact](#11-expected-impact)
12. [Project Status and Roadmap](#12-project-status-and-roadmap)
13. [Contributing and License](#13-contributing-and-license)

---

## 1. Project Information

A resilient, automated Data Engineering pipeline designed to extract business data from multiple disparate third-party APIs (e.g., **Salesforce**, **Stripe**, **Zendesk**), transform and clean the data to fit internal schemas, and load it securely into a centralized **Data Warehouse**.

The system handles **rate limiting, pagination, incremental loads, and robust error logging** out of the box, so business decision-makers always have access to up-to-date, structured data.

| Attribute | Detail |
|---|---|
| **Domain** | Data Engineering / ELT Automation |
| **Language** | Python 3.11+ |
| **Data Processing** | Pandas, Polars (high-performance transformations) |
| **Orchestration** | Apache Airflow (daily scheduled DAGs) |
| **Database / Storage** | SQLAlchemy, PostgreSQL / Snowflake, AWS S3 (raw data lake) |
| **Networking / APIs** | Requests, Tenacity (retry logic), Pydantic (data validation) |
| **Status** | Planning — Week 1 kickoff |

---

## 2. Key Features

-  **Multi-source extraction** — pluggable connectors for Salesforce, Stripe, and Zendesk REST APIs.
-  **Cursor-based pagination** — reliable traversal of large datasets without missing or duplicating records.
-  **Rate-limit handling** — Tenacity-powered exponential backoff with jitter, honoring `Retry-After` headers.
-  **Incremental loads** — high-water-mark tracking so only new/updated records are pulled each run.
-  **Raw data lake** — immutable raw JSON landing in AWS S3 for auditability and replay.
-  **High-performance transforms** — Polars (primary) with Pandas compatibility for cleaning and standardization.
-  **Strict validation** — Pydantic models enforce data types and a unified internal schema.
-  **Idempotent upserts** — update/insert logic prevents duplicate warehouse records.
-  **Airflow orchestration** — daily scheduled DAGs with task-level retries.
-  **Failure alerting** — Slack / Email notifications whenever a pipeline stage fails.
-  **Robust logging** — structured error logging at every stage of the pipeline.
-  **Production-ready** — Dockerized application with CI/CD pipelines.

---

## 3. Architecture

```
┌──────────────────────────────  SOURCES  ──────────────────────────────┐
│    Salesforce API        Stripe API        Zendesk API                │
│  (Accounts, Opps)    (Payments, Invoices)  (Tickets, Customers)       │
└─────────┬──────────────────────┬──────────────────────┬───────────────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌───────────────────────────────────────────────────────────────────────┐
│   EXTRACT — Python + Requests + Tenacity                             │
│     • Cursor-based pagination      • Rate-limit aware backoff         │
│     • Incremental high-water-mark pulls                               │
└──────────────────────────────┬────────────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│   RAW DATA LAKE — AWS S3 (immutable JSON landing zone)               │
└──────────────────────────────┬────────────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│   TRANSFORM — Polars / Pandas + Pydantic validation                  │
│     • Null handling   • Date/currency standardization                 │
│     • Schema mapping → unified internal model                         │
└──────────────────────────────┬────────────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│   LOAD — SQLAlchemy → PostgreSQL / Snowflake                         │
│     • Upsert (update/insert)       • No duplicate records             │
└──────────────────────────────┬────────────────────────────────────────┘
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│   SERVE — Single source of truth for BI & Analytics teams            │
└───────────────────────────────────────────────────────────────────────┘

          Orchestrated by Apache Airflow DAGs (24-hour cycle)
          Slack / Email alerting on pipeline failures
          Structured error logging at every stage
```

### Lifecycle of One Pipeline Run

1. **Trigger** — the Airflow DAG fires on its daily schedule (or a manual run).
2. **Extract** — connectors pull only records newer than the last successful high-water mark, walking cursor pagination while respecting API rate limits; raw JSON is written to S3.
3. **Transform** — Polars/Pandas clean and standardize the data; Pydantic validates every record against the unified schema.
4. **Load** — SQLAlchemy upserts clean records into the warehouse; existing rows are updated, new rows inserted, duplicates impossible.
5. **Finalize** — watermarks and run metadata are updated; any failure triggers Airflow retries plus Slack/Email alerts with full error context.

---

## 4. Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Language** | Python 3.11+ | Core pipeline code |
| **Data Processing** | Pandas, Polars | High-performance cleaning & transformation |
| **Validation** | Pydantic | Data models, schema enforcement, typed settings |
| **Extraction** | Requests, Tenacity | API calls, retry & rate-limit logic |
| **Raw Storage** | AWS S3 (boto3) | Intermediate data lake for raw JSON |
| **Warehouse** | SQLAlchemy, PostgreSQL / Snowflake | Central Data Warehouse target |
| **Orchestration** | Apache Airflow | Daily scheduled DAGs, task retries |
| **Testing** | Pytest | Unit & integration tests |
| **Packaging** | Docker, Docker Compose | Containerized deployment |
| **CI/CD** | GitHub Actions | Lint, test, build automation |
| **Alerting** | Slack Webhooks / SMTP Email | Pipeline failure notifications |

---

## 5. Development Timeline (4 Weeks)

###  Week 1 — API Integration & Data Extraction

| Days | Task |
|---|---|
| 1–2 | Define data models using **Pydantic** and configure environment variables for secure API key management. |
| 3–5 | Build extraction scripts for **Stripe** and **Salesforce** APIs, implementing **cursor-based pagination**. |
| 6–7 | Implement **rate-limit handling** and write raw extracted JSON data to an **AWS S3** bucket. |

###  Week 2 — Data Transformation & Validation

| Days | Task |
|---|---|
| 1–3 | Develop **Polars/Pandas** scripts to clean raw data, handle null values, and standardize date/currency formats. |
| 4–6 | Write transformation logic to map disparate API fields into a **unified schema**. |
| 7 | Implement unit tests using **Pytest** to validate data types and transformation logic. |

###  Week 3 — Data Loading & Database Sync

| Days | Task |
|---|---|
| 1–3 | Configure **SQLAlchemy** to connect to the target Data Warehouse (**PostgreSQL/Snowflake**). |
| 4–6 | Implement **upsert** logic (update/insert) to handle incremental data loads without duplicating records. |
| 7 | Perform **end-to-end testing** of the Extraction, Transformation, and Loading phases. |

###  Week 4 — Orchestration, Monitoring & Deployment

| Days | Task |
|---|---|
| 1–3 | Wrap the ETL scripts into **Apache Airflow DAGs** (Directed Acyclic Graphs) for daily scheduled execution. |
| 4–5 | Implement **Slack/Email alerting** mechanisms for pipeline failures. |
| 6–7 | **Containerize** the application with **Docker** and finalize **CI/CD** pipelines. |

---

```
.
├── dags/                            # Airflow DAGs (Week 4)
├── src/
│   ├── config/
│   │   └── settings.py              # [x] Pydantic Settings & env management
│   ├── models/
│   │   ├── base.py                  # [x] Base schemas, Source enum, validate_batch
│   │   ├── stripe.py                # [x] Stripe API models
│   │   ├── salesforce.py            # [x] Salesforce API models + SOQL queries
│   │   └── unified.py               # [x] Canonical warehouse schema (Week 2)
│   ├── extract/
│   │   ├── base.py                  # [x] RateLimitedSession (retry/backoff) + BaseExtractor
│   │   ├── stripe.py                # [x] Stripe extractor (cursor pagination)
│   │   └── salesforce.py            # [x] Salesforce extractor (OAuth2 + nextRecordsUrl)
│   ├── transform/
│   │   ├── clean.py                 # [x] Null/date/currency/amount standardization
│   │   ├── clean_frames.py          # [x] Polars + Pandas frame cleaning & dedupe
│   │   ├── mappings.py              # [x] Declarative source -> unified field maps
│   │   ├── pipeline.py              # [x] Map + validate + dedupe (engine selectable)
│   │   └── runner.py                # [x] Raw part -> processed zone (JSON/Parquet)
│   ├── storage/
│   │   └── s3_lake.py               # [x] S3 raw landing (+ local dev fallback)
│   ├── load/                        # [ ] SQLAlchemy warehouse writers (Week 3)
│   ├── alerting/                    # [ ] Slack / Email failure alerts (Week 4)
│   ├── utils/
│   │   └── logging_config.py        # [x] Structured logging
│   └── main.py                      # [x] CLI: extract / transform / all stages
|
├── sql/                             # [ ] Warehouse DDL (Week 3)
├── .github/workflows/               # [ ] CI/CD (Week 4)
├── docker/                          # [ ] Dockerfiles (Week 4)
├── pyproject.toml                   # [x] pytest / ruff / black / mypy config
├── requirements.txt                 # [x] Dependencies
├── .env.example                     # [x] Environment template
└── README.md
```

---

## 7. Getting Started

>  **Note:** The workflow below is the target setup — commands are wired up as components land across Weeks 1–4 of the timeline.

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- PostgreSQL or Snowflake warehouse access
- AWS account with an S3 bucket (raw data lake)
- API credentials for Salesforce / Stripe / Zendesk

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/pranabesh-mondal/Enterprise_ETL_Pipeline_And_Data_Warehouse_Synchronizer.git
cd Enterprise_ETL_Pipeline_And_Data_Warehouse_Synchronizer

# 2. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
copy .env.example .env        # then fill in your credentials

# 5. Run the ETL pipeline locally (one-shot)
python -m src.main

# 6. Run the test suite
pytest -v

# 7. Launch the Airflow stack (daily schedule) via Docker Compose
docker compose up -d
```

### Environment Variables (via `.env`)

| Variable | Description |
|---|---|
| `STRIPE_API_KEY` | Stripe secret API key |
| `SALESFORCE_USERNAME` / `SALESFORCE_PASSWORD` / `SALESFORCE_SECURITY_TOKEN` | Salesforce integration-user credentials |
| `ZENDESK_SUBDOMAIN` / `ZENDESK_EMAIL` / `ZENDESK_API_TOKEN` | Zendesk API credentials |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_S3_BUCKET` / `AWS_REGION` | S3 raw data lake access |
| `DATABASE_URL` | SQLAlchemy DSN for PostgreSQL (e.g., `postgresql+psycopg2://user:pass@host:5432/warehouse`) |
| `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` / `SNOWFLAKE_WAREHOUSE` / `SNOWFLAKE_DATABASE` / `SNOWFLAKE_SCHEMA` | Snowflake credentials (alternative target) |
| `SLACK_WEBHOOK_URL` | Slack webhook for pipeline failure alerts |
| `ALERT_EMAIL` | Email recipient for failure alerts |

>  **Security:** `.env` is gitignored — never commit real credentials. In production, use AWS Secrets Manager or Airflow Connections/Variables instead.

---

## 8. Resilience and Error Handling

| Concern | Strategy |
|---|---|
| API rate limits | Per-source throttling + Tenacity exponential backoff with jitter, honoring `Retry-After` headers |
| Large datasets | Cursor-based pagination implemented per connector |
| Transient network/API errors | Automatic retries (Tenacity) with capped attempts and backoff |
| Duplicate records | Idempotent upserts keyed on natural/primary keys + incremental high-water marks |
| Partial/bad records | Pydantic validation with per-batch error isolation — invalid records are quarantined and logged without blocking the batch |
| Auditability | Immutable raw JSON in S3 + structured logs + run metadata for every execution |
| Full pipeline failure | Airflow task retries + Slack/Email alerts with failure context and stage attribution |

---

## 9. Testing Strategy

- **Unit tests (Pytest)** — data types, Pydantic models, transformation logic, null handling, date/currency standardization. *(Week 2, Day 7)*
- **Integration tests** — mocked third-party APIs covering pagination and rate-limit scenarios, plus test-database upsert checks. *(Weeks 2–3)*
- **End-to-end tests** — a full Extract → Transform → Load dry run against the target stack. *(Week 3, Day 7)*

---

## 10. Deployment and CI/CD

-  **Docker image** for the ETL application; **Docker Compose** for the local stack (Airflow + PostgreSQL).
-  **GitHub Actions** pipeline: lint/format checks → Pytest suite → Docker build on every push/PR.
-  **Production execution** via the Airflow daily-scheduled DAG with automatic retries.

---

## 11. Expected Impact

-  Establishes a **single source of truth** for the organization's BI and analytics teams.
-  Eliminates **hundreds of hours** of manual data extraction.
-  **Reduces human error** through automated validation and idempotent loads.
-  Ensures decision-makers have access to **up-to-date, structured data** within a reliable **24-hour cycle**.

---

## 12. Project Status and Roadmap

- [x] **Week 1** — API Integration & Data Extraction
- [ ] **Week 2** — Data Transformation & Validation
- [ ] **Week 3** — Data Loading & Database Sync
- [ ] **Week 4** — Orchestration, Monitoring & Deployment

>  Status is updated here as deliverables land, following the [Development Timeline](#5-development-timeline-4-weeks).

---

## 13. Contributing and License

### Contributing

1. Fork the repository and create a feature branch (`git checkout -b feature/my-feature`).
2. Commit your changes (`git commit -m "Add: my feature"`).
3. Push to the branch (`git push origin feature/my-feature`).
4. Open a Pull Request — CI must pass before merge.

### License

License to be decided (MIT / Apache 2.0 / proprietary).
