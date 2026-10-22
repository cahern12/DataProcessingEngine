# Enterprise Data Processing Platform
## POC Validation Plan and Platform Requirements

## Platform Definition

> **The Enterprise Data Processing Platform accepts raw enterprise data through standardized interfaces, applies governed and repeatable processing, and publishes trusted curated data through stable contracts while abstracting ingestion, storage, compute, and transformation implementation from downstream consumers.**

The platform should behave as a **black box to consumers, but not to operators**. Consumers should not need to know how data is ingested, stored, transformed, or which engine executes the workload. Operators must retain visibility into lineage, quality, execution state, failures, performance, reliability, and cost.

---

# 1. POC Objective

The POC will determine whether an Azure-based processing platform can provide a reusable enterprise capability with:

- trustworthy outputs,
- repeatable and recoverable execution,
- predictable performance and scale,
- measurable workload economics,
- open and portable data structures,
- abstraction from downstream consumers.

The POC is not intended to prove the complete future-state platform. It is intended to produce enough evidence to make a production architecture decision.

### Processing pattern to validate

> **Representative Raw Sources → Standardized Ingestion → Bronze → Silver → Gold**

### Core question

> **Can we provide enterprise data processing as a reusable service with predictable performance, trustworthy outputs, operational reliability, and measurable economics while insulating consumers from the underlying processing implementation?**

### Required outcome

The POC must conclude with one of three recommendations:

- **Proceed:** sufficient technical, operational, and economic fit.
- **Proceed with Changes:** viable, but design or technology changes are required.
- **Do Not Proceed:** material constraints make the approach unsuitable.

Technical feasibility alone is not enough.

---

# 2. Architecture Hypotheses

| ID | Hypothesis |
|---|---|
| **H1** | An Azure lakehouse using open storage and table formats can process data from raw source through curated output without coupling the data to one processing engine. |
| **H2** | **dbt + Trino** can provide a viable primary path for representative SQL-centric transformations at acceptable performance, reliability, and cost. |
| **H3** | **Spark** may provide a better fit for workload classes where SQL-centric execution through Trino is insufficient, less resilient, or less economical. |
| **H4** | Trino and Spark can produce equivalent governed Iceberg outputs behind a common platform contract. |
| **H5** | Workload-level cost can be measured accurately enough to support future workload placement decisions. |

---

# 3. Seven Must-Have POC Gates

These are the non-negotiable POC requirements.

| Gate | Requirement | The POC Must Prove |
|---|---|---|
| **G1** | **End-to-End Processing** | Representative datasets can be ingested and processed reliably through **Bronze → Silver → Gold**. |
| **G2** | **Processing Abstraction** | Gold outputs remain consumable without dependency on ingestion tools, execution engines, cluster topology, or physical file layout. |
| **G3** | **Correctness and Trust** | Outputs can be validated through schema, reconciliation, duplicate, null/domain, and expected-result checks. |
| **G4** | **Repeatability and Recovery** | Workloads can be rerun consistently and recovered safely from representative failures. |
| **G5** | **Performance and Scale** | Runtime, throughput, utilization, and scaling behavior can be measured as data volume and compute change. |
| **G6** | **Transparent Economics** | The platform can calculate both **marginal workload cost** and **fully allocated platform cost**. |
| **G7** | **Technology Fit** | The POC produces enough evidence to recommend the simplest sustainable production processing architecture. |

Supporting capabilities include version-controlled transformations, execution metadata, lineage, open formats, standardized orchestration, reproducible benchmarks, source-to-target reconciliation, and failure testing.

---

# 4. POC Scope

## Representative source patterns

Use a small number of datasets that represent common enterprise patterns:

- **Enterprise application extracts:** ERP, HR, ITSM, finance, and other systems of record
- **Relational extracts:** SQL Server, Oracle, PostgreSQL, or equivalent
- **Files and bulk feeds:** CSV, JSON, XML, Parquet
- **APIs:** REST APIs, application exports, service integrations

The initial POC should remain **batch-first**. CDC and streaming should only be added if needed to validate a specific architectural risk.

## Explicit non-goals

The initial POC does not require:

- downstream production integrations,
- Snowflake integration,
- broad Kafka or streaming deployment,
- real-time CDC as a primary workload,
- enterprise-wide domain segmentation,
- enterprise self-service onboarding,
- advanced anomaly detection,
- production disaster recovery,
- AI/ML consumption patterns,
- production-scale multi-tenancy.

The POC should stay focused on processing architecture, correctness, reliability, performance, operability, and cost.

---

# 5. Benchmark and Economics Plan

Use a small, controlled workload set.

**Success thresholds for runtime, cost, recovery, and scale must be agreed before benchmark execution so results are evaluated against predefined criteria rather than interpreted after the fact.**

| Workload | Primary Purpose | Small | Medium | Large | Trino | Spark |
|---|---|---:|---:|---:|---:|---:|
| **Scan / Filter** | Read throughput, partition pruning, predicate pushdown | ✓ | ✓ | ✓ | ✓ | Optional |
| **Multi-Table Join** | Distributed joins, shuffles, memory, worker scaling | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Aggregation / Window** | Grouping, sorting, windows, compute intensity | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Bronze → Silver → Gold** | Full transformation lifecycle and writes | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Failure / Recovery** | Restart, replay, worker loss, safe recovery |  | ✓ | ✓ | ✓ | ✓ |

Small, Medium, and Large should be defined after representative datasets are selected.

### Every benchmark run should capture

- workload and dataset,
- input and output volume,
- transformation version,
- execution engine and version,
- worker configuration,
- runtime,
- CPU and memory utilization,
- bytes read and written,
- file count and size distribution,
- quality result,
- retry or failure events,
- operator actions and manual recovery steps,
- tuning effort and notable configuration changes,
- marginal execution cost,
- allocated platform overhead,
- fully allocated workload cost,
- cost per TB processed.

Equivalent runs should produce materially comparable outputs and telemetry.

## Current-state reference benchmark

Where practical, equivalent workloads should also be measured against the current processing approach to establish a reference point for runtime, cost, and operational complexity. No downstream integration is required for this comparison.

## Cost model

### Marginal Workload Cost

Incremental cost to execute a workload, including compute, storage reads and writes, temporary processing storage, and applicable network cost.

### Fully Allocated Platform Cost

Marginal workload cost plus allocated shared services such as orchestration, catalog services, persistent cluster capacity, monitoring, container infrastructure, and baseline storage.

### Cost attribution requirement

POC infrastructure must support defensible cost attribution through dedicated resource groups, tagging, dedicated node pools, or equivalent isolation. Shared infrastructure should not prevent workload-level cost measurement.

### Required economic outputs

- cost per workload run,
- cost per TB processed,
- cost by execution engine,
- standing platform cost,
- cost as volume scales,
- cost as worker count scales,
- cost/performance trade-offs.

---

# 6. POC Technology Architecture and Processing Paths

The POC should validate technology roles, not assume every component becomes a permanent production dependency.

| Capability | Initial POC Choice | Role |
|---|---|---|
| Cloud | **Microsoft Azure** | Platform boundary |
| Storage | **ADLS Gen2** | Durable object storage |
| File format | **Parquet** | Open columnar storage |
| Table format | **Apache Iceberg** | Table state, snapshots, schema evolution |
| Catalog | **Iceberg REST-compatible catalog** | Portable table metadata interface |
| Orchestration | **Apache Airflow** | Scheduling, dependencies, retries, benchmark coordination |
| Transformation framework | **dbt** | SQL models, dependencies, tests, documentation, lineage |
| SQL execution engine | **Trino** | Distributed SQL execution and common SQL access |
| Comparative engine | **Apache Spark** | General-purpose distributed processing for selected workloads |
| Container platform | **AKS** | Runtime environment for processing services |

### Technology roles

- **dbt** defines and manages SQL transformation logic, dependencies, tests, documentation, and lineage. It does not provide distributed compute.
- **Trino** executes distributed SQL, reads and writes Iceberg tables, and provides a common SQL access layer over the lakehouse.
- **Spark** provides general-purpose distributed processing for workload classes where it may offer better performance, resilience, or flexibility.

The primary comparison is **Trino vs. Spark as execution engines**, with dbt used as a transformation framework where appropriate.

## Path A: SQL-Centric Processing

> **Airflow → dbt → Trino → Iceberg / ADLS**

Test scans, joins, aggregations, windows, incremental processing, Bronze → Silver → Gold writes, concurrency, failure behavior, and cost.

## Path B: General-Purpose Processing

> **Airflow → Spark → Iceberg / ADLS**

Use Spark selectively for workloads that stress Path A, such as very large joins, heavy shuffles, complex multi-stage processing, long-running jobs, and failure recovery.

## Multi-engine principle

The POC should not assume both engines are required in production. A second engine should only be adopted if benchmarks demonstrate a meaningful advantage that justifies the added operational complexity.

The preferred outcome is the **simplest architecture that satisfies the workload requirements**.

---

# 7. Platform Contracts

The black-box model should be defined through three contracts.

| Contract | Defines |
|---|---|
| **Input Contract** | Source identity, schema, ingestion method, expected volume/frequency, classification, and raw retention. |
| **Processing Contract** | Transformation version, dependencies, execution engine, resource profile, quality rules, recovery behavior, and telemetry. This remains internal to the platform. |
| **Output Contract** | Dataset name, schema, semantics, owner, version, freshness, quality expectation, and logical access interface. |

The output contract should remain stable even if the execution engine changes.

```text
SOURCE
  |
  v
Input Contract
  |
  v
Enterprise Data Processing Platform
  Airflow / dbt
  Trino / Spark
  Iceberg / ADLS
  |
  v
Output Contract
  |
  v
CONSUMER
```

The enterprise processing service, not the individual execution engine, should become the architectural contract.

---

# 8. Deliverables and Decision Framework

## Required deliverables

1. Working Azure POC environment
2. Representative Bronze, Silver, and Gold datasets
3. Version-controlled pipeline and transformation definitions
4. Data-quality and reconciliation results
5. Execution and lineage metadata
6. Performance and cost benchmark results
7. Trino vs. Spark comparison for selected workloads
8. Failure and recovery findings
9. Operational complexity assessment
10. Architecture risks and findings
11. Production architecture recommendation

## Final decision criteria

| Area | Evaluate |
|---|---|
| **Technical Fit** | Correctness, scalability, portability, engine interoperability |
| **Operational Fit** | Observability, recovery, automation, tuning effort, operator effort, manageable complexity |
| **Economic Fit** | Workload-level cost, standing platform cost, predictable scaling economics |
| **Architectural Fit** | Consumer abstraction, open interfaces, minimal engine dependency, sustainable simplicity |

## Working architecture decision to validate

> **An Azure data-processing platform using ADLS Gen2, Apache Iceberg, a portable Iceberg catalog, Airflow orchestration, dbt-managed transformation logic, and Trino as the initial SQL execution engine can provide a reusable, observable, and economically measurable enterprise processing capability. Spark should be benchmarked selectively where a general-purpose distributed engine may provide materially better fit.**

The POC is not intended to prove that Trino, Spark, or any other engine should process every workload. The objective is to determine the **simplest sustainable processing architecture that satisfies enterprise requirements while preserving engine flexibility behind a stable platform contract**.

---

# 9. Broader Enterprise Platform Requirements

These requirements describe the longer-term platform direction. They should inform the POC, but they do not all need to be proven during initial validation.

| ID | Business Requirement |
|---|---|
| **BR-01** | Decouple data processing from consumption. |
| **BR-02** | Provide stable data contracts for curated data products. |
| **BR-03** | Standardize onboarding across common source patterns. |
| **BR-04** | Standardize the raw → standardized → curated processing lifecycle. |
| **BR-05** | Deliver trusted data through repeatable quality and reconciliation controls. |
| **BR-06** | Provide end-to-end lineage and traceability. |
| **BR-07** | Support measurable freshness, processing, availability, and recovery service levels. |
| **BR-08** | Provide transparent workload-level processing economics. |
| **BR-09** | Scale processing independently from downstream consumers. |
| **BR-10** | Support reliable retry, replay, restart, and recovery. |
| **BR-11** | Enforce enterprise security, privacy, retention, and audit requirements. |
| **BR-12** | Preserve technology flexibility through open formats and interfaces. |
| **BR-13** | Automate routine ingestion, transformation, validation, maintenance, and recovery. |
| **BR-14** | Reduce time to deliver new governed data products. |
| **BR-15** | Support future CDC, streaming, additional engines, and consumption patterns without redesigning the core platform. |

---

# 10. Architectural Principle

> **We are not validating Trino as the platform. We are validating an open enterprise data-processing architecture in which processing engines can be selected based on workload fit while consumers remain insulated from those implementation decisions.**

The durable asset is the **processing service, open data foundation, and stable data contract**, not any individual execution engine.
