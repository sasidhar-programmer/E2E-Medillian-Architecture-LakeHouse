# 🚀 Enterprise Medallion Lakehouse Architecture

<img width="1015" height="628" alt="image" src="https://github.com/user-attachments/assets/3e1ce6d6-7ea7-463c-bdb1-d27cdfdcd93b" />


<img width="1560" height="183" alt="image" src="https://github.com/user-attachments/assets/764550b1-e98e-4c0d-aaa2-bc5d3ad6ca97" />

*Above: The Medallion Architecture DAG orchestrated via Databricks Workflows, demonstrating the fan-out (Dimensions) and fan-in (Facts) dependencies.*

## 📌 Project Overview
An end-to-end, highly scalable Data Engineering batch processing pipeline built on **Azure Databricks** and **Delta Lake**. This project orchestrates complex raw JSON API payloads into a fully governed **Unity Catalog** Medallion Architecture (Bronze, Silver, Gold). 

The core focus of this project is building highly resilient, fault-tolerant pipelines that gracefully handle upstream API schema mutations, optimize cloud compute costs, and enforce strict enterprise data governance.

## 🛠️ Tech Stack
* **Compute & Orchestration:** Azure Databricks, Databricks Workflows (DAG)
* **Storage & Format:** Azure Data Lake Storage Gen2 (ADLS), Delta Lake
* **Governance:** Databricks Unity Catalog
* **Language:** PySpark, Spark SQL
* **BI / Analytics:** Databricks AI/BI Genie Spaces

## 🏗️ Architecture Design

### 🥉 Bronze Layer (Raw to Delta)
* Ingests incremental JSON payloads from raw ADLS containers.
* Utilizes `mergeSchema` to handle safe, expected schema evolution.
* **Dynamic Metaprogramming:** Automatically detects and upgrades column data types on the fly to protect downstream tables.

### 🥈 Silver Layer (Cleansed & Conformed)
* Implements strict Data Quality (DQ) checks.
* Routes invalid records (e.g., Null keys, Duplicates) to a centralized Unity Catalog Exception/Quarantine table.
* Performs complex dimensional broadcast joins and exchange rate (FX) calculations natively in PySpark.
* Utilizes `MERGE INTO` (Upserts) to maintain Slowly Changing Dimensions (SCD) and Fact tables.

### 🥇 Gold Layer (Aggregated Business Logic)
* Aggregates `prx.prx_silver.f_salesline` and dimensions into a `prx.prx_gold.dailysalesdetail` reporting table.
* Implements cascading fallback logic for business dates (Realized -> Booked -> Planned -> Audit).
* Computes complex KPIs (Gross Margin, Backlog Quantities).

---

## 🌩️ Engineering Challenges & Architectural Solutions

In a true production environment, source systems and APIs are rarely perfectly clean. This pipeline was architected to survive real-world data anomalies.

### Challenge 1: The Incremental JSON Schema Drift 
**The Problem:** The upstream API provided dynamic JSON payloads. On Day 1, financial columns (e.g., `AmountDiscount`) contained only whole numbers, causing `inferSchema` to lock the Delta column as `LongType`. On Day 2, the API sent decimal values. Appending this data caused Delta Lake to throw a `[DELTA_FAILED_TO_MERGE_FIELDS]` exception to prevent precision loss. Similarly, alphanumeric Identifiers (e.g., `DocumentNumber`) were initially inferred as integers, crashing the pipeline when string-based IDs arrived later.

**The Solution:** Implemented **"The Universal Bronze Shield"**. Instead of hardcoding schemas, I utilized Python metaprogramming to dynamically scan the `dtypes` of the incoming DataFrame before writing to Delta.
* **Financial Rule:** Any column containing keywords like `amount`, `discount`, or `price` inferred as an integer is dynamically cast to `DoubleType`.
* **Identifier Rule:** Any column containing `id`, `number`, or `code` is dynamically cast to `StringType`.
This decoupled the pipeline from API unreliability and mathematically guaranteed type safety for the Silver layer.

### Challenge 2: "Ghosts" in External Tables
**The Problem:** During the schema drift resolution, dropping the corrupted Bronze table using `DROP TABLE` in Unity Catalog did not resolve the merge conflicts. Databricks kept referencing the old `LongType` schema. 

**The Solution:** Recognized that dropping an **External Table** in Unity Catalog only deletes the metastore pointer, not the underlying `_delta_log` or Parquet files in Azure Data Lake. I executed a physical annihilation of the corrupted ADLS directory via `dbutils.fs.rm`, followed by a full historical backfill reading `*/*/*/*.json` to lay down a pristine, shielded schema foundation.

### Challenge 3: Cluster Compute & DAG Optimization
**The Problem:** The Silver sales line transformation required heavy cross-currency exchange rate calculations, backlog logic, and duplicate detection utilizing Window functions. Running multiple actions (e.g., separating valid records from quarantine records) caused Spark to recalculate the entire DAG multiple times, driving up cloud costs.

**The Solution:** 1. **DAG Protection:** Injected `.cache()` immediately after the complex transformations but before the valid/error routing. This forced Spark to compute the heavy logic exactly once per batch.
2. **Auto-Optimization:** Enabled `spark.databricks.delta.optimizeWrite.enabled` and `autoCompact` to natively kill the small-file problem during the high-volume `MERGE INTO` operations, drastically reducing storage transaction costs.

---

## 💡 Future Scalability Notes
For this portfolio demonstration, the DAG dependencies were explicitly mapped in the Databricks Workflow UI to showcase the fanning-out and fanning-in of the Medallion structure visually. In an enterprise environment orchestrating 50+ dimensions and facts, I would migrate this to a Metadata-Driven Framework using **Azure Data Factory (ADF) ForEach loops** or **Databricks Asset Bundles (DABs)** for automated CI/CD deployment, eliminating UI overhead and parameterizing generic transformation scripts.
