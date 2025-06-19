# 📊 SCD2 Merge with Booking Fact Aggregation — Databricks Pipeline

This project implements a robust data pipeline on **Databricks** for:
- **Aggregating Booking Data**
- **Performing SCD Type 2 (Slowly Changing Dimension)** merge logic on Customer data
- **Validating Data Quality** using [PyDeequ](https://github.com/awslabs/deequ)
- **Scheduling using Databricks Workflows**

---

## 🧠 Problem Statement

To ensure clean, timely, and historical tracking of customer data and booking transactions, this pipeline:
- Performs data quality checks
- Aggregates transactional booking data
- Maintains historical changes in customer dimension using **SCD Type 2**

---

## 🛠 Tech Stack

- **Apache Spark (PySpark)**
- **Databricks Delta Lake**
- **Databricks Workflows**
- **PyDeequ for Data Quality Checks**
- **CSV ingestion from Volumes**
- **SCD Type 2 Merge using DeltaTable APIs**

---


---

## Architechture
![Pipeline Architecture](https://raw.githubusercontent.com/Faiz-Ali-Faizi/SCD2-Merge/main/scd2_architecture_diagram.svg)









## ⚙️ Pipeline Workflow

1. **Parameterization**
   - Takes `arrival_date` as input via `dbutils.widgets`.
   - Dynamically constructs file paths from `/Volumes/incremental_load/...`.

2. **Data Ingestion**
   - Reads two CSVs:
     - Booking Data: `bookings_<date>.csv`
     - Customer Data: `customers_<date>.csv`

3. **Data Quality Validation** (via PyDeequ)
   - Checks for:
     - Completeness
     - Uniqueness of primary keys
     - Non-negativity
     - Null validations

4. **Transformation**
   - Adds `ingestion_time`
   - Joins booking & customer data
   - Computes `total_cost` = `amount - discount`
   - Aggregates data by `booking_type` and `customer_id`

5. **Fact Table (Booking) Aggregation**
   - Merges new aggregates into `booking_fact` Delta table
   - Appends or overwrites intelligently

6. **SCD Type 2 on Customer Dimension**
   - Updates `valid_to` on matched rows
   - Appends new rows with updated data

---

## 🗃 Delta Tables Used

| Table Name                            | Description                          |
|--------------------------------------|--------------------------------------|
| `incremental_load.default.booking_fact` | Aggregated transactional fact table |
| `incremental_load.default.customer_dim` | Customer dimension with SCD2 logic  |

---

## 🧪 Sample Data Quality Checks

### Booking Data
- Must have non-negative amount, quantity, discount
- Must have unique `booking_id`
- Must not have null `customer_id` or `amount`

### Customer Data
- Must have unique `customer_id`
- Must not have null `customer_name`, `email`, or `address`

---

## 🧩 Workflow JSON Configuration

The Databricks Workflow runs the notebook with an arrival date as input.

```json
{
  "name": "scd2_merge_flow",
  "tasks": [
    {
      "task_key": "booking_fact_job",
      "notebook_path": "/Workspace/Users/shashankde219@gmail.com/scd2_merge/Travel_Booking_SCD2_Merge",
      "base_parameters": {
        "arrival_date": "2024-07-26"
      },
      "existing_cluster_id": "1220-164149-p159zqx1"
    }
  ]
}
```
## 📅 Schedule & Orchestration
The workflow is designed to run daily or on-demand by uploading files with a matching arrival_date.

