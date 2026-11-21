# E-Commerce Data Engineering & Analytics Platform

## Source Dataset

This project uses the **Brazilian E-Commerce Public Dataset by Olist**, available on Kaggle.

📂 **Download the dataset:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The raw CSV files are not included in this repository due to their size. To reproduce the project, download the datasets and place them in the `data/raw/` directory.

The marketing-qualified leads and closed-deals datasets may need to be downloaded separately.

**Version 1 — Python / pandas ETL and analytical modelling**

An end-to-end e-commerce data engineering case study that profiles, cleans, validates and integrates heterogeneous marketplace data into a reproducible analytical dataset. The solution uses **Python 3.12, pandas, NumPy and PyArrow**, with Jupyter notebooks for exploration and a Python script for repeatable execution.

## Business context and problem

An online marketplace's data spans customers, sellers, products, orders, payments, reviews, marketing-qualified leads and related activities. The business wants a **trusted analytical representation of the journey from marketing activity through sellers and marketplace operations**, while retaining the detail needed to investigate customer behaviour, products, transactions, payments, reviews and geography.

The 11 source datasets have different natural grains and relationships. Directly joining them can multiply rows, double-count financial measures and produce misleading insights. The engineering challenge is to understand the source entities, preserve the correct grain, manage data-quality exceptions and create a validated analytical foundation.

**Scope clarification:** Version 1 delivers a transaction-focused canonical dataset at **order-item grain**. Marketing-qualified leads and closed deals are profiled as a separate domain; a fully validated lead → closed deal → seller → order conversion funnel is **not** included in the canonical output. This is a documented limitation, not a claimed deliverable.

## Results at a glance

| Validated outcome | Result |
|---|---:|
| Source datasets | 11 CSV files |
| Canonical grain | `order_id` + `order_item_id` |
| Final rows | **112,650** |
| Final columns | **59** |
| Duplicate canonical keys | **0** |
| Export formats | CSV and Parquet |

These figures are from the completed project run against the original source data. Results will differ if the input files change.

## Architecture

```text
11 raw CSV sources (data/raw)
          |
          v
  Extract and profile
          |
          v
 Clean and standardise
          |
          v
 Grain-aware integration
          |
          v
 Feature engineering
          |
          v
 Validate and reconcile
          |
          v
CSV + Parquet (data/output)
          |
          v
 Business insight notebooks
```

The canonical model starts from **order items**, then enriches them with orders, customers, products, translated categories, sellers, aggregated order-level payments and reviews, and geographic lookups. One-to-many inputs are aggregated before joining where necessary to avoid row fan-out.

### Key engineering decisions

- **Explicit grain:** one record per (`order_id`, `order_item_id`).
- **Cardinality controls:** merge validation, row-count reconciliation and duplicate-key checks.
- **Left joins:** retain valid order items even when optional enrichment is unavailable.
- **Order-level aggregation:** summarise payments and reviews before joining to order items.
- **Geography lookup:** deduplicate geolocation records and derive ZIP-prefix-based coordinates.
- **Transparent data quality:** flag exceptions rather than silently discarding records.
- **Separate marketing domain:** avoid forcing marketing leads and closed deals into a transaction-grain table.

## Data sources

The 11 input files are:

1. `customers_dataset.csv`
2. `orders_dataset.csv`
3. `order_items_dataset.csv`
4. `products_dataset.csv`
5. `sellers_dataset.csv`
6. `order_payments_dataset.csv`
7. `order_reviews_dataset.csv`
8. `geolocation_dataset.csv`
9. `product_category_name_translation.csv`
10. `marketing_qualified_leads_dataset.csv`
11. `closed_deals_dataset.csv`

The source files are treated as immutable inputs. **Source CSVs are not necessarily redistributed with this public repository.** Obtain the original data from its authorised source and confirm its licence and permitted reuse before publishing or sharing copies.

## Project structure

```text
data_engineering_exercise/
├── data/
│   ├── raw/                       # input CSV files (may be excluded publicly)
│   └── output/                    # generated CSV and Parquet outputs
├── notebooks/
│   ├── 01_data_profiling.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_data_integration.ipynb
│   └── 04_business_insight.ipynb
├── src/
│   └── pipeline.py
├── docs/
│   ├── Business Problem Statement.docx
│   ├── Technical Implementation Report.docx
│   └── Diagram 1.png
├── README.md
└── requirements.txt
```

*The documentation filenames above describe the intended public repository organisation. Adjust the paths to match the files actually committed.*

## Setup and execution

**Requirements:** Python 3.12 and a terminal. Run the commands from the project root.

### 1. Create and activate a virtual environment (Windows PowerShell)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, you can use the project interpreter directly without changing execution policy:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe src/pipeline.py
```

### 2. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 3. Provide the data

Place the 11 authoritative CSV files, with the filenames listed above, in `data/raw/`.

### 4. Run the ETL pipeline

```powershell
python src/pipeline.py
```

For the original dataset, the successful run reported:

```text
Final rows                : 112,650
Unique order-item keys    : 112,650
Duplicate order-item keys : 0
FINAL DATASET VALIDATION: PASSED
EXPORT VALIDATION: PASSED
```

Generated files:

```text
data/output/olist_integrated_dataset.csv
data/output/olist_integrated_dataset.parquet
```

The pipeline is the repeatable implementation. The notebooks document profiling, cleaning, integration and exploratory business insights; open them in VS Code or Jupyter with the project Python environment selected.

## Data quality and validation

Documented findings from the original run include:

| Check or exception | Result |
|---|---:|
| Exact geolocation duplicates removed | 261,831 |
| Datetime columns standardised | 10 |
| Text values cleaned | 11,450 |
| Missing delivered-date exceptions | 8 |
| Non-positive payment exceptions | 9 |
| Non-positive product-weight exceptions | 4 |
| Order-item rows without review information | 942 |
| Customer records without available coordinates | 278 |
| Seller records without available coordinates | 7 |
| Calculated customer-to-seller distances | 112,096 |
| Unavailable distances | 554 |

Validation distinguishes **fatal pipeline errors** (unexpected row multiplication, duplicate grain keys, invalid lookup cardinality) from **known source exceptions** that are retained and flagged.

## Analytical features and questions

The engineered output includes `item_total_value`, `delivery_days`, `delivery_vs_estimate_days`, `delivery_performance` and `customer_seller_distance_km`. Geographic distance uses the Haversine formula and represents straight-line distance, **not** road distance.

The dataset supports exploration of sales and category performance, seller activity, delivery reliability, freight and distance patterns, and the relationship between delivery experience and review scores.

**Important modelling caution:** Payment totals and order-level review measures are repeated across items after enrichment. Do not sum order-level payment values across the item-grain table without an appropriate distinct-order aggregation or allocation method.

## Limitations

- Version 1 does **not** establish a fully validated marketing-lead-to-transaction attribution funnel.
- Source geography is based on ZIP-code-prefix lookup data, not exact addresses.
- Missing enrichment values may remain in the canonical output.
- The solution is a local batch pipeline, not a scheduled or incrementally ingested production service.
- Some validation expectations are tied to the original supplied dataset.
- Notebooks support exploration; the Python pipeline is the reproducible ETL entry point.

## Roadmap — Version 2 (proposed, not implemented)

- Modular and configuration-driven transformation and insight generation.
- Automated tests, structured logging and monitoring.
- Containerised execution with Docker and a documented handover process.
- More flexible validation rules and data contracts.
- Validated marketing-to-seller-to-order linkage, if supported by source identifiers.
- Optional orchestration and cloud deployment (e.g. Microsoft Fabric or Databricks).

## Documentation

See the `docs/` folder for the **Business Problem Statement**, **Technical Implementation Report**, and the architecture / data-model diagrams. The technical report contains the detailed profiling, transformation, validation and modelling rationale.

## Author

**Hanson Olatunde**  
Data Engineering | Business Intelligence | Analytics

---

*This repository is an independent technical portfolio case study. It is not affiliated with or endorsed by the organisation that supplied the original exercise.*
