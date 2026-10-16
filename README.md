# Banking Transactions Data Pipeline

This project builds a small data pipeline that ingests raw banking transactions, finds data quality problems, cleans and validates the data, and publishes curated tables for analytics.

> The dataset is **synthetic** (invented for learning). It contains no real customer data.

## Files

| File | Purpose |
|---|---|
| `bank_transactions.csv` | Raw transaction feed (50 rows, with deliberate data quality issues) |
| `bank_data_pipeline.ipynb` | pandas notebook: profile, clean, validate, transform, aggregate, save (single machine) |
| `bank_data_pipeline_spark.ipynb` | PySpark notebook: the same pipeline as bronze, silver and gold Parquet layers (distributed engine) |
| `README.md` | This document: problems, solutions and how to run |

## Dataset dictionary

| Column | Description |
|---|---|
| transaction_id | Unique ID of each transaction (expected to be unique) |
| account_id | Bank account the transaction belongs to |
| customer_id | Owner of the account |
| transaction_date | Date in YYYY-MM-DD format |
| transaction_time | Time in HH:MM:SS format |
| transaction_type | CREDIT (money in) or DEBIT (money out) |
| amount | Transaction value (always positive in a clean feed) |
| currency | ISO currency code, for example MUR or USD |
| merchant_category | Type of spending or income |
| channel | POS, ONLINE, MOBILE, ATM, BRANCH or TRANSFER |
| branch_code | Branch that processed the transaction |
| balance_after | Account balance after the transaction |
| status | SUCCESS or FAILED |

## Problems found and solutions

| # | Problem | Example in data | Why it matters | Solution in the notebook |
|---|---|---|---|---|
| 1 | Duplicate records | TXN0009 has a different ID but is an exact copy of TXN0008 (15,000 MUR transfer) | Double-counts money and inflates totals | Remove rows that repeat every field except `transaction_id`, keep the first copy. Checking only `transaction_id` would miss this case |
| 2 | Missing amount | TXN0017 has an empty amount | Cannot compute balances or totals | Convert to NaN, flag, and send to quarantine. Never guess a money value |
| 3 | Missing account ID | TXN0044 has no account_id | Cannot link the transaction to an account or customer | Flag and quarantine for investigation |
| 4 | Missing category | TXN0048 has an empty merchant_category | Breaks category reports | Fill with `UNKNOWN` and keep the row (the money movement is still valid) |
| 5 | Impossible date | TXN0022 is dated 2026-02-30 | Date does not exist; breaks time reports | Parse with `errors='coerce'` so it becomes NaT, then quarantine |
| 6 | Inconsistent text formatting | TXN0027 has `debit ` (lowercase, trailing space) | Groups like "DEBIT" and "debit " are counted separately | Strip spaces and convert to upper case |
| 7 | Inconsistent currency code | TXN0035 has `mur` instead of `MUR` | Currency totals split into two groups | Normalise to upper case. Also check the currency against the account (see note below) |
| 8 | Negative amount on a DEBIT | TXN0031 is -200.00 | Sign convention is ambiguous; may hide a reversal or an error | Flag `amount < 0` and quarantine for review |
| 9 | Failed transactions | TXN0036 and TXN0040 have status FAILED | They did not move money, so counting them overstates activity | Separate them into quarantine. Only SUCCESS rows enter curated data |
| 10 | Numbers stored as text | All columns are read as text from the CSV | Sums and comparisons do not work on text | Convert amount and balance to numeric types |
| 11 | Dates stored as text | transaction_date is a string | Cannot filter or group by month | Convert to datetime and create `txn_month` |
| 12 | Large transactions need review | TXN0008 (15,000 MUR) | Large transfers are typically reviewed for anti-money-laundering (AML) rules | Flag amounts of 10,000 or more into `large_txns.csv` |
| 13 | Mixed currencies on one account | ACC1004 is a USD account, but TXN0035 is in MUR | Adding MUR and USD together gives a wrong total | Keep currencies separate in aggregates. The Spark notebook adds a `currency_mismatch` flag to every row of an account that uses more than one currency. Rows are kept, not quarantined |

### Note on problem 13
The notebook does not convert currencies. In a real bank you would use a daily exchange-rate table, convert to one reporting currency, and then aggregate.

## Two versions of the pipeline

| Version | Notebook | Engine | Output |
|---|---|---|---|
| pandas | `bank_data_pipeline.ipynb` | pandas, one machine | CSV files in `output/` |
| PySpark | `bank_data_pipeline_spark.ipynb` | Spark, local or cluster | Parquet layers in `lakehouse/` plus CSV extracts in `output_spark/` |

Both versions give the same results on this dataset.

### Medallion layers (PySpark version)

| Layer | Contents | Why |
|---|---|---|
| Bronze | Raw rows as Parquet, with `_source_file` and `_ingested_at` | Keeps an unchanged copy of what arrived, so any later step can be replayed |
| Silver (clean) | Typed, de-duplicated, validated rows, partitioned by `txn_month` | Trusted data for analytics |
| Silver (quarantine) | Rejected rows with a `reason_codes` column (for example `MISSING_AMOUNT,FAILED_STATUS`) | Reviewers see exactly why each row was held back |
| Gold | Monthly account flows by currency, channel summary, large transactions | Business-ready tables for reports |

The Spark version is ELT: the raw file is loaded first (bronze), then transformed inside the lakehouse (silver and gold).

### Reason codes

| Code | Rule |
|---|---|
| `MISSING_ACCOUNT` | `account_id` is blank |
| `MISSING_AMOUNT` | `amount` is blank or not a number |
| `INVALID_DATE` | `transaction_date` is missing or impossible, such as 2026-02-30 |
| `NEGATIVE_AMOUNT` | `amount` is below zero |
| `FAILED_STATUS` | `status` is not SUCCESS |

### Reconciliation

The Spark notebook checks that every raw row is accounted for:

`raw rows (50) = clean (43) + quarantine (6) + duplicate copies removed (1)`

## Pipeline stages

```
raw CSV  ->  profile  ->  clean  ->  validate  ->  transform  ->  aggregate  ->  save
                                        |
                                        +-> quarantine (rows that need review)
```

1. **Ingest:** Read everything as text so nothing is changed silently.
2. **Profile:** Count nulls, duplicates and distinct values.
3. **Clean:** Fix casing, spaces, types and duplicates.
4. **Validate:** Apply business rules and split rows into clean and quarantine.
5. **Transform:** Add signed amount, month, hour and a large-transaction flag.
6. **Aggregate:** Build monthly account flows and channel summaries.
7. **Save:** Write curated and quarantine outputs to `output/`.

## Expected results

| Measure | Value |
|---|---|
| Raw rows | 50 |
| After removing duplicates | 49 |
| Quarantined (IDs 0017, 0022, 0031, 0036, 0040, 0044) | 6 |
| Clean curated rows | 43 |
| Large transactions (10,000 or more) | 1 (TXN0008) |

## How to run

1. Put `bank_transactions.csv` and `bank_data_pipeline.ipynb` in the same folder.
2. Install the libraries: `pip install pandas numpy`
3. Open the notebook in Jupyter, VS Code or Google Colab and run all cells.
4. Results appear in the `output/` folder:
   - `curated_transactions.csv`
   - `quarantine_transactions.csv`
   - `monthly_account_summary.csv`
   - `channel_summary.csv`
   - `large_transactions.csv`

## Running the PySpark version

1. Put `bank_transactions.csv` and `bank_data_pipeline_spark.ipynb` in the same folder.
2. Install PySpark and pandas (Java 17 or 21 must be installed): `pip install pyspark pandas`
3. Run all cells. Parquet layers are written to `lakehouse/` and CSV extracts to `output_spark/`.
4. On Databricks or a Spark cluster, remove the `.master('local[*]')` line and replace the local paths with cloud paths such as `abfss://` or `s3://`.

## Scaling to production

The notebook uses pandas, which fits a single file. For large volumes or a cloud platform, the same logic maps to:

| Notebook step | Production equivalent |
|---|---|
| CSV files | Landing zone in cloud storage (for example S3, ADLS or GCS) |
| pandas cleaning | PySpark or Databricks jobs (distributed processing) |
| Quarantine CSV | Quarantine table with reason codes |
| Curated CSV | Delta Lake or Parquet tables (bronze, silver and gold layers) |
| Rules in the notebook | Data quality framework such as Great Expectations or dbt tests |
| Re-running manually | Scheduled orchestration with Airflow, ADF or Databricks Workflows |
| Pandas notebook | The PySpark notebook (implemented in this project) |

## Known limitations

- Only one file is processed, and the pipeline is not incremental (it rebuilds the lakehouse on each run).
- The PySpark notebook runs on a single machine by default. It is distributed in design, but only a cluster run uses several machines.
- Business rules are simple examples. Real banks have many more controls.
- No balance reconciliation is performed. Adding this would be a good next step: check that each `balance_after` equals the previous balance plus or minus the amount.
