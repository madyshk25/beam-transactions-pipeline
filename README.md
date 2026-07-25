# Beam Transactions Pipeline

An Apache Beam batch pipeline that reads transaction data from a public GCS
bucket, filters and aggregates it, and writes the result as gzip-compressed
JSON Lines.

## What it does

1. Reads `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
   (columns: `timestamp`, `origin`, `destination`, `transaction_amount`)
2. Filters transactions with `transaction_amount > 20`
3. Excludes transactions dated before the year `2010` (based on the `timestamp` column)
4. Sums the total `transaction_amount` grouped by date (the `YYYY-MM-DD` portion of `timestamp`)
5. Writes the result to `output/results.jsonl.gz`

Steps 2-4 are grouped into a single **Composite Transform**, `SumTransactionsByDate`,
defined in `transactions_pipeline.py`.

## Prerequisites

- Python 3.10, 3.11, or 3.12
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud` CLI)
- Google Cloud Application Default Credentials (needed even for public buckets):