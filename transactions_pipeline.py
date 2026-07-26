"""
Apache Beam batch pipeline that:
  1. Reads transactions from a public GCS CSV file.
  2. Filters transactions with transaction_amount > 20.
  3. Excludes transactions made before the year 2010.
  4. Sums total transaction_amount grouped by date.
  5. Writes the result as gzip-compressed JSON Lines to output/results.jsonl.gz.

All of steps 2-4 (filter, exclude, sum) are grouped into a single
Composite Transform: `SumTransactionsByDate`.
"""

import csv
import io
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_PATH = "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
OUTPUT_PREFIX = "output/results"
MIN_AMOUNT = 20
MIN_YEAR = 2010


def parse_csv_line(line, header):
    """Parse a single CSV line into a dict using the given header list."""
    reader = csv.reader(io.StringIO(line))
    row = next(reader)
    return dict(zip(header, row))


class SumTransactionsByDate(beam.PTransform):
    """Composite transform: filter by amount, exclude pre-2010 rows, sum by date.

    Expects a PCollection of dicts with at least the keys:
      - 'timestamp' (string, format 'YYYY-MM-DD HH:MM:SS UTC')
      - 'transaction_amount' (string or numeric)

    Returns a PCollection of (date, total_amount) tuples, where date is the
    YYYY-MM-DD portion of the timestamp.
    """

    def expand(self, pcoll):
        return (
            pcoll
            | "FilterAmountGreaterThan20" >> beam.Filter(
                lambda row: float(row["transaction_amount"]) > MIN_AMOUNT
            )
            | "ExcludeBefore2010" >> beam.Filter(
                lambda row: int(row["timestamp"][:4]) >= MIN_YEAR
            )
            | "KeyByDate" >> beam.Map(
                lambda row: (row["timestamp"][:10], float(row["transaction_amount"]))
            )
            | "SumPerDate" >> beam.CombinePerKey(sum)
        )


def format_as_jsonl(date_total):
    """Format a (date, total) tuple as a JSON line."""
    date, total = date_total
    return json.dumps({"date": date, "total_amount": round(total, 2)})


def run():
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | "ReadCSV" >> beam.io.ReadFromText(
            INPUT_PATH, skip_header_lines=0
        )

        # Extract header from the first line, broadcast it as a side input.
        header_pcoll = (
            lines
            | "TakeFirstLine" >> beam.combiners.Sample.FixedSizeGlobally(1)
            | "ExtractHeader" >> beam.Map(
                lambda lines_list: next(csv.reader(io.StringIO(lines_list[0])))
            )
        )

        # Skip blank lines (e.g. a trailing empty line at end of file) before parsing.
        non_empty_lines = lines | "SkipBlankLines" >> beam.Filter(
            lambda line: line.strip() != ""
        )

        rows = (
            non_empty_lines
            | "ParseRows" >> beam.Map(
                parse_csv_line, header=beam.pvalue.AsSingleton(header_pcoll)
            )
        )

        # Drop the header row itself, and drop any malformed row missing
        # required columns (e.g. from an unexpected blank/short line).
        data_rows = rows | "DropHeaderAndMalformedRows" >> beam.Filter(
            lambda row: row.get("timestamp") != "timestamp"
            and "transaction_amount" in row
            and row["transaction_amount"] != ""
        )

        (
            data_rows
            | "SumTransactionsByDate" >> SumTransactionsByDate()
            | "FormatAsJson" >> beam.Map(format_as_jsonl)
            | "WriteOutput" >> beam.io.WriteToText(
                OUTPUT_PREFIX,
                file_name_suffix=".jsonl.gz",
                shard_name_template="",
            )
        )


if __name__ == "__main__":
    run()