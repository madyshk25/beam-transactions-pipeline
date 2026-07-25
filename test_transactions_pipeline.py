"""
Unit test for the SumTransactionsByDate composite transform.

Run with:
    py -m pytest test_transactions_pipeline.py -v
"""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from transactions_pipeline import SumTransactionsByDate


class SumTransactionsByDateTest(unittest.TestCase):

    def test_filters_amount_year_and_sums_by_date(self):
        input_rows = [
            # Kept: amount > 20, year >= 2010
            {"timestamp": "2011-01-01 10:00:00 UTC", "transaction_amount": "50.00"},
            {"timestamp": "2011-01-01 12:00:00 UTC", "transaction_amount": "25.00"},
            # Excluded: amount <= 20
            {"timestamp": "2011-01-01 14:00:00 UTC", "transaction_amount": "20.00"},
            {"timestamp": "2012-06-15 09:00:00 UTC", "transaction_amount": "5.00"},
            # Excluded: year < 2010
            {"timestamp": "2009-12-31 23:59:59 UTC", "transaction_amount": "1000.00"},
            # Kept: separate date bucket
            {"timestamp": "2012-06-15 18:30:00 UTC", "transaction_amount": "30.50"},
        ]

        expected_output = [
            ("2011-01-01", 75.00),
            ("2012-06-15", 30.50),
        ]

        with TestPipeline() as pipeline:
            result = (
                pipeline
                | "CreateInput" >> beam.Create(input_rows)
                | "SumTransactionsByDate" >> SumTransactionsByDate()
            )

            assert_that(result, equal_to(expected_output))

    def test_empty_input_produces_empty_output(self):
        with TestPipeline() as pipeline:
            result = (
                pipeline
                | "CreateEmptyInput" >> beam.Create([])
                | "SumTransactionsByDate" >> SumTransactionsByDate()
            )

            assert_that(result, equal_to([]))

    def test_boundary_values(self):
        # amount == 20 should be excluded (must be strictly greater than 20)
        # year == 2010 should be included (boundary is "before 2010")
        input_rows = [
            {"timestamp": "2010-01-01 00:00:00 UTC", "transaction_amount": "20.01"},
            {"timestamp": "2010-01-01 00:00:01 UTC", "transaction_amount": "20.00"},
        ]

        expected_output = [
            ("2010-01-01", 20.01),
        ]

        with TestPipeline() as pipeline:
            result = (
                pipeline
                | "CreateInput" >> beam.Create(input_rows)
                | "SumTransactionsByDate" >> SumTransactionsByDate()
            )

            assert_that(result, equal_to(expected_output))


if __name__ == "__main__":
    unittest.main()