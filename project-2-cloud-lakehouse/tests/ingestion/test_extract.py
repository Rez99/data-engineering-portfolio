import gzip
import io
import sys
import unittest
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / "deployment"
        / "containers"
        / "ingestion"
    ),
)

from extract import EXPECTED_COLUMNS, copy_sample, validate_sample


class CopySampleTest(unittest.TestCase):
    def test_writes_header_and_exact_row_limit(self):
        source = io.BytesIO(b"header\nrow-1\nrow-2\nrow-3\n")
        destination = io.BytesIO()

        rows_written = copy_sample(source, destination, max_rows=2)

        self.assertEqual(rows_written, 2)
        self.assertEqual(destination.getvalue(), b"header\nrow-1\nrow-2\n")

    def test_rejects_empty_source(self):
        with self.assertRaisesRegex(ValueError, "empty"):
            copy_sample(io.BytesIO(), io.BytesIO(), max_rows=1)

    def test_rejects_source_with_too_few_rows(self):
        with self.assertRaisesRegex(ValueError, "expected 2"):
            copy_sample(
                io.BytesIO(b"header\nrow-1\n"),
                io.BytesIO(),
                max_rows=2,
            )


class ValidateSampleTest(unittest.TestCase):
    def compressed_csv(self, header, rows):
        output = io.BytesIO()
        with gzip.GzipFile(fileobj=output, mode="wb") as compressed:
            compressed.write((",".join(header) + "\n").encode())
            for row in rows:
                compressed.write((",".join(row) + "\n").encode())

        output.seek(0)
        return output

    def test_validates_schema_and_exact_row_count(self):
        source = self.compressed_csv(
            EXPECTED_COLUMNS,
            [["value"] * len(EXPECTED_COLUMNS)] * 2,
        )

        rows_read = validate_sample(source, 2, EXPECTED_COLUMNS)

        self.assertEqual(rows_read, 2)

    def test_rejects_unexpected_schema(self):
        source = self.compressed_csv(["wrong_column"], [["value"]])

        with self.assertRaisesRegex(ValueError, "Unexpected columns"):
            validate_sample(source, 1, EXPECTED_COLUMNS)

    def test_rejects_unexpected_row_count(self):
        source = self.compressed_csv(
            EXPECTED_COLUMNS,
            [["value"] * len(EXPECTED_COLUMNS)],
        )

        with self.assertRaisesRegex(ValueError, "expected 2"):
            validate_sample(source, 2, EXPECTED_COLUMNS)

    def test_rejects_uncompressed_content(self):
        with self.assertRaisesRegex(ValueError, "not gzip-compressed"):
            validate_sample(
                io.BytesIO(b"event_time,event_type\n"),
                1,
                EXPECTED_COLUMNS,
            )


if __name__ == "__main__":
    unittest.main()
