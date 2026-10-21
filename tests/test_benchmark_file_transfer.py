import csv
import tempfile
import unittest
from pathlib import Path

from benchmarks.benchmark_file_transfer import (
    DEFAULT_REPETITIONS,
    DEFAULT_SIZES,
    MEASUREMENT_CONTEXT,
    _create_source,
    _verify_paths,
    _write_svg,
    calculate_throughput_kb_s,
    make_result,
    run_measurements,
    summarize_results,
    write_results_csv,
    write_summary_csv,
    CSV_FIELDS,
)


class BenchmarkCalculationTests(unittest.TestCase):
    def test_defaults_are_five_repetitions_of_requested_sizes(self):
        self.assertEqual(DEFAULT_REPETITIONS, 5)
        self.assertEqual(DEFAULT_SIZES, (4096, 65536, 1048576))

    def test_throughput_uses_1024_bytes_per_kb(self):
        self.assertEqual(calculate_throughput_kb_s(4096, 2), 2.0)

    def test_throughput_rejects_invalid_measurements(self):
        with self.assertRaises(ValueError):
            calculate_throughput_kb_s(-1, 1)
        with self.assertRaises(ValueError):
            calculate_throughput_kb_s(10, 0)

    def test_elapsed_median_includes_failures_but_throughput_does_not(self):
        rows = [
            make_result(1024, 2, True, True),   # 0.5 KB/s
            make_result(1024, 10, False, False, error="timeout"),  # 0.1
            make_result(1024, 4, True, True),   # 0.25
        ]
        summary, = summarize_results(rows)
        self.assertEqual(summary["median_elapsed_seconds_all_attempts"], 4)
        self.assertEqual(summary["median_throughput_kb_s_verified_successes"], 0.375)
        self.assertEqual(summary["successful_throughput_samples"], 2)
        self.assertEqual(summary["successful_runs"], 2)
        self.assertEqual(summary["total_runs"], 3)
        self.assertEqual(rows[1]["throughput_kb_s"], "")

    def test_source_and_destination_sha256_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.bin"
            destination = Path(directory) / "received.bin"
            source.write_bytes(b"binary\x00payload")
            destination.write_bytes(source.read_bytes())
            source_hash, destination_hash, verified = _verify_paths(
                source, destination
            )
            self.assertEqual(source_hash, destination_hash)
            self.assertTrue(verified)

            destination.write_bytes(b"changed")
            _, destination_hash, verified = _verify_paths(source, destination)
            self.assertNotEqual(source_hash, destination_hash)
            self.assertFalse(verified)

    def test_each_source_is_new_and_existing_source_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "run_1.bin"
            second = Path(directory) / "run_2.bin"
            _create_source(first, 128)
            _create_source(second, 128)
            self.assertEqual(first.stat().st_size, 128)
            self.assertEqual(second.stat().st_size, 128)
            self.assertNotEqual(first.read_bytes(), second.read_bytes())
            with self.assertRaises(FileExistsError):
                _create_source(first, 1)
            self.assertEqual(first.stat().st_size, 128)


class BenchmarkOutputTests(unittest.TestCase):
    def test_individual_and_summary_csv_output(self):
        results = [
            make_result(4096, 2, True, True, "abc", "abc", run_number=1),
            make_result(4096, 4, False, False, "abc", "bad", "timeout",
                        run_number=2),
        ]
        summaries = summarize_results(results)
        with tempfile.TemporaryDirectory() as directory:
            individual_path = write_results_csv(
                Path(directory) / "nested" / "runs.csv", results
            )
            summary_path = write_summary_csv(
                Path(directory) / "nested" / "summary.csv", summaries
            )
            with individual_path.open(newline="", encoding="utf-8") as source:
                individual = list(csv.DictReader(source))
            with summary_path.open(newline="", encoding="utf-8") as source:
                summary_rows = list(csv.DictReader(source))

        self.assertEqual(len(individual), 2)
        self.assertEqual(individual[1]["transfer_success"], "False")
        self.assertEqual(individual[1]["error"], "timeout")
        self.assertEqual(individual[0]["measurement_context"],
                         MEASUREMENT_CONTEXT)
        self.assertEqual(summary_rows[0]["successful_runs"], "1")
        self.assertEqual(summary_rows[0]["total_runs"], "2")
        self.assertEqual(summary_rows[0]["successful_throughput_samples"], "1")
        self.assertEqual(summary_rows[0]["median_elapsed_seconds_all_attempts"], "3.0")

    def test_existing_csv_is_preserved_and_new_name_is_selected(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "benchmark.csv"
            output.write_text("user data", encoding="utf-8")
            actual = write_results_csv(output, [])
            self.assertEqual(output.read_text(encoding="utf-8"), "user data")
            self.assertEqual(actual.name, "benchmark_1.csv")

    def test_graph_uses_medians_and_units_in_file_size_labels(self):
        summaries = [{
            "file_size_bytes": 4096,
            "median_throughput_kb_s_verified_successes": 12.5,
            "successful_runs": 4,
            "total_runs": 5,
        }, {
            "file_size_bytes": 65536,
            "median_throughput_kb_s_verified_successes": 25.0,
            "successful_runs": 5,
            "total_runs": 5,
        }]
        with tempfile.TemporaryDirectory() as directory:
            graph = _write_svg(Path(directory) / "nested" / "plot.svg", summaries)
            svg = graph.read_text(encoding="utf-8")
        self.assertIn("Mini-TCP Localhost Throughput", svg)
        self.assertIn("Throughput (KB/s)", svg)
        self.assertIn("4,096 B", svg)
        self.assertIn("65,536 B", svg)
        self.assertIn("Median of verified successful runs", svg)


class RepeatedMeasurementsTests(unittest.TestCase):
    def test_failed_rows_are_incrementally_written_with_context(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.csv"
            with path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
                writer.writeheader()

                def fail(size, run_id, work_dir, log_stream):
                    raise TimeoutError("lost packet")

                rows = run_measurements(
                    (4096,), 2, benchmark_func=fail,
                    measurement_context="operator_label_loss_test",
                    results_writer=writer,
                )
                output.flush()
            with path.open(newline="", encoding="utf-8") as source:
                saved_rows = list(csv.DictReader(source))
        self.assertEqual(len(saved_rows), 2)
        self.assertTrue(all(row["transfer_success"] == "False" for row in saved_rows))
        self.assertTrue(all(row["throughput_kb_s"] == "" for row in saved_rows))
        self.assertTrue(all(row["measurement_context"] == "operator_label_loss_test"
                            for row in saved_rows))
        self.assertEqual(len(rows), 2)

    def test_runs_each_size_requested_number_of_times_and_cleans_temp_tree(self):
        work_dirs = []
        seen_paths = []

        def fake_benchmark(size, run_id, work_dir, log_stream):
            work_dirs.append(Path(work_dir))
            source_path = Path(work_dir) / f"fresh_{run_id}.bin"
            source_path.write_bytes(f"{run_id}:{size}".encode())
            seen_paths.append(source_path)
            return make_result(size, float(run_id), True, True,
                               run_number=run_id)

        results = run_measurements((4096, 65536), 5, benchmark_func=fake_benchmark)
        self.assertEqual(len(results), 10)
        self.assertEqual([r["run_number"] for r in results], list(range(1, 11)))
        self.assertEqual(len(set(seen_paths)), 10)
        self.assertEqual(len(set(work_dirs)), 1)
        self.assertFalse(work_dirs[0].exists())

    def test_failed_transfer_is_retained_and_failure_is_visible_in_summary(self):
        def failed_benchmark(size, run_id, work_dir, log_stream):
            raise TimeoutError("simulated transfer timeout")

        rows = run_measurements((4096,), 2, benchmark_func=failed_benchmark)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(not row["transfer_success"] for row in rows))
        self.assertTrue(all("TimeoutError" in row["error"] for row in rows))
        summary, = summarize_results(rows)
        self.assertEqual(summary["successful_runs"], 0)
        self.assertEqual(summary["total_runs"], 2)

    def test_user_files_outside_benchmark_temp_tree_are_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sample = root / "sample.bin"
            received = root / "received_files"
            received.mkdir()
            existing = received / "existing.bin"
            sample.write_bytes(b"user sample")
            existing.write_bytes(b"user received content")

            def fake_benchmark(size, run_id, work_dir, log_stream):
                return make_result(size, 1, True, True)

            run_measurements((4096,), 1, benchmark_func=fake_benchmark)
            self.assertEqual(sample.read_bytes(), b"user sample")
            self.assertEqual(existing.read_bytes(), b"user received content")
            self.assertEqual(list(received.iterdir()), [existing])


if __name__ == "__main__":
    unittest.main()
