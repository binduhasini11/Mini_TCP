import csv
import tempfile
import unittest
from pathlib import Path

from benchmarks.benchmark_file_transfer import (
    run_measurements,
    write_congestion_csv,
    write_congestion_svg,
)
from benchmarks.compare_kernel_tcp import summarize, tcp_transfer


class CongestionTraceToolTests(unittest.TestCase):
    def test_actual_sender_events_can_be_saved_and_plotted(self):
        events = []
        rows = run_measurements((256,), 1, congestion_events=events)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["transfer_success"])
        self.assertTrue(events)
        self.assertEqual(events[0]["event"], "initial")
        self.assertIn("cwnd_segments", events[0])
        with tempfile.TemporaryDirectory() as directory:
            csv_path = write_congestion_csv(Path(directory) / "events.csv", events)
            svg_path = write_congestion_svg(
                Path(directory) / "cwnd.svg", events, "unit_test_context"
            )
            with csv_path.open(newline="", encoding="utf-8") as source:
                saved = list(csv.DictReader(source))
            svg = svg_path.read_text(encoding="utf-8")
        self.assertEqual(len(saved), len(events))
        self.assertIn("Congestion window (segments)", svg)
        self.assertIn("unit_test_context", svg)


class KernelTcpComparisonTests(unittest.TestCase):
    def test_local_kernel_tcp_transfer_verifies_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            result = tcp_transfer(4096, 1, directory, "tcp_unit_test")
            run_dir = Path(directory) / "tcp_0001_4096"
            source = run_dir / "source.bin"
            destination = run_dir / "received.bin"
            self.assertTrue(result["transfer_success"], result["error"])
            self.assertTrue(result["sha256_verified"])
            self.assertEqual(source.stat().st_size, destination.stat().st_size)
            self.assertEqual(result["source_sha256"], result["destination_sha256"])

    def test_comparison_summary_keeps_failures_in_elapsed_denominator(self):
        rows = [
            {"protocol": "mini_tcp", "file_size_bytes": 1024,
             "elapsed_seconds": 1.0, "throughput_kb_s": 1.0,
             "transfer_success": True, "sha256_verified": True},
            {"protocol": "mini_tcp", "file_size_bytes": 1024,
             "elapsed_seconds": 9.0, "throughput_kb_s": "",
             "transfer_success": False, "sha256_verified": False},
        ]
        summary, = summarize(rows)
        self.assertEqual(summary["median_elapsed_seconds_all_attempts"], 5.0)
        self.assertEqual(summary["median_throughput_kb_s_verified_successes"], 1.0)
        self.assertEqual(summary["successful_runs"], 1)
        self.assertEqual(summary["total_runs"], 2)


if __name__ == "__main__":
    unittest.main()
