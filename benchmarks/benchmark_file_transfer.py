"""Repeated loopback benchmark for the existing Mini-TCP file transfer.

Run from the repository root with ``PYTHONPATH=. python3
benchmarks/benchmark_file_transfer.py``. This script never configures network
impairment and labels results as localhost measurements.
"""

import argparse
import csv
import hashlib
import html
import inspect
import io
import os
import queue
import secrets
import statistics
import tempfile
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from minitcp.connection import MiniTCPConnection
from minitcp.file_transfer import receive_file, send_file
from minitcp.sliding_window import SlidingWindowReceiver, SlidingWindowSender
from minitcp.transport import UDPTransport

DEFAULT_SIZES = (4 * 1024, 64 * 1024, 1024 * 1024)
DEFAULT_REPETITIONS = 5
MEASUREMENT_CONTEXT = "localhost_loopback_external_conditions_unverified"
CSV_FIELDS = (
    "run_number", "file_size_bytes", "elapsed_seconds", "throughput_kb_s",
    "transfer_success", "sha256_verified", "source_sha256",
    "destination_sha256", "measurement_context", "error",
)
SUMMARY_FIELDS = (
    "file_size_bytes", "median_elapsed_seconds_all_attempts",
    "median_throughput_kb_s_verified_successes",
    "successful_throughput_samples", "successful_runs", "total_runs",
)
CWND_FIELDS = (
    "run_number", "file_size_bytes", "elapsed_seconds", "event", "state",
    "cwnd_segments", "ssthresh_segments", "duplicate_ack_count",
)


def calculate_throughput_kb_s(file_size_bytes, elapsed_seconds):
    """Calculate KB/s using 1024 bytes per KB."""
    if file_size_bytes < 0:
        raise ValueError("file size cannot be negative")
    if elapsed_seconds <= 0:
        raise ValueError("elapsed time must be positive")
    return file_size_bytes / 1024 / elapsed_seconds


def make_result(file_size_bytes, elapsed_seconds, transfer_success,
                sha256_verified, source_sha256="", destination_sha256="",
                error="", run_number=1, measurement_context=MEASUREMENT_CONTEXT):
    verified_success = bool(transfer_success and sha256_verified)
    return {
        "run_number": run_number,
        "file_size_bytes": file_size_bytes,
        "elapsed_seconds": elapsed_seconds,
        "throughput_kb_s": (
            calculate_throughput_kb_s(file_size_bytes, elapsed_seconds)
            if verified_success else ""
        ),
        "transfer_success": bool(transfer_success),
        "sha256_verified": bool(sha256_verified),
        "source_sha256": source_sha256,
        "destination_sha256": destination_sha256,
        "measurement_context": measurement_context,
        "error": error,
    }


def summarize_results(results):
    """Summarize elapsed time across attempts and throughput across verified successes."""
    grouped = {}
    for row in results:
        grouped.setdefault(row["file_size_bytes"], []).append(row)
    summaries = []
    for size in sorted(grouped):
        rows = grouped[size]
        successful_throughputs = [
            row["throughput_kb_s"] for row in rows
            if row["transfer_success"] and row["sha256_verified"]
        ]
        summaries.append({
            "file_size_bytes": size,
            "median_elapsed_seconds_all_attempts": statistics.median(
                row["elapsed_seconds"] for row in rows
            ),
            "median_throughput_kb_s_verified_successes": (
                statistics.median(successful_throughputs)
                if successful_throughputs else ""
            ),
            "successful_throughput_samples": len(successful_throughputs),
            "successful_runs": sum(bool(row["transfer_success"]) for row in rows),
            "total_runs": len(rows),
        })
    return summaries


def _open_unique(path, mode, **kwargs):
    """Open a new output path without overwriting an existing user file."""
    requested = Path(path)
    requested.parent.mkdir(parents=True, exist_ok=True)
    attempt = 0
    while True:
        candidate = requested if attempt == 0 else requested.with_name(
            f"{requested.stem}_{attempt}{requested.suffix}"
        )
        try:
            return candidate.open(mode.replace("w", "x"), **kwargs)
        except FileExistsError:
            attempt += 1


def write_results_csv(path, results):
    """Write individual attempts and return the actual non-overwritten path."""
    with _open_unique(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(results)
        return Path(output.name)


def write_summary_csv(path, summaries):
    """Write per-size medians/counts and return the actual path used."""
    with _open_unique(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summaries)
        return Path(output.name)


def write_congestion_csv(path, events):
    """Write structured observations emitted by the actual controller."""
    with _open_unique(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=CWND_FIELDS)
        writer.writeheader()
        writer.writerows(events)
        return Path(output.name)


def write_congestion_svg(path, events, measurement_context=MEASUREMENT_CONTEXT):
    """Plot recorded congestion-controller events, without estimating values."""
    if not events:
        raise ValueError("cannot graph an empty congestion trace")
    width, height = 900, 560
    left, right, top, bottom = 95, 40, 85, 105
    chart_w, chart_h = width - left - right, height - top - bottom
    max_time = max((float(row["elapsed_seconds"]) for row in events), default=0) or 1
    max_cwnd = max(float(row["cwnd_segments"]) for row in events) or 1
    sizes = sorted({row["file_size_bytes"] for row in events})
    palette = ("#1769aa", "#e67e22", "#238b45", "#9c36b5", "#c0392b")
    grouped = {}
    for row in events:
        grouped.setdefault((row["file_size_bytes"], row["run_number"]), []).append(row)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="450" y="28" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">Mini-TCP Congestion Window Evolution</text>',
        f'<text x="450" y="52" text-anchor="middle" font-family="sans-serif" font-size="12">Actual controller events; context: {html.escape(measurement_context)}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_h}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top + chart_h}" x2="{left + chart_w}" y2="{top + chart_h}" stroke="#333"/>',
        f'<text x="25" y="{top + chart_h / 2}" transform="rotate(-90 25 {top + chart_h / 2})" text-anchor="middle" font-family="sans-serif" font-size="14">Congestion window (segments)</text>',
        f'<text x="{left + chart_w / 2}" y="{height - 20}" text-anchor="middle" font-family="sans-serif" font-size="14">Time since sender start (seconds)</text>',
    ]
    for (size, run), rows in sorted(grouped.items()):
        color = palette[sizes.index(size) % len(palette)]
        rows.sort(key=lambda row: float(row["elapsed_seconds"]))
        points = " ".join(
            f'{left + chart_w * float(row["elapsed_seconds"]) / max_time:.1f},'
            f'{top + chart_h * (1 - float(row["cwnd_segments"]) / max_cwnd):.1f}'
            for row in rows
        )
        parts.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="1.5" opacity="0.65"><title>{size:,} B run {run}</title></polyline>'
        )
    for fraction in (0, 0.5, 1):
        x = left + chart_w * fraction
        parts.append(
            f'<text x="{x:.1f}" y="{top + chart_h + 24}" text-anchor="middle" font-family="sans-serif" font-size="12">{max_time * fraction:.2f}</text>'
        )
    for i, size in enumerate(sizes):
        parts.append(
            f'<text x="{left + i * 155}" y="{height - 52}" font-family="sans-serif" font-size="12" fill="{palette[i % len(palette)]}">{size:,} B (runs)</text>'
        )
    with _open_unique(path, "w", encoding="utf-8") as output:
        output.write("\n".join(parts + ["</svg>"]))
        return Path(output.name)


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(64 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_paths(source_path, destination_path):
    """Return both file digests and whether the destination is a match."""
    source_digest = _sha256(source_path)
    if not destination_path or not Path(destination_path).is_file():
        return source_digest, "", False
    destination_digest = _sha256(destination_path)
    return source_digest, destination_digest, source_digest == destination_digest


def _create_source(path, size):
    """Create fresh, non-reused source bytes in this run's private directory."""
    remaining = size
    with Path(path).open("xb") as output:
        while remaining:
            block = secrets.token_bytes(min(64 * 1024, remaining))
            output.write(block)
            remaining -= len(block)


def _benchmark_one(file_size_bytes, run_id, work_dir,
                   measurement_context=MEASUREMENT_CONTEXT,
                   congestion_events=None):
    """Run one complete handshake, file transfer, and verification response."""
    attempt_started = time.perf_counter()
    measurement_started = None
    measured_finished = None
    run_dir = Path(work_dir) / f"run_{run_id:04d}"
    run_dir.mkdir()
    source = run_dir / f"source_{file_size_bytes}.bin"
    destination_dir = run_dir / "received"
    destination_dir.mkdir()
    source_digest = ""
    status = None
    send_error = ""
    receiver_path = None
    receiver_error = None
    server_transport = None
    client_transport = None
    thread = None
    server_result = queue.Queue(maxsize=1)

    try:
        _create_source(source, file_size_bytes)
        source_digest = _sha256(source)

        server_transport = UDPTransport("127.0.0.1", 0)
        server_transport.bind()
        server_address = server_transport.socket.getsockname()

        def receive_transfer():
            try:
                connection = MiniTCPConnection(server_transport)
                server_transport.set_timeout(2.0)
                connection.accept()
                server_transport.set_timeout(2.0)
                receiver = SlidingWindowReceiver(connection, window_size=8,
                                                 segment_size=512)
                response_sender = SlidingWindowSender(
                    connection, window_size=8, segment_size=512,
                    timeout=0.25, max_retries=2,
                    incoming_data_handler=receiver.acknowledge_retransmitted_data,
                )
                saved_path = receive_file(receiver, response_sender,
                                          destination_dir)
                server_result.put((saved_path, None))
            except Exception as exc:
                server_result.put((None, exc))
            finally:
                server_transport.close()

        thread = threading.Thread(target=receive_transfer, daemon=True)
        thread.start()

        client_transport = UDPTransport("127.0.0.1", 0)
        client_transport.bind()
        client_transport.set_timeout(2.0)
        client_connection = MiniTCPConnection(client_transport)
        measurement_started = time.perf_counter()
        client_connection.connect(server_address)
        client_transport.set_timeout(0.25)
        def record_congestion_event(event):
            if congestion_events is not None:
                congestion_events.append({
                    "run_number": run_id,
                    "file_size_bytes": file_size_bytes,
                    **event,
                })

        sender = SlidingWindowSender(client_connection, window_size=8,
                                     segment_size=512, timeout=0.25,
                                     max_retries=2,
                                     congestion_event_handler=(
                                         record_congestion_event
                                         if congestion_events is not None
                                         else None
                                     ))
        status_receiver = SlidingWindowReceiver(
            client_connection, window_size=8, segment_size=512
        )
        status = send_file(sender, status_receiver, source)
        measured_finished = time.perf_counter()
    except Exception as exc:
        send_error = f"{type(exc).__name__}: {exc}"
    finally:
        if measured_finished is None:
            measured_finished = time.perf_counter()
        if client_transport is not None:
            client_transport.close()
        if thread is not None:
            thread.join(timeout=3)
            if thread.is_alive():
                if server_transport is not None:
                    server_transport.close()
                thread.join(timeout=2)
            if thread.is_alive():
                receiver_error = TimeoutError(
                    "receiver thread did not stop within 5 seconds"
                )
            else:
                try:
                    receiver_path, receiver_error = server_result.get_nowait()
                except queue.Empty:
                    receiver_error = RuntimeError(
                        "receiver finished without returning a result"
                    )
        elif server_transport is not None:
            server_transport.close()

    elapsed = max(measured_finished - (measurement_started if measurement_started is not None else attempt_started), 1e-9)
    if source.exists():
        source_digest, destination_digest, sha_verified = _verify_paths(
            source, receiver_path
        )
    else:
        destination_digest, sha_verified = "", False
    success = bool(
        status and status.get("ok") and receiver_error is None
        and sha_verified and Path(receiver_path).stat().st_size == file_size_bytes
    )
    errors = [message for message in (
        send_error,
        f"{type(receiver_error).__name__}: {receiver_error}"
        if receiver_error else "",
    ) if message]
    if not success and not errors:
        errors.append("transfer or integrity verification failed")
    return make_result(file_size_bytes, elapsed, success, sha_verified,
                       source_digest, destination_digest, "; ".join(errors),
                       run_number=run_id, measurement_context=measurement_context)


def benchmark_one(file_size_bytes, run_id, work_dir, log_stream=None,
                  measurement_context=MEASUREMENT_CONTEXT,
                  congestion_events=None):
    """Run quietly by default; optionally route detailed protocol logs to a file."""
    sink = log_stream if log_stream is not None else io.StringIO()
    if log_stream is not None:
        log_stream.write(f"\n--- file_size={file_size_bytes} run={run_id} ---\n")
        log_stream.flush()
    with redirect_stdout(sink), redirect_stderr(sink):
        return _benchmark_one(file_size_bytes, run_id, work_dir,
                              measurement_context, congestion_events)


def run_measurements(sizes, repetitions=DEFAULT_REPETITIONS, log_stream=None,
                     benchmark_func=benchmark_one, measurement_context=MEASUREMENT_CONTEXT,
                     results_writer=None, congestion_events=None):
    """Run every size/repetition in a fresh, automatically cleaned temp tree."""
    if repetitions < 1:
        raise ValueError("repetitions must be at least one")
    results = []
    run_id = 0
    with tempfile.TemporaryDirectory(prefix="minitcp-bench-") as work_dir:
        for size in sizes:
            if size < 0:
                raise ValueError("file sizes must be nonnegative")
            for _ in range(repetitions):
                run_id += 1
                started = time.perf_counter()
                try:
                    parameters = inspect.signature(benchmark_func).parameters
                    kwargs = {}
                    args = [size, run_id, work_dir, log_stream]
                    if "measurement_context" in parameters:
                        kwargs["measurement_context"] = measurement_context
                    elif len(parameters) >= 5:
                        args.append(measurement_context)
                    if "congestion_events" in parameters:
                        kwargs["congestion_events"] = congestion_events
                    result = benchmark_func(*args, **kwargs)
                except Exception as exc:
                    elapsed = max(time.perf_counter() - started, 1e-9)
                    result = make_result(
                        size, elapsed, False, False,
                        error=f"{type(exc).__name__}: {exc}",
                        run_number=run_id,
                    )
                result["run_number"] = run_id
                result["measurement_context"] = measurement_context
                results.append(result)
                if results_writer is not None:
                    results_writer.writerow(result)
                    stream = getattr(results_writer, "f", results_writer)
                    flush = getattr(stream, "flush", None)
                    if flush:
                        flush()
    return results


def _write_svg(path, summaries, measurement_context=MEASUREMENT_CONTEXT):
    """Plot median throughput, including labels and success counts per size."""
    if not summaries:
        raise ValueError("cannot graph an empty summary")
    width, height = 820, 500
    left, right, top, bottom = 100, 35, 85, 115
    chart_w, chart_h = width - left - right, height - top - bottom
    maximum = max((r["median_throughput_kb_s_verified_successes"] or 0 for r in summaries), default=0)
    maximum = maximum or 1
    xs = [left + chart_w * i / max(1, len(summaries) - 1)
          for i in range(len(summaries))]
    points = [(x, top + chart_h * (1 - (r["median_throughput_kb_s_verified_successes"] or 0) / maximum))
              for x, r in zip(xs, summaries)]
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="410" y="28" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">Mini-TCP Localhost Throughput</text>',
        f'<text x="410" y="52" text-anchor="middle" font-family="sans-serif" font-size="12">Median of verified successful runs; context: {html.escape(measurement_context)}</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_h}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top + chart_h}" x2="{left + chart_w}" y2="{top + chart_h}" stroke="#333"/>',
        f'<polyline points="{polyline}" fill="none" stroke="#1769aa" stroke-width="3"/>',
        f'<text x="25" y="{top + chart_h / 2}" transform="rotate(-90 25 {top + chart_h / 2})" text-anchor="middle" font-family="sans-serif" font-size="14">Throughput (KB/s)</text>',
        f'<text x="{left + chart_w / 2}" y="{height - 20}" text-anchor="middle" font-family="sans-serif" font-size="14">File size (bytes)</text>',
    ]
    for (x, y), row in zip(points, summaries):
        label = html.escape(f"{row['file_size_bytes']:,} B")
        parts.extend((
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="#1769aa"/>',
            f'<text x="{x:.1f}" y="{top + chart_h + 24}" text-anchor="middle" font-family="sans-serif" font-size="13">{label}</text>',
            f'<text x="{x:.1f}" y="{y - 12:.1f}" text-anchor="middle" font-family="sans-serif" font-size="13">{(row["median_throughput_kb_s_verified_successes"] or 0):.1f}</text>',
            f'<text x="{x:.1f}" y="{top + chart_h + 43}" text-anchor="middle" font-family="sans-serif" font-size="11">{row["successful_runs"]}/{row["total_runs"]} successful</text>',
        ))
    graph_path = Path(path)
    with _open_unique(graph_path, "w", encoding="utf-8") as output:
        output.write("\n".join(parts + ["</svg>"]))
        return Path(output.name)


def _parse_sizes(value):
    try:
        sizes = tuple(int(part.strip()) for part in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "sizes must be comma-separated integers"
        ) from exc
    if not sizes or any(size < 0 for size in sizes):
        raise argparse.ArgumentTypeError("sizes must be nonnegative")
    return sizes


def _print_summary(summaries, results):
    for row in summaries:
        hashes_verified = sum(
            bool(result["sha256_verified"])
            for result in results
            if result["file_size_bytes"] == row["file_size_bytes"]
        )
        hashes = f"SHA-256 verified {hashes_verified}/{row['total_runs']}"
        print(
            f"{row['file_size_bytes']:,} B: median elapsed "
            f"{row['median_elapsed_seconds_all_attempts']:.6f} s (all attempts); median throughput "
            f"{(row['median_throughput_kb_s_verified_successes'] or 0):.2f} KB/s "
            f"({row['successful_throughput_samples']} verified samples); successful runs "
            f"{row['successful_runs']}/{row['total_runs']}; {hashes}"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=_parse_sizes, default=DEFAULT_SIZES,
                        help="comma-separated byte counts (default: 4096,65536,1048576)")
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS,
                        help="runs per size (default: 5)")
    parser.add_argument("--csv", default="benchmark_results.csv",
                        help="individual-run CSV path")
    parser.add_argument("--summary-csv", default="benchmark_summary.csv",
                        help="per-size summary CSV path")
    parser.add_argument("--plot", default="benchmark_throughput.svg",
                        help="median-throughput SVG path; empty disables graph")
    parser.add_argument("--log-file", default="",
                        help="save detailed protocol logs to this new file")
    parser.add_argument("--measurement-context", default=MEASUREMENT_CONTEXT,
                        help="user-supplied context label; impairment is never detected automatically")
    parser.add_argument("--cwnd-csv", default="",
                        help="optional CSV of actual sender congestion-controller events")
    parser.add_argument("--cwnd-svg", default="",
                        help="optional SVG path for the congestion-window plot; defaults beside --cwnd-csv")
    args = parser.parse_args(argv)
    if args.repetitions < 1:
        parser.error("--repetitions must be at least one")

    log_context = None
    log_stream = None
    if args.log_file:
        log_stream = _open_unique(args.log_file, "w", encoding="utf-8")
        log_context = log_stream
    else:
        log_context = open(os.devnull, "w", encoding="utf-8")
    try:
        csv_output = _open_unique(args.csv, "w", newline="", encoding="utf-8")
        try:
            csv_writer = csv.DictWriter(csv_output, fieldnames=CSV_FIELDS)
            csv_writer.writeheader()
            csv_output.flush()
            congestion_events = [] if (args.cwnd_csv or args.cwnd_svg) else None
            results = run_measurements(args.sizes, args.repetitions,
                                       log_stream=log_context,
                                       measurement_context=args.measurement_context,
                                       results_writer=csv_writer,
                                       congestion_events=congestion_events)
            individual_path = Path(csv_output.name)
        finally:
            csv_output.close()
    finally:
        log_context.close()

    summaries = summarize_results(results)
    _print_summary(summaries, results)
    summary_path = write_summary_csv(args.summary_csv, summaries)
    print(f"Individual results CSV: {individual_path}")
    print(f"Summary CSV: {summary_path}")
    if args.plot:
        graph_path = _write_svg(args.plot, summaries, args.measurement_context)
        print(f"Median throughput graph: {graph_path}")
    if args.cwnd_csv or args.cwnd_svg:
        event_rows = congestion_events or []
        if args.cwnd_csv:
            trace_path = write_congestion_csv(args.cwnd_csv, event_rows)
            print(f"Congestion events CSV: {trace_path}")
        if args.cwnd_svg:
            plot_requested = args.cwnd_svg
        elif args.cwnd_csv:
            plot_requested = str(Path(args.cwnd_csv).with_suffix(".svg"))
        else:
            plot_requested = "benchmark_congestion_window.svg"
        if event_rows:
            cwnd_plot = write_congestion_svg(
                plot_requested, event_rows, args.measurement_context
            )
            print(f"Congestion-window graph: {cwnd_plot}")
        else:
            print("No congestion events were recorded; no cwnd graph generated.")
    if args.log_file:
        print(f"Detailed protocol logs: {log_context.name}")
    print(f"Measurement context: {args.measurement_context}")
    failed = [row for row in results if not row["transfer_success"]]
    if failed:
        for row in failed:
            print(f"Run {row['run_number']} ({row['file_size_bytes']} B) failed: {row['error']}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
