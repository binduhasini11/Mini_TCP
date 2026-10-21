"""Compare Mini-TCP file transfers with a kernel TCP localhost baseline.

Run with ``PYTHONPATH=. python3 benchmarks/compare_kernel_tcp.py``. This is a
throughput comparison, not a fairness or packet-loss experiment.
"""

import argparse
import csv
import hashlib
import html
import json
import socket
import statistics
import tempfile
import threading
import time
from pathlib import Path

from benchmarks.benchmark_file_transfer import (
    DEFAULT_REPETITIONS,
    DEFAULT_SIZES,
    benchmark_one,
    _create_source,
    _open_unique,
    _parse_sizes,
    _verify_paths,
    write_congestion_csv,
    write_congestion_svg,
)

FIELDS = (
    "protocol", "run_number", "file_size_bytes", "elapsed_seconds",
    "throughput_kb_s", "transfer_success", "sha256_verified",
    "source_sha256", "destination_sha256", "measurement_context", "error",
)
SUMMARY_FIELDS = (
    "protocol", "file_size_bytes", "median_elapsed_seconds_all_attempts",
    "median_throughput_kb_s_verified_successes", "successful_runs", "total_runs",
)


def _digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(64 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        block = sock.recv(size - len(chunks))
        if not block:
            raise EOFError(f"TCP peer closed after {len(chunks)} of {size} bytes")
        chunks.extend(block)
    return bytes(chunks)


def tcp_transfer(file_size, run_number, work_dir, context):
    """Run one bounded TCP transfer including connect and verification reply."""
    run_dir = Path(work_dir) / f"tcp_{run_number:04d}_{file_size}"
    run_dir.mkdir()
    source = run_dir / "source.bin"
    destination = run_dir / "received.bin"
    _create_source(source, file_size)
    source_hash = _digest(source)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(5.0)
    address = listener.getsockname()
    server_result = []
    accepted_socket = []

    def receive_and_verify():
        try:
            peer, _ = listener.accept()
            accepted_socket.append(peer)
            with peer:
                peer.settimeout(5.0)
                digest = hashlib.sha256()
                received = 0
                with destination.open("xb") as output:
                    while True:
                        block = peer.recv(64 * 1024)
                        if not block:
                            break
                        output.write(block)
                        digest.update(block)
                        received += len(block)
                actual_hash = digest.hexdigest()
                status = {
                    "ok": received == file_size and actual_hash == source_hash,
                    "size": received,
                    "sha256": actual_hash,
                }
                encoded = json.dumps(status, separators=(",", ":")).encode()
                peer.sendall(len(encoded).to_bytes(4, "big") + encoded)
                server_result.append(status)
        except Exception as exc:
            server_result.append(exc)
        finally:
            listener.close()

    worker = threading.Thread(target=receive_and_verify, daemon=True)
    worker.start()
    started = time.perf_counter()
    error = ""
    status = None
    try:
        with socket.create_connection(address, timeout=3.0) as client:
            client.settimeout(5.0)
            with source.open("rb") as payload:
                for block in iter(lambda: payload.read(64 * 1024), b""):
                    client.sendall(block)
            client.shutdown(socket.SHUT_WR)
            record_size = int.from_bytes(_read_exact(client, 4), "big")
            if record_size <= 0 or record_size > 4096:
                raise ValueError("invalid TCP verification response length")
            status = json.loads(_read_exact(client, record_size))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    elapsed = max(time.perf_counter() - started, 1e-9)
    worker.join(timeout=5.5)
    if worker.is_alive():
        if accepted_socket:
            accepted_socket[0].close()
        listener.close()
        worker.join(timeout=0.5)
        error = error or "TimeoutError: TCP receiver did not stop within 6 seconds"

    source_hash, destination_hash, hashes_match = _verify_paths(source, destination)
    successful = bool(
        status and status.get("ok") and not error and not worker.is_alive()
        and hashes_match and destination.stat().st_size == file_size
    )
    if server_result and isinstance(server_result[0], Exception):
        error = error or f"{type(server_result[0]).__name__}: {server_result[0]}"
        successful = False
    elif not successful and not error:
        error = "TCP transfer or integrity verification failed"
    throughput = file_size / 1024 / elapsed if successful else ""
    return {
        "protocol": "kernel_tcp", "run_number": run_number,
        "file_size_bytes": file_size, "elapsed_seconds": elapsed,
        "throughput_kb_s": throughput, "transfer_success": successful,
        "sha256_verified": hashes_match, "source_sha256": source_hash,
        "destination_sha256": destination_hash,
        "measurement_context": context, "error": error,
    }


def summarize(rows):
    summaries = []
    for protocol in ("mini_tcp", "kernel_tcp"):
        for size in sorted({row["file_size_bytes"] for row in rows}):
            attempts = [row for row in rows if row["protocol"] == protocol
                        and row["file_size_bytes"] == size]
            if not attempts:
                continue
            good = [row for row in attempts if row["transfer_success"]
                    and row["sha256_verified"]]
            summaries.append({
                "protocol": protocol,
                "file_size_bytes": size,
                "median_elapsed_seconds_all_attempts": statistics.median(
                    row["elapsed_seconds"] for row in attempts
                ),
                "median_throughput_kb_s_verified_successes": (
                    statistics.median(row["throughput_kb_s"] for row in good)
                    if good else ""
                ),
                "successful_runs": len(good),
                "total_runs": len(attempts),
            })
    return summaries


def _write_csv(path, fields, rows):
    with _open_unique(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        return Path(output.name)


def write_svg(path, summaries, context):
    width, height = 900, 500
    left, right, top, bottom = 100, 40, 85, 100
    chart_w, chart_h = width - left - right, height - top - bottom
    sizes = sorted({row["file_size_bytes"] for row in summaries})
    values = [row["median_throughput_kb_s_verified_successes"]
              for row in summaries if row["median_throughput_kb_s_verified_successes"] != ""]
    maximum = max(values, default=0) or 1
    x_pos = {size: left + chart_w * i / max(1, len(sizes) - 1)
             for i, size in enumerate(sizes)}
    colors = {"mini_tcp": "#1769aa", "kernel_tcp": "#e67e22"}
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="450" y="28" text-anchor="middle" font-family="sans-serif" font-size="20" font-weight="bold">Mini-TCP vs Kernel TCP Throughput</text>',
        f'<text x="450" y="52" text-anchor="middle" font-family="sans-serif" font-size="12">Localhost comparison; {html.escape(context)}; verified-success medians</text>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_h}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top + chart_h}" x2="{left + chart_w}" y2="{top + chart_h}" stroke="#333"/>',
        f'<text x="25" y="{top + chart_h / 2}" transform="rotate(-90 25 {top + chart_h / 2})" text-anchor="middle" font-family="sans-serif" font-size="14">Throughput (KB/s)</text>',
        f'<text x="{left + chart_w / 2}" y="{height - 18}" text-anchor="middle" font-family="sans-serif" font-size="14">File size (bytes)</text>',
    ]
    for protocol, offset in (("mini_tcp", -8), ("kernel_tcp", 8)):
        points = []
        for row in summaries:
            if row["protocol"] != protocol:
                continue
            value = row["median_throughput_kb_s_verified_successes"]
            if value == "":
                continue
            x = x_pos[row["file_size_bytes"]] + offset
            y = top + chart_h * (1 - value / maximum)
            points.append(f"{x:.1f},{y:.1f}")
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{colors[protocol]}"/>')
        if points:
            parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{colors[protocol]}" stroke-width="2"/>')
    for size, x in x_pos.items():
        parts.append(f'<text x="{x:.1f}" y="{top + chart_h + 24}" text-anchor="middle" font-family="sans-serif" font-size="12">{size:,} B</text>')
    parts.extend((
        f'<text x="{left + 30}" y="{height - 48}" fill="{colors["mini_tcp"]}" font-family="sans-serif" font-size="13">Mini-TCP</text>',
        f'<text x="{left + 150}" y="{height - 48}" fill="{colors["kernel_tcp"]}" font-family="sans-serif" font-size="13">Kernel TCP</text>',
        "</svg>",
    ))
    with _open_unique(path, "w", encoding="utf-8") as output:
        output.write("\n".join(parts))
        return Path(output.name)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=_parse_sizes, default=DEFAULT_SIZES)
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument("--csv", default="tcp_comparison_results.csv")
    parser.add_argument("--summary-csv", default="tcp_comparison_summary.csv")
    parser.add_argument("--plot", default="tcp_comparison_throughput.svg")
    parser.add_argument("--measurement-context",
                        default="localhost_tcp_comparison_external_conditions_unverified")
    args = parser.parse_args(argv)
    if args.repetitions < 1:
        parser.error("--repetitions must be at least one")

    rows = []
    congestion_events = []
    with tempfile.TemporaryDirectory(prefix="minitcp-tcp-compare-") as work_dir:
        mini_root = Path(work_dir) / "mini"
        tcp_root = Path(work_dir) / "tcp"
        mini_root.mkdir()
        tcp_root.mkdir()
        run_number = 0
        for size in args.sizes:
            if size < 0:
                parser.error("file sizes must be nonnegative")
            for _ in range(args.repetitions):
                run_number += 1
                rows.append({
                    "protocol": "mini_tcp",
                    **benchmark_one(
                        size, run_number, mini_root,
                        measurement_context=args.measurement_context,
                        congestion_events=congestion_events,
                    ),
                })
                rows.append(tcp_transfer(size, run_number, tcp_root,
                                         args.measurement_context))

    summaries = summarize(rows)
    individual_path = _write_csv(args.csv, FIELDS, rows)
    summary_path = _write_csv(args.summary_csv, SUMMARY_FIELDS, summaries)
    graph_path = write_svg(args.plot, summaries, args.measurement_context)
    event_path = write_congestion_csv(
        str(Path(args.csv).with_name("congestion_events.csv")),
        congestion_events,
    )
    cwnd_path = write_congestion_svg(
        str(Path(args.plot).with_name("congestion_window.svg")),
        congestion_events,
        args.measurement_context,
    )
    for row in summaries:
        median = row["median_throughput_kb_s_verified_successes"]
        text = f"{median:.2f} KB/s" if median != "" else "no verified samples"
        print(f"{row['protocol']} {row['file_size_bytes']:,} B: {text}; "
              f"success {row['successful_runs']}/{row['total_runs']}")
    print(f"Individual CSV: {individual_path}")
    print(f"Summary CSV: {summary_path}")
    print(f"Throughput graph: {graph_path}")
    print(f"Congestion events CSV: {event_path}")
    print(f"Congestion-window graph: {cwnd_path}")
    print(f"Measurement context: {args.measurement_context}")
    return 1 if any(not row["transfer_success"] for row in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
