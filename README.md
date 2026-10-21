# Mini-TCP

A simplified reliable transport protocol implemented over UDP for studying transport-layer
mechanisms.

Mini-TCP provides packet framing, a three-way connection handshake, byte-based sequence numbers,
cumulative ACKs, sliding-window transmission, adaptive retransmission, simplified TCP-style
congestion control, and binary file transfer with SHA-256 verification. It is an educational
protocol, not a full TCP implementation.

## Features and architecture

The protocol library is under minitcp/. Applications and experiment tools are under examples/ and
benchmarks/.

~~~~text
Application: text or binary file framing
                    |
Mini-TCP: handshake, sequencing, ACKs, retransmission,
          sliding window, RTT estimation, congestion control
                    |
                 UDP socket
                    |
             Linux network path
             (optional tc netem)
~~~~

Implemented components:

- Fixed packet header encoding and validation.
- Client/server connection states and a three-message handshake.
- Cumulative ACKs and retransmission of unacknowledged data.
- RTT estimation, Karn's algorithm, and exponential timeout backoff.
- Slow start, AIMD congestion avoidance, duplicate-ACK fast retransmit, and fast recovery.
- Streaming, single-file binary transfer with receiver-side SHA-256 verification.
- Localhost benchmarks for Mini-TCP and a kernel-TCP comparison; optional, explicitly configured
  netem runs.

## Protocol design and implementation

A packet contains a one-byte type, 32-bit sequence number, 32-bit acknowledgement number, 16-bit
payload length, and variable-length payload. Multi-byte fields use network byte order. Packet types
are SYN, SYN_ACK, ACK, DATA, FIN, and FIN_ACK.

The handshake is:

~~~~text
Client                         Server
  SYN --------------------------->
      <-------------------- SYN_ACK
  ACK --------------------------->
  ESTABLISHED                 ESTABLISHED
~~~~

The server retransmits SYN_ACK while waiting for a valid final ACK, with bounded retries. If a
matching SYN_ACK is retransmitted while the client is sending DATA, the client repeats its final ACK
and performs a bounded recovery resend of outstanding DATA.

Connection states and the implemented handshake transitions:

~~~~mermaid
stateDiagram-v2
    [*] --> CLOSED
    CLOSED --> SYN_SENT : client sends SYN
    SYN_SENT --> ESTABLISHED : receives valid SYN_ACK; sends ACK
    CLOSED --> SYN_RECEIVED : server receives SYN; sends SYN_ACK
    SYN_RECEIVED --> SYN_RECEIVED : bounded SYN_ACK retransmission
    SYN_RECEIVED --> ESTABLISHED : receives valid final ACK
~~~~

SlidingWindowSender sends byte-sequenced DATA in a bounded window. The receiver buffers in-window
out-of-order segments, delivers contiguous bytes, and sends cumulative ACKs. The sender retransmits
expired segments and uses duplicate ACKs for fast retransmit. RTT samples are taken only for
eligible, never-retransmitted data (Karn's algorithm); the retransmission timeout backs off after a
timeout.

Public building blocks:

- Packet: minitcp.packet.Packet
- UDPTransport(host, port): bind(), send(), receive(), set_timeout(), close()
- MiniTCPConnection(transport): connect(server_address), accept(handshake_timeout=1.0,
  max_retries=5)
- SlidingWindowSender(connection, window_size=4, timeout=1.0, max_retries=5, segment_size=512):
  send(bytes)
- SlidingWindowReceiver(connection, window_size=4, segment_size=512): receive() returns contiguous
  bytes
- send_file(sender, status_receiver, source_path)
- receive_file(receiver, response_sender, destination_dir)

The initial SYN wait in accept() and the SYN_ACK wait in connect() are bounded by the
caller-configured socket timeout. The final-ACK wait in accept() has its own retry deadline.

## Binary file transfer and SHA-256 verification

The file-transfer format is a four-byte big-endian metadata length, UTF-8 JSON metadata (filename,
byte size, lowercase SHA-256 digest), then exactly the declared number of file bytes. The sender
streams file content through Mini-TCP. The receiver writes to a temporary file in the destination
directory, verifies the size and digest, and publishes the file only after verification. It then
sends a framed status response; the sender reports success only after receiving a positive
verification response.

Transfers support empty and binary files. Unsafe filenames are rejected, and the destination is
constrained to the selected directory. The implementation transfers one file per connection.

## Setup and usage

Requirements: Python 3 on Ubuntu. The unit tests, benchmarks, and SVG plots use the Python standard
library. The optional impairment script also requires iproute2 (tc) and sudo for qdisc operations.

From the repository root, start the receiver in one terminal:

~~~~bash
PYTHONPATH=. python3 examples/file_receiver.py received_files
~~~~

In a second terminal, send a file:

~~~~bash
PYTHONPATH=. python3 examples/file_sender.py path/to/source.bin
~~~~

The examples use UDP port 9001 for the receiver and port 9000 for the sender, connecting over
localhost. The receiver prints the saved path after size and SHA-256 verification; the sender prints
the verified digest.

Generate a binary sample and compare the received file:

~~~~bash
python3 -c 'from pathlib import Path; Path("sample.bin").write_bytes(bytes(range(256)) * 4096)'
stat -c '%n %s bytes' sample.bin received_files/sample.bin
sha256sum sample.bin received_files/sample.bin
~~~~

## Testing

Run the complete automated test suite from the repository root:

~~~~bash
PYTHONPATH=. python3 -m unittest discover -s tests -v
~~~~

Tests cover packet encoding, connection handshakes, sliding-window reliability, RTT estimation,
congestion control, file framing and integrity, retry behavior, benchmark calculations, CSV output,
and local transport integration. The suite includes deterministic local UDP tests for selected
packet-loss scenarios; passing unit tests do not establish performance under every network
impairment.

## Benchmark methodology and results

### Mini-TCP localhost benchmark

Run five repetitions for each default payload size (4 KiB, 64 KiB, and 1 MiB):

~~~~bash
PYTHONPATH=. python3 benchmarks/benchmark_file_transfer.py
~~~~

The benchmark measures the handshake, data transfer, and receiver verification response. Throughput
is calculated as file bytes divided by elapsed seconds and 1,024 bytes per KB. Each run generates
fresh data, checks source and destination SHA-256, records failures, and writes individual results
incrementally. Failed runs remain in the CSV; failed throughput is blank. Summary elapsed-time
medians include every attempt, while throughput medians include only verified successful transfers.

Default outputs are benchmark_results.csv, benchmark_summary.csv, and benchmark_throughput.svg.
Existing outputs are preserved; the benchmark selects an unused numbered filename when necessary. It
creates and removes its own temporary files and does not overwrite sample.bin or contents of
received_files/.

Useful options:

~~~~bash
PYTHONPATH=. python3 benchmarks/benchmark_file_transfer.py \
  --sizes 4096,65536,1048576 \
  --repetitions 5 \
  --measurement-context localhost_loopback_external_conditions_unverified \
  --csv benchmark_results.csv \
  --summary-csv benchmark_summary.csv \
  --plot benchmark_throughput.svg
~~~~

Verbose per-packet logs are suppressed by default. Add --log-file benchmark_debug.log to save them.
Add --cwnd-csv congestion_events.csv --cwnd-svg congestion_window.svg to record actual
congestion-controller events and plot the observed window. The ordinary benchmark does not inspect
or modify network settings. Its default label indicates that external conditions are unverified; do
not call a measurement an unimpaired baseline unless the path is known to be unimpaired.

### Mini-TCP versus kernel TCP

Run equivalent payload sizes over localhost:

~~~~bash
PYTHONPATH=. python3 benchmarks/compare_kernel_tcp.py
~~~~

The comparison creates fresh payloads of equal size for each transport, includes connection setup,
data transfer, and verification, and records individual results, summaries, throughput plots, and
Mini-TCP congestion events. Default outputs are tcp_comparison_results.csv,
tcp_comparison_summary.csv, tcp_comparison_throughput.svg, congestion_events.csv, and
congestion_window.svg. Existing files are preserved using unused numbered names.

This is a localhost throughput comparison, not a fairness experiment. Results depend on the host,
Python runtime, and measurement conditions; they are not universal performance guarantees. No
controlled TCP fairness experiment is implemented.

### Recorded results

The following are measured medians from the specific controlled run saved at
netem_results/netem_20261021T061054Z_8017/. That run used localhost tc-netem with 1% packet loss, 20
ms delay, 0% reordering, and five repetitions per size. All 15 Mini-TCP transfers and all 15
kernel-TCP transfers succeeded, and each transfer passed SHA-256 verification.

| File size | Mini-TCP median throughput | Kernel TCP median throughput |
| ---: | ---: | ---: |
| 4,096 B | 19.56 KB/s | 48.62 KB/s |
| 65,536 B | 74.53 KB/s | 524.61 KB/s |
| 1,048,576 B | 81.00 KB/s | 3,551.67 KB/s |

These values describe that test configuration and environment only. Ordinary localhost results and
netem-impaired results are separate measurements.

## Controlled tc netem experiments

The opt-in runner installs and verifies a loopback qdisc, runs the Mini-TCP/kernel-TCP comparison,
saves raw CSVs, plots, qdisc snapshots, and environment details under a unique directory in
netem_results/, then removes the qdisc if its configured state is still active.

~~~~bash
bash benchmarks/run_netem_file_transfer.sh \
  --loss 1 --delay 20 --reorder 0 --repetitions 5
~~~~

Options include --sizes BYTES,... and --output-root DIR; see:

~~~~bash
bash benchmarks/run_netem_file_transfer.sh --help
~~~~

The script requires sudo for tc qdisc inspection, installation, and cleanup because Linux restricts
qdisc changes to administrators. The benchmark subprocess itself runs without root privileges. The
script refuses to replace a non-default loopback root qdisc, verifies configured
loss/delay/reordering, records before/active/after state, and attempts cleanup on exit or
interruption. Netem affects loopback traffic while active; avoid unrelated loopback workloads during
a run. The ordinary benchmarks never configure netem.

## Limitations and future improvements

- This is a simplified educational transport, not TCP; packet format, state handling, and
  congestion control are limited.
- FIN and FIN_ACK constants exist, but protocol-level graceful connection teardown is not
  implemented.
- Sequence numbers are 32-bit and wraparound is not handled.
- A single connection transfers one file; there is no multiplexing or authentication.
- Handshake startup waits rely on configured socket timeouts.
- The TCP comparison measures localhost throughput only. Controlled TCP fairness is not implemented.
- Experiments cover selected configurations; further repeated tests across loss, delay, and
  reordering levels would improve evaluation confidence.

## Contributors

B BINDU HASINI

A JERUBA CARLLIN

