# Mini-TCP — A Reliable Transport Protocol from Scratch

A simplified, TCP-inspired reliable transport protocol implemented over UDP in Python. Mini-TCP implements connection establishment, reliable and ordered data delivery, adaptive retransmission timeouts, congestion control, and verified binary file transfer.

**This is an educational implementation of selected TCP mechanisms, not a complete or standards-compliant TCP implementation.**

## Features

* **Packet framing:** Custom 11-byte header with sequence numbers, acknowledgements, and payload length.
* **Three-way handshake:** SYN → SYN-ACK → ACK, with connection-state tracking.
* **Reliable delivery:** Stop-and-wait and sliding-window transmission with cumulative ACKs, retransmissions, and out-of-order buffering.
* **Adaptive timeouts:** SRTT, RTTVAR, retransmission timeout (RTO), exponential backoff, and Karn's algorithm.
* **Congestion control:** Simplified TCP Reno-style slow start, AIMD congestion avoidance, fast retransmit, and fast recovery.
* **File integrity:** Binary file transfer with size checks and SHA-256 verification before the receiver publishes the file.
* **Performance evaluation:** Automated tests, throughput benchmarks, congestion-window plots, and comparison with native TCP.
* **Network impairment testing:** Optional Linux `tc netem` experiments with configured packet loss and delay.

## Architecture

```text
Application (text / binary files)
              |
       Mini-TCP Protocol
  +-----------+------------+
  |           |            |
Handshake  Reliability  Congestion
              |         Control
         RTT Estimation
              |
       Packet Framing
              |
          UDP Sockets
              |
        Network / netem
```

### Technology stack

Python 3 · UDP sockets · `unittest` · Linux `tc netem` · SHA-256

## Protocol Design

### Packet format

Each packet contains an **11-byte header** followed by an optional payload.

| Field                  |         Size |
| ---------------------- | -----------: |
| Packet type            |       1 byte |
| Sequence number        |      4 bytes |
| Acknowledgement number |      4 bytes |
| Payload length         |      2 bytes |
| **Total header**       | **11 bytes** |

The header uses Python's `struct` format `!BIIH`, with multi-byte fields in network byte order.

Supported packet types: `SYN`, `SYN_ACK`, `ACK`, `DATA`, `FIN`, `FIN_ACK`.

### Reliable transmission

Mini-TCP develops reliability in two stages:

1. **Stop-and-wait:** Sends one unit of data at a time and retransmits when an ACK does not arrive before the timeout.
2. **Sliding window:** Allows multiple segments in flight, uses cumulative ACKs, buffers out-of-order data, and retransmits lost segments.

The sender's effective window is constrained by both the configured sending window and the congestion window.

### RTT estimation and congestion control

* The RTT estimator maintains a smoothed RTT (SRTT) and RTT variation (RTTVAR) to calculate the retransmission timeout.
* Karn's algorithm avoids using ambiguous RTT samples from retransmitted segments.
* Exponential backoff increases the timeout after repeated failures.
* Simplified Reno-style congestion control uses slow start, additive increase/multiplicative decrease (AIMD), fast retransmit, and fast recovery.

### File transfer and integrity

The file-transfer application sends metadata containing the filename, file size, and SHA-256 digest, followed by the file bytes. The receiver checks the received size and digest before publishing the file and returns a verification status to the sender.

## Project Structure

```text
Mini_TCP/
├── minitcp/
│   ├── packet.py
│   ├── transport.py
│   ├── connection.py
│   ├── state.py
│   ├── reliability.py
│   ├── sliding_window.py
│   ├── rtt.py
│   └── congestion.py
├── examples/
│   ├── client.py
│   ├── server.py
│   ├── file_sender.py
│   └── file_receiver.py
├── tests/
├── benchmarks/
└── README.md
```

## Setup and Demo

**Requirements:** Python 3 and Ubuntu/Linux or WSL. The optional network-impairment experiments require `iproute2` (`tc`) and `sudo`.

Run commands from the repository root.

### 1. Start the file receiver

In Terminal 1:

```bash
cd ~/Mini_TCP
PYTHONPATH=. python3 examples/file_receiver.py received_files
```

### 2. Send a file

In Terminal 2:

```bash
cd ~/Mini_TCP
PYTHONPATH=. python3 examples/file_sender.py sample.bin
```

The receiver saves the file only after successful integrity verification. The sender reports the verification result.

### 3. Verify the file independently

```bash
sha256sum sample.bin received_files/sample.bin
```

Matching SHA-256 digests indicate that the source and received files have matching contents.

To create a sample file:

```bash
python3 -c 'from pathlib import Path; Path("sample.bin").write_bytes(bytes(range(256)) * 16)'
```

### 4. Run the automated tests

```bash
PYTHONPATH=. python3 -m unittest discover -s tests -v
```

The tests cover packet handling, connection establishment, reliability, sliding-window behaviour, RTT estimation, congestion control, file-transfer integrity, and benchmark utilities.

## Benchmarks

### Run Mini-TCP benchmarks

```bash
PYTHONPATH=. python3 benchmarks/benchmark_file_transfer.py
```

Measures file-transfer time and throughput for 4 KiB, 64 KiB, and 1 MiB payloads, with five repetitions by default. Successful transfers are checked using SHA-256.

### Compare Mini-TCP with native TCP

```bash
PYTHONPATH=. python3 benchmarks/compare_kernel_tcp.py
```

Compares file-transfer performance over localhost and generates result files and throughput plots.

### Run controlled network-impairment experiments

```bash
bash benchmarks/run_netem_file_transfer.sh \
  --loss 1 --delay 20 --reorder 0 --repetitions 5
```

This opt-in experiment requires `sudo` for Linux traffic-control operations. It affects loopback traffic while the impairment configuration is active; avoid unrelated loopback workloads during a run.

### Recorded results

The following medians were measured in one localhost experiment using 1% packet loss, 20 ms added delay, 0% reordering, and five repetitions per file size.

| File size |   Mini-TCP |    Native TCP |
| --------: | ---------: | ------------: |
|     4 KiB | 19.56 KB/s |    48.62 KB/s |
|    64 KiB | 74.53 KB/s |   524.61 KB/s |
|     1 MiB | 81.00 KB/s | 3,551.67 KB/s |

All 15 transfers for each transport completed with successful SHA-256 verification in that experiment.

**Interpretation:** Native TCP achieved higher throughput in the tested environment. Its mature, optimized kernel implementation outperformed this educational user-space Python implementation. These measurements are environment-specific, not universal performance guarantees.

The benchmark scripts generate CSV results and SVG plots. Exact output filenames may vary when files already exist.

## Limitations

* Not a complete or standards-compliant TCP implementation.
* Graceful connection teardown using FIN/FIN-ACK is not implemented.
* Sequence-number wraparound is not handled.
* One file is transferred per connection; multiplexing and authentication are not implemented.
* Network testing covers selected impairment configurations; broader loss, delay, and reordering experiments remain future work.
* No controlled TCP fairness experiment has been implemented.

## Contributors

* **B Bindu Hasini**
* **A Jeruba Carllin**

## Repository

[GitHub — binduhasini11/Mini_TCP](https://github.com/binduhasini11/Mini_TCP)
