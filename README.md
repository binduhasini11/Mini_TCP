# Mini-TCP

**A Reliable Transport Protocol Built from Scratch over UDP**

Mini-TCP is a custom reliable transport protocol implemented on top of UDP to explore and reproduce core mechanisms of TCP at the application level.

The project progressively implements connection establishment, reliable data transfer, sliding-window flow control, adaptive retransmission, congestion control, and performance evaluation under controlled network impairments.

---

## Objectives

The primary objectives of Mini-TCP are to implement and evaluate:

* Custom packet format and protocol headers
* Connection establishment using a three-way handshake
* Explicit connection state machine
* Reliable data transfer over UDP
* Sequence numbers and cumulative acknowledgements
* Sliding-window protocol
* RTT estimation and adaptive retransmission
* Karn's algorithm
* TCP-style congestion control

  * Slow Start
  * AIMD Congestion Avoidance
  * Fast Retransmit
* File transfer under packet loss, delay, and reordering
* Performance comparison with kernel TCP

---

## Architecture

```text
                Mini-TCP
                   │
          ┌────────┴────────┐
          │                 │
       Sender            Receiver
          │                 │
          └────── UDP ──────┘
                   │
             Linux Network
                   │
              tc netem
          (loss / delay /
           reordering)
```

Mini-TCP operates above UDP and implements reliability and congestion-control mechanisms within the protocol itself.

---

## Current Implementation

### Phase 1 — Packet Layer

The initial packet layer has been implemented.

Current packet structure:

| Field                  |     Size |
| ---------------------- | -------: |
| Packet Type            |   1 byte |
| Sequence Number        |  4 bytes |
| Acknowledgement Number |  4 bytes |
| Payload Length         |  2 bytes |
| Payload                | Variable |

The packet layer supports:

* Packet serialization
* Packet deserialization
* Packet type identification
* Sequence numbering
* Acknowledgement numbering
* Variable-length payloads
* Payload-length validation

Implemented packet types:

```text
SYN
SYN_ACK
ACK
DATA
FIN
FIN_ACK
```

### Testing

Packet serialization and deserialization are currently verified using a dedicated test:

```bash
PYTHONPATH=. python3 tests/test_packet.py
```

The test confirms that a packet can be serialized into a byte stream and reconstructed without losing its protocol fields or payload.

---

## Protocol Flow

### Connection Establishment

Mini-TCP will use a three-way handshake similar to TCP:

```text
Client                         Server
  │                              │
  │ -------- SYN --------------> │
  │                              │
  │ <------ SYN + ACK ---------- │
  │                              │
  │ -------- ACK --------------> │
  │                              │
  │        ESTABLISHED           │
```

### Reliable Data Transfer

Data will be transmitted using sequence numbers and acknowledgements.

```text
Sender                         Receiver
  │                              │
  │ -------- DATA(seq) --------> │
  │                              │
  │ <--------- ACK ------------- │
  │                              │
```

Lost packets will eventually trigger retransmission.

---

## Project Structure
(sample-can evolve)
```text
Mini_TCP/
│
├── minitcp/
│   ├── __init__.py
│   ├── packet.py
│   ├── socket.py
│   ├── sender.py
│   ├── receiver.py
│   ├── connection.py
│   ├── state.py
│   └── constants.py
│
├── examples/
│   ├── server.py
│   └── client.py
│
├── tests/
│   └── test_packet.py
│
├── docs/
│
├── plots/
│
├── README.md
└── requirements.txt
```

---

## Network Emulation

Linux `tc netem` will be used to introduce controlled network conditions such as:

* Packet loss
* Network delay
* Packet reordering

Example:

```bash
sudo tc qdisc add dev lo root netem loss 10%
```

The impairment can be removed using:

```bash
sudo tc qdisc del dev lo root
```

---

## Technologies

* **Python 3**
* UDP sockets
* Linux networking
* `tc netem`
* Wireshark
* Matplotlib
* Git / GitHub

---

## Testing Strategy

Testing will progressively cover:

1. Packet encoding and decoding
2. UDP communication
3. Connection establishment
4. Reliable transmission
5. Retransmission under packet loss
6. Sliding-window behaviour
7. RTT estimation
8. Congestion-control behaviour
9. File integrity
10. Performance under network impairment

---

**Contributors:** 

B BINDU HASINI

A JERUBA CARLLIN

