import socket
import time

from minitcp.connection import MiniTCPConnection
from minitcp.packet import ACK, DATA, Packet
from minitcp.rtt import RTTEstimator
from minitcp.congestion import CongestionController


class SlidingWindowSender:
    """
    Sliding-window reliable sender using byte-based sequence numbers.

    Supports cumulative ACKs, adaptive retransmission timeouts,
    Karn's algorithm, and exponential timeout backoff.
    """

    def __init__(
        self,
        connection: MiniTCPConnection,
        window_size: int = 4,
        timeout: float = 1.0,
        max_retries: int = 5,
        segment_size: int = 512,
    ):
        if connection.peer_address is None:
            raise RuntimeError("Connection has no peer address")

        if connection.state.value != "ESTABLISHED":
            raise RuntimeError("Connection is not established")

        if window_size < 1:
            raise ValueError("window_size must be at least 1")

        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if segment_size < 1:
            raise ValueError("segment_size must be at least 1")

        self.connection = connection
        self.window_size = window_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.segment_size = segment_size

        self.rtt_estimator = RTTEstimator(initial_rto=timeout)
        self.congestion_controller = CongestionController()

        self.base = connection.sequence_number + 1
        self.next_sequence_number = self.base

    def send(self, payload: bytes) -> None:
        """Send all bytes reliably using a sliding window."""
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        if not payload:
            return

        packets = []
        sequence = self.next_sequence_number

        for offset in range(0, len(payload), self.segment_size):
            chunk = payload[offset:offset + self.segment_size]

            packets.append(
                Packet(
                    packet_type=DATA,
                    sequence_number=sequence,
                    payload=chunk,
                )
            )
            sequence += len(chunk)

        first_sequence = packets[0].sequence_number
        final_sequence = sequence

        # Per segment:
        # packet, sent_at, retries, retransmitted
        outstanding = {}
        next_packet_index = 0
        acknowledged_until = first_sequence

        transport_socket = self.connection.transport.socket
        old_timeout = transport_socket.gettimeout()

        try:
            while acknowledged_until < final_sequence:

                # Fill the window with new segments.
                # Fill the window, respecting both configured capacity
                # and the congestion-control window.
                effective_window = min(
                    self.window_size,
                    max(1, int(self.congestion_controller.cwnd)),
                )

                while (
                    next_packet_index < len(packets)
                    and len(outstanding) < effective_window
                ):
                    packet = packets[next_packet_index]
                    now = time.monotonic()

                    print(
                        f"SENDER: Sending DATA "
                        f"SEQ={packet.sequence_number} "
                        f"| Length={len(packet.payload)} bytes"
                    )

                    self.connection.send_packet(
                        packet,
                        self.connection.peer_address,
                    )

                    outstanding[packet.sequence_number] = {
                        "packet": packet,
                        "sent_at": now,
                        "retries": 0,
                        "retransmitted": False,
                    }

                    next_packet_index += 1

                # Wait briefly, allowing us to notice ACKs and deadlines.
                self.connection.transport.set_timeout(0.05)

                try:
                    response, address = self.connection.receive_packet()
                except socket.timeout:
                    response = None
                    address = None

                now = time.monotonic()

                # Process valid cumulative ACKs.
                if (
                    response is not None
                    and address == self.connection.peer_address
                    and response.packet_type == ACK
                ):
                    ack_number = response.acknowledgement_number

                    highest_sent_end = max(
                        (
                            item["packet"].sequence_number
                            + len(item["packet"].payload)
                            for item in outstanding.values()
                        ),
                        default=acknowledged_until,
                    )

                    if acknowledged_until < ack_number <= highest_sent_end:

                        # Karn's algorithm:
                        # Take an RTT sample only if exactly one newly
                        # acknowledged segment was never retransmitted.
                        newly_acked = []

                        for seq, item in outstanding.items():
                            packet_end = (
                                item["packet"].sequence_number
                                + len(item["packet"].payload)
                            )

                            if (
                                acknowledged_until < packet_end <= ack_number
                            ):
                                newly_acked.append((seq, item))

                        eligible_samples = [
                            item
                            for _, item in newly_acked
                            if not item["retransmitted"]
                        ]

                        if len(newly_acked) == 1 and len(eligible_samples) == 1:
                            sample_item = eligible_samples[0]
                            measured_rtt = now - sample_item["sent_at"]

                            if measured_rtt > 0:
                                self.rtt_estimator.update(measured_rtt)
                                print(
                                    "SENDER: RTT sample="
                                    f"{measured_rtt:.4f}s | "
                                    f"New RTO={self.rtt_estimator.rto:.4f}s"
                                )

                        if newly_acked:
                            self.congestion_controller.on_new_ack(
                                len(newly_acked)
                            )

                        acknowledged_until = ack_number
                        self.base = ack_number

                        # Remove every fully acknowledged segment.
                        for seq in list(outstanding):
                            item = outstanding[seq]
                            packet_end = (
                                item["packet"].sequence_number
                                + len(item["packet"].payload)
                            )

                            if packet_end <= ack_number:
                                del outstanding[seq]

                        print(
                            f"SENDER: Received cumulative ACK={ack_number}"
                        )

                    elif ack_number == acknowledged_until:
                        if self.congestion_controller.on_duplicate_ack():
                            if outstanding:
                                earliest_seq = min(outstanding)
                                item = outstanding[earliest_seq]
                                print(
                                    f"SENDER: Fast Retransmit for SEQ={earliest_seq}"
                                )
                                self.connection.send_packet(
                                    item["packet"],
                                    self.connection.peer_address,
                                )
                                item["sent_at"] = time.monotonic()
                                item["retries"] += 1
                                item["retransmitted"] = True

                # Check each outstanding segment's adaptive deadline.
                now = time.monotonic()
                expired_packets = []

                for seq in sorted(outstanding):
                    item = outstanding[seq]

                    if now - item["sent_at"] >= self.rtt_estimator.rto:
                        expired_packets.append((seq, item))

                if expired_packets:
                    self.congestion_controller.on_timeout()

                    # Apply exponential backoff once per timeout event.
                    self.rtt_estimator.backoff()

                    print(
                        "SENDER: Exponential backoff; "
                        f"new RTO={self.rtt_estimator.rto:.4f}s"
                    )

                    for seq, item in expired_packets:
                        if item["retries"] >= self.max_retries:
                            raise TimeoutError(
                                f"DATA SEQ={seq} failed after "
                                f"{self.max_retries} retransmissions"
                            )

                        print(
                            f"SENDER: TIMEOUT for SEQ={seq}; retransmitting"
                        )

                        self.connection.send_packet(
                            item["packet"],
                            self.connection.peer_address,
                        )

                        item["sent_at"] = time.monotonic()
                        item["retries"] += 1
                        item["retransmitted"] = True
        finally:
            self.connection.transport.set_timeout(old_timeout)

        self.next_sequence_number = final_sequence
        self.connection.sequence_number = final_sequence

        print(
            f"SENDER: All {len(payload)} bytes acknowledged. "
            f"Next SEQ={self.next_sequence_number}"
        )


class SlidingWindowReceiver:
    """
    Sliding-window receiver with cumulative ACKs.

    Buffers out-of-order segments and delivers contiguous data in order.
    """

    def __init__(
        self,
        connection: MiniTCPConnection,
        window_size: int = 4,
        segment_size: int = 512,
    ):
        if connection.peer_address is None:
            raise RuntimeError("Connection has no peer address")

        if connection.state.value != "ESTABLISHED":
            raise RuntimeError("Connection is not established")

        if window_size < 1:
            raise ValueError("window_size must be at least 1")

        if segment_size < 1:
            raise ValueError("segment_size must be at least 1")

        self.connection = connection
        self.window_size = window_size
        self.segment_size = segment_size
        self.receive_window_bytes = window_size * segment_size

        self.expected_sequence_number = connection.acknowledgement_number

        self.buffer = {}
        self._ready = bytearray()

    def receive(self) -> bytes:
        """Return contiguous DATA while buffering out-of-order segments."""
        while not self._ready:
            packet, address = self.connection.receive_packet()

            if address != self.connection.peer_address:
                continue

            if packet.packet_type != DATA:
                continue

            seq = packet.sequence_number
            payload = packet.payload

            if not payload:
                self._send_ack()
                continue

            packet_end = seq + len(payload)

            # A fully received segment is a duplicate.
            if packet_end <= self.expected_sequence_number:
                print(
                    f"RECEIVER: Duplicate DATA SEQ={seq}; "
                    f"sending ACK={self.expected_sequence_number}"
                )
                self._send_ack()
                continue

            max_sequence = (
                self.expected_sequence_number + self.receive_window_bytes
            )

            # Reject data that begins before the receive point or extends
            # beyond the currently permitted receive window.
            if (
                seq < self.expected_sequence_number
                or packet_end > max_sequence
            ):
                print(
                    f"RECEIVER: DATA SEQ={seq} outside receive window; "
                    f"sending ACK={self.expected_sequence_number}"
                )
                self._send_ack()
                continue

            # Reject overlapping buffered segments to avoid duplicate bytes.
            overlaps = any(
                seq < buffered_seq + len(buffered_payload)
                and buffered_seq < packet_end
                for buffered_seq, buffered_payload in self.buffer.items()
            )

            if overlaps:
                print(
                    f"RECEIVER: Overlapping DATA SEQ={seq}; "
                    "discarding and sending cumulative ACK"
                )
                self._send_ack()
                continue

            if seq not in self.buffer:
                self.buffer[seq] = payload

            # Deliver any contiguous segments now available.
            while self.expected_sequence_number in self.buffer:
                chunk = self.buffer.pop(self.expected_sequence_number)
                self._ready.extend(chunk)
                self.expected_sequence_number += len(chunk)

            self._send_ack()

        data = bytes(self._ready)
        self._ready.clear()
        return data

    def _send_ack(self):
        ack = Packet(
            packet_type=ACK,
            sequence_number=self.connection.sequence_number,
            acknowledgement_number=self.expected_sequence_number,
        )

        print(
            f"RECEIVER: Sending cumulative "
            f"ACK={self.expected_sequence_number}"
        )

        self.connection.send_packet(
            ack,
            self.connection.peer_address,
        )
