import socket

from minitcp.connection import MiniTCPConnection
from minitcp.packet import ACK, DATA, Packet


class ReliableSender:
    """Stop-and-Wait reliable sender built on an established Mini-TCP connection."""

    def __init__(
        self,
        connection: MiniTCPConnection,
        timeout: float = 1.0,
        max_retries: int = 5,
    ):
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if connection.peer_address is None:
            raise RuntimeError("Connection has no peer address")

        self.connection = connection
        self.timeout = timeout
        self.max_retries = max_retries

        # The handshake consumes the initial sequence number.
        self.next_sequence_number = connection.sequence_number + 1

    def send(self, payload: bytes) -> None:
        """Reliably send one DATA segment using Stop-and-Wait ARQ."""

        if self.connection.state.value != "ESTABLISHED":
            raise RuntimeError("Connection is not established")

        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        sequence_number = self.next_sequence_number

        packet = Packet(
            packet_type=DATA,
            sequence_number=sequence_number,
            payload=payload,
        )

        for attempt in range(self.max_retries + 1):
            print(
                f"SENDER: Sending DATA seq={sequence_number} "
                f"(attempt {attempt + 1})"
            )

            self.connection.send_packet(
                packet,
                self.connection.peer_address,
            )

            try:
                self.connection.transport.set_timeout(self.timeout)

                while True:
                    response, address = self.connection.receive_packet()

                    # Ignore packets from an unexpected peer.
                    if address != self.connection.peer_address:
                        continue

                    if response.packet_type != ACK:
                        continue

                    expected_ack = sequence_number + len(payload)

                    if response.acknowledgement_number != expected_ack:
                        print(
                            f"SENDER: Ignoring ACK "
                            f"{response.acknowledgement_number}; "
                            f"expected {expected_ack}"
                        )
                        continue

                    print(
                        f"SENDER: Received ACK={expected_ack}"
                    )

                    self.next_sequence_number = expected_ack
                    self.connection.sequence_number = expected_ack

                    return

            except socket.timeout:
                if attempt >= self.max_retries:
                    raise TimeoutError(
                        f"DATA seq={sequence_number} failed after "
                        f"{self.max_retries + 1} attempts"
                    )

                print(
                    f"SENDER: Timeout waiting for ACK={expected_ack}; "
                    "retransmitting"
                )

        raise TimeoutError("Reliable transmission failed")


class ReliableReceiver:
    """Stop-and-Wait receiver for reliable Mini-TCP DATA packets."""

    def __init__(self, connection: MiniTCPConnection):
        if connection.peer_address is None:
            raise RuntimeError("Connection has no peer address")

        self.connection = connection

        # The handshake's acknowledgement number is the first
        # sequence number expected from the peer.
        self.expected_sequence_number = connection.acknowledgement_number

    def receive(self) -> bytes:
        """Receive and acknowledge one DATA segment."""

        if self.connection.state.value != "ESTABLISHED":
            raise RuntimeError("Connection is not established")

        while True:
            packet, address = self.connection.receive_packet()

            if address != self.connection.peer_address:
                continue

            if packet.packet_type != DATA:
                continue

            sequence_number = packet.sequence_number
            payload = packet.payload

            if sequence_number == self.expected_sequence_number:
                print(
                    f"RECEIVER: Received DATA seq={sequence_number}"
                )

                next_expected = sequence_number + len(payload)

                ack = Packet(
                    packet_type=ACK,
                    sequence_number=self.connection.sequence_number + 1,
                    acknowledgement_number=next_expected,
                )

                self.connection.send_packet(
                    ack,
                    self.connection.peer_address,
                )

                print(
                    f"RECEIVER: Sending ACK={next_expected}"
                )

                self.expected_sequence_number = next_expected
                self.connection.acknowledgement_number = next_expected

                return payload

            if sequence_number < self.expected_sequence_number:
                print(
                    f"RECEIVER: Duplicate DATA seq={sequence_number}; "
                    "resending ACK"
                )

                ack = Packet(
                    packet_type=ACK,
                    sequence_number=self.connection.sequence_number + 1,
                    acknowledgement_number=self.expected_sequence_number,
                )

                self.connection.send_packet(
                    ack,
                    self.connection.peer_address,
                )

                continue

            print(
                f"RECEIVER: Out-of-order DATA seq={sequence_number}; "
                f"expected {self.expected_sequence_number}"
            )

            ack = Packet(
                packet_type=ACK,
                sequence_number=self.connection.sequence_number + 1,
                acknowledgement_number=self.expected_sequence_number,
            )

            self.connection.send_packet(
                ack,
                self.connection.peer_address,
            )
