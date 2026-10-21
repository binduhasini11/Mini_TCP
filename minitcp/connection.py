import socket
import time

from minitcp.packet import Packet, SYN, SYN_ACK, ACK
from minitcp.state import ConnectionState
from minitcp.transport import UDPTransport


class MiniTCPConnection:
    def __init__(self, transport: UDPTransport):
        self.transport = transport
        self.state = ConnectionState.CLOSED
        self.sequence_number = 0
        self.acknowledgement_number = 0
        self.peer_address = None

    def send_packet(self, packet: Packet, address):
        self.transport.send(packet.serialize(), address)

    def respond_to_retransmitted_syn_ack(self, packet, address):
        """Repeat this connection's final ACK for a duplicate server SYN-ACK."""
        if (
            self.state != ConnectionState.ESTABLISHED
            or address != self.peer_address
            or packet.packet_type != SYN_ACK
            or packet.sequence_number + 1 != self.acknowledgement_number
            or packet.acknowledgement_number != self.sequence_number + 1
            or packet.payload
        ):
            return False

        ack = Packet(
            packet_type=ACK,
            sequence_number=self.sequence_number + 1,
            acknowledgement_number=self.acknowledgement_number,
        )
        print("CLIENT: Re-sending final handshake ACK for duplicate SYN-ACK")
        self.send_packet(ack, self.peer_address)
        return True

    def receive_packet(self):
        data, address = self.transport.receive()
        packet = Packet.deserialize(data)
        return packet, address

    def connect(self, server_address):
        if self.state != ConnectionState.CLOSED:
            raise RuntimeError("Connection is not in CLOSED state")

        self.peer_address = server_address
        self.sequence_number = 100

        # Step 1: Send SYN
        syn = Packet(
            packet_type=SYN,
            sequence_number=self.sequence_number,
        )

        print("CLIENT: Sending SYN")
        self.send_packet(syn, server_address)

        self.state = ConnectionState.SYN_SENT
        print("CLIENT STATE:", self.state.value)

        # Step 2: Wait for a valid SYN-ACK
        while True:
            packet, address = self.receive_packet()

            if address != server_address:
                print("CLIENT: Ignoring packet from unexpected address")
                continue

            if packet.packet_type != SYN_ACK:
                print(
                    f"CLIENT: Ignoring unexpected packet "
                    f"type={packet.packet_type}"
                )
                continue

            if packet.acknowledgement_number != self.sequence_number + 1:
                print("CLIENT: Ignoring SYN-ACK with incorrect ACK number")
                continue

            break

        print("CLIENT: Received SYN-ACK")

        self.acknowledgement_number = packet.sequence_number + 1

        # Step 3: Send final ACK
        ack = Packet(
            packet_type=ACK,
            sequence_number=self.sequence_number + 1,
            acknowledgement_number=self.acknowledgement_number,
        )

        print("CLIENT: Sending ACK")
        self.send_packet(ack, server_address)

        self.state = ConnectionState.ESTABLISHED
        print("CLIENT STATE:", self.state.value)

    def accept(self, handshake_timeout=1.0, max_retries=5):
        if self.state != ConnectionState.CLOSED:
            raise RuntimeError("Connection is not in CLOSED state")
        if handshake_timeout <= 0:
            raise ValueError("handshake_timeout must be greater than zero")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        # Step 1: Wait for SYN
        print("SERVER: Waiting for SYN")

        while True:
            packet, address = self.receive_packet()

            if packet.packet_type == SYN:
                break

            print(
                f"SERVER: Ignoring unexpected packet "
                f"type={packet.packet_type} from {address}"
            )

        print("SERVER: Received SYN")

        self.peer_address = address
        self.acknowledgement_number = packet.sequence_number + 1

        self.state = ConnectionState.SYN_RECEIVED
        print("SERVER STATE:", self.state.value)

        # Step 2: Send SYN-ACK
        self.sequence_number = 500

        syn_ack = Packet(
            packet_type=SYN_ACK,
            sequence_number=self.sequence_number,
            acknowledgement_number=self.acknowledgement_number,
        )

        print("SERVER: Sending SYN-ACK")
        self.send_packet(syn_ack, self.peer_address)

        # Step 3: Wait for the final ACK, retransmitting SYN-ACK on timeout.
        # The per-wait timeout and retry count jointly impose a hard deadline.
        transport_socket = self.transport.socket
        previous_timeout = transport_socket.gettimeout()
        deadline = time.monotonic() + handshake_timeout * (max_retries + 1)
        retries = 0
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("handshake timed out waiting for final ACK")
                self.transport.set_timeout(min(handshake_timeout, remaining))
                try:
                    packet, address = self.receive_packet()
                except socket.timeout as exc:
                    if retries >= max_retries or time.monotonic() >= deadline:
                        raise TimeoutError(
                            "handshake timed out waiting for final ACK "
                            f"after {retries} SYN-ACK retransmissions"
                        ) from exc
                    print("SERVER: Final ACK timed out; retransmitting SYN-ACK")
                    self.send_packet(syn_ack, self.peer_address)
                    retries += 1
                    continue

                if time.monotonic() >= deadline:
                    raise TimeoutError("handshake deadline expired waiting for final ACK")
                if address != self.peer_address:
                    continue

                if (packet.packet_type != ACK or packet.payload
                        or packet.acknowledgement_number != self.sequence_number + 1
                        or packet.sequence_number != self.acknowledgement_number):
                    print("SERVER: Ignoring invalid final handshake packet")
                    continue
                break
        finally:
            self.transport.set_timeout(previous_timeout)

        print("SERVER: Received ACK")

        self.state = ConnectionState.ESTABLISHED
        print("SERVER STATE:", self.state.value)
