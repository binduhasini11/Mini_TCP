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

    def receive_packet(self):
        data, address = self.transport.receive()
        packet = Packet.deserialize(data)
        return packet, address

    def connect(self, server_address):
        self.peer_address = server_address
        self.sequence_number = 100

        syn = Packet(
            packet_type=SYN,
            sequence_number=self.sequence_number
        )

        print("CLIENT: Sending SYN")
        self.send_packet(syn, server_address)

        self.state = ConnectionState.SYN_SENT
        print("CLIENT STATE:", self.state.value)

        packet, address = self.receive_packet()

        if packet.packet_type != SYN_ACK:
            raise RuntimeError("Expected SYN-ACK")

        print("CLIENT: Received SYN-ACK")

        self.acknowledgement_number = packet.sequence_number + 1

        ack = Packet(
            packet_type=ACK,
            sequence_number=self.sequence_number + 1,
            acknowledgement_number=self.acknowledgement_number
        )

        print("CLIENT: Sending ACK")
        self.send_packet(ack, server_address)

        self.state = ConnectionState.ESTABLISHED
        print("CLIENT STATE:", self.state.value)

    def accept(self):
        print("SERVER: Waiting for SYN")

        packet, address = self.receive_packet()

        if packet.packet_type != SYN:
            raise RuntimeError("Expected SYN")

        print("SERVER: Received SYN")

        self.peer_address = address

        self.acknowledgement_number = packet.sequence_number + 1

        self.state = ConnectionState.SYN_RECEIVED
        print("SERVER STATE:", self.state.value)

        self.sequence_number = 500

        syn_ack = Packet(
            packet_type=SYN_ACK,
            sequence_number=self.sequence_number,
            acknowledgement_number=self.acknowledgement_number
        )

        print("SERVER: Sending SYN-ACK")
        self.send_packet(syn_ack, address)

        packet, address = self.receive_packet()

        if packet.packet_type != ACK:
            raise RuntimeError("Expected ACK")

        if packet.acknowledgement_number != self.sequence_number + 1:
            raise RuntimeError("Incorrect ACK number")

        print("SERVER: Received ACK")

        self.state = ConnectionState.ESTABLISHED
        print("SERVER STATE:", self.state.value)