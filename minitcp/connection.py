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

    def accept(self):
        if self.state != ConnectionState.CLOSED:
            raise RuntimeError("Connection is not in CLOSED state")

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

        # Step 3: Wait for the final ACK
        while True:
            packet, address = self.receive_packet()

            if address != self.peer_address:
                continue

            if packet.packet_type != ACK:
                print(
                    f"SERVER: Ignoring unexpected packet "
                    f"type={packet.packet_type}"
                )
                continue

            if packet.acknowledgement_number != self.sequence_number + 1:
                print(
                    "SERVER: Ignoring ACK with incorrect "
                    "acknowledgement number"
                )
                continue

            if packet.sequence_number != self.acknowledgement_number:
                print(
                    "SERVER: Ignoring ACK with incorrect sequence number"
                )
                continue

            break

        print("SERVER: Received ACK")

        self.state = ConnectionState.ESTABLISHED
        print("SERVER STATE:", self.state.value)
