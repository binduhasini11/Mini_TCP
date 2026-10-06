import struct
from dataclasses import dataclass


# Packet types
SYN = 1
SYN_ACK = 2
ACK = 3
DATA = 4
FIN = 5
FIN_ACK = 6


# Packet header:
# type (1 byte)
# sequence number (4 bytes)
# acknowledgement number (4 bytes)
# payload length (2 bytes)
HEADER_FORMAT = "!BIIH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


@dataclass
class Packet:
    packet_type: int
    sequence_number: int = 0
    acknowledgement_number: int = 0
    payload: bytes = b""

    def serialize(self) -> bytes:
        """Convert a Packet object into bytes for UDP transmission."""

        header = struct.pack(
            HEADER_FORMAT,
            self.packet_type,
            self.sequence_number,
            self.acknowledgement_number,
            len(self.payload),
        )

        return header + self.payload

    @classmethod
    def deserialize(cls, data: bytes):
        """Convert received UDP bytes back into a Packet object."""

        if len(data) < HEADER_SIZE:
            raise ValueError("Packet is too short")

        header = data[:HEADER_SIZE]
        payload = data[HEADER_SIZE:]

        (
            packet_type,
            sequence_number,
            acknowledgement_number,
            payload_length,
        ) = struct.unpack(HEADER_FORMAT, header)

        if len(payload) != payload_length:
            raise ValueError("Invalid payload length")

        return cls(
            packet_type=packet_type,
            sequence_number=sequence_number,
            acknowledgement_number=acknowledgement_number,
            payload=payload,
        )
