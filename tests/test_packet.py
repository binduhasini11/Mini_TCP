from minitcp.packet import Packet, DATA


packet = Packet(
    packet_type=DATA,
    sequence_number=100,
    acknowledgement_number=50,
    payload=b"Hello Mini-TCP",
)

print("Original packet:")
print(packet)

raw_data = packet.serialize()

print("\nSerialized bytes:")
print(raw_data)

received_packet = Packet.deserialize(raw_data)

print("\nDeserialized packet:")
print(received_packet)

assert received_packet.packet_type == DATA
assert received_packet.sequence_number == 100
assert received_packet.acknowledgement_number == 50
assert received_packet.payload == b"Hello Mini-TCP"

print("\n✅ Packet serialization test passed!")
