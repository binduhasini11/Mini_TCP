from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection


transport = UDPTransport("127.0.0.1", 9000)

connection = MiniTCPConnection(transport)

connection.connect(("127.0.0.1", 9001))

print("CLIENT: Connection established!")

transport.close()