from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection


transport = UDPTransport("127.0.0.1", 9001)

transport.bind()

connection = MiniTCPConnection(transport)

connection.accept()

print("SERVER: Connection established!")

transport.close()