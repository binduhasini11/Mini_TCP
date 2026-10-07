from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection
from minitcp.reliability import ReliableSender


transport = UDPTransport("127.0.0.1", 9000)

connection = MiniTCPConnection(transport)

connection.connect(("127.0.0.1", 9001))

print("CLIENT: Connection established!")

sender = ReliableSender(connection)

sender.send(b"Hello from Mini-TCP!")

print("CLIENT: Data sent successfully!")

transport.close()
