from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection
from minitcp.reliability import ReliableReceiver


transport = UDPTransport("127.0.0.1", 9001)
transport.bind()

connection = MiniTCPConnection(transport)

connection.accept()

print("SERVER: Connection established!")

receiver = ReliableReceiver(connection)

data = receiver.receive()

print("SERVER: Received:", data.decode())

transport.close()
