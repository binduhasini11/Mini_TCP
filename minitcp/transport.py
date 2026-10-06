import socket


class UDPTransport:
    def __init__(self, host: str, port: int):
        self.address = (host, port)

        self.socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

    def bind(self):
        self.socket.bind(self.address)

    def send(self, data: bytes, address):
        self.socket.sendto(data, address)

    def receive(self, buffer_size: int = 65535):
        return self.socket.recvfrom(buffer_size)

    def close(self):
        self.socket.close()