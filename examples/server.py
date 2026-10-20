from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection
from minitcp.sliding_window import SlidingWindowReceiver

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9001


def read_exact(receiver, buffer, size):
    """Read exactly size bytes while preserving any extra received data."""
    while len(buffer) < size:
        buffer.extend(receiver.receive())

    result = bytes(buffer[:size])
    del buffer[:size]
    return result


def main():
    transport = UDPTransport(SERVER_HOST, SERVER_PORT)
    transport.bind()

    connection = MiniTCPConnection(transport)

    try:
        print("MINI-TCP SERVER")
        print("----------------")
        print(f"Listening on {SERVER_HOST}:{SERVER_PORT}")

        connection.accept()
        print("\nSERVER: Connection established!")

        receiver = SlidingWindowReceiver(
            connection,
            window_size=4,
            segment_size=8,
        )

        # Retain bytes that belong to the next read.
        receive_buffer = bytearray()

        while True:
            # Read the four-byte message length.
            header = read_exact(receiver, receive_buffer, 4)
            message_length = int.from_bytes(header, byteorder="big")

            if message_length == 0:
                print("SERVER: Received an empty message.")
                continue

            # Read the complete application message.
            data = read_exact(
                receiver,
                receive_buffer,
                message_length,
            )

            try:
                message = data.decode("utf-8")
            except UnicodeDecodeError:
                message = repr(data)

            print(f"\nReceived complete message: {message}")

    except (OSError, RuntimeError) as error:
        print(f"SERVER ERROR: {error}")

    finally:
        transport.close()


if __name__ == "__main__":
    main()
