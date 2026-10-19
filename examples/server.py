from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection
from minitcp.reliability import ReliableReceiver


SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9001


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

        receiver = ReliableReceiver(connection)

        while True:
            data = receiver.receive()

            try:
                message = data.decode("utf-8")
            except UnicodeDecodeError:
                message = repr(data)

            print(f"\nReceived message: {message}")

    except (OSError, RuntimeError) as error:
        print(f"SERVER ERROR: {error}")

    finally:
        transport.close()


if __name__ == "__main__":
    main()
