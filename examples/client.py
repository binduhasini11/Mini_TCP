from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection
from minitcp.reliability import ReliableSender


SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9001
CLIENT_HOST = "127.0.0.1"
CLIENT_PORT = 9000


def main():
    transport = UDPTransport(CLIENT_HOST, CLIENT_PORT)
    connection = MiniTCPConnection(transport)

    try:
        connection.connect((SERVER_HOST, SERVER_PORT))
        print("\nCLIENT: Connection established!")

        sender = ReliableSender(connection)

        print("\nType a message and press Enter.")
        print("Type /quit to exit.\n")

        while True:
            message = input("You: ")

            if message.strip() == "/quit":
                print("CLIENT: Closing application.")
                break

            if not message:
                print("Please enter a non-empty message.")
                continue

            payload = message.encode("utf-8")

            try:
                sender.send(payload)
                print("CLIENT: Message sent successfully.")

            except TimeoutError as error:
                print(f"CLIENT: Transmission failed: {error}")

    except (OSError, RuntimeError) as error:
        print(f"CLIENT ERROR: {error}")

    finally:
        transport.close()


if __name__ == "__main__":
    main()
