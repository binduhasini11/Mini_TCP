from minitcp.transport import UDPTransport
from minitcp.connection import MiniTCPConnection
from minitcp.sliding_window import SlidingWindowSender

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

        sender = SlidingWindowSender(
            connection,
            window_size=4,
            timeout=1.0,
            max_retries=5,
            segment_size=8,
        )

        print("\nSliding-window mode enabled.")
        print("Window size: 4 segments")
        print("Segment size: 8 bytes")
        print("Type /quit to exit.\n")

        while True:
            message = input("You: ")

            if message.strip() == "/quit":
                print("CLIENT: Closing application.")
                break

            if not message:
                print("Please enter a non-empty message.")
                continue

            try:
                # Add a 4-byte header containing the message length.
                message_bytes = message.encode("utf-8")
                framed_message = (
                    len(message_bytes).to_bytes(4, byteorder="big")
                    + message_bytes
                )

                sender.send(framed_message)

                print("CLIENT: Message sent successfully.\n")

            except TimeoutError as error:
                print(f"CLIENT: Transmission failed: {error}")

    except (OSError, RuntimeError) as error:
        print(f"CLIENT ERROR: {error}")

    finally:
        transport.close()


if __name__ == "__main__":
    main()
