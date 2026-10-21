"""Run with: PYTHONPATH=. python3 examples/file_sender.py SOURCE_FILE"""
import sys

from minitcp.connection import MiniTCPConnection
from minitcp.file_transfer import FileTransferError, send_file
from minitcp.sliding_window import SlidingWindowReceiver, SlidingWindowSender
from minitcp.transport import UDPTransport


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 examples/file_sender.py SOURCE_FILE")
    transport = UDPTransport("0.0.0.0", 9000)
    transport.bind()
    transport.set_timeout(3.0)
    connection = MiniTCPConnection(transport)
    try:
        connection.connect(("127.0.0.1", 9001))
        # Bound the wait for the receiver's verification result.
        transport.set_timeout(30.0)
        transport.set_timeout(0.5)
        sender = SlidingWindowSender(connection, window_size=8,
                                     segment_size=512, timeout=0.5,
                                     max_retries=3)
        status_receiver = SlidingWindowReceiver(connection, window_size=8,
                                                segment_size=512)
        result = send_file(sender, status_receiver, sys.argv[1])
        print(f"Receiver verified {result['name']} ({result['size']} bytes)")
        print(f"SHA-256: {result['sha256']}")
    except (OSError, RuntimeError, FileTransferError) as exc:
        print(f"File transfer failed: {exc}")
        raise SystemExit(1) from exc
    finally:
        transport.close()


if __name__ == "__main__":
    main()
