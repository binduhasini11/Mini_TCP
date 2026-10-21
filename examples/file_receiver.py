"""Run with: PYTHONPATH=. python3 examples/file_receiver.py [destination]"""
import sys

from minitcp.connection import MiniTCPConnection
from minitcp.file_transfer import FileTransferError, receive_file
from minitcp.sliding_window import SlidingWindowReceiver, SlidingWindowSender
from minitcp.transport import UDPTransport


def main():
    destination = sys.argv[1] if len(sys.argv) > 1 else "received_files"
    transport = UDPTransport("0.0.0.0", 9001)
    transport.bind()
    transport.set_timeout(3.0)
    connection = MiniTCPConnection(transport)
    try:
        print("Waiting for Mini-TCP file sender on UDP port 9001...")
        connection.accept()
        # A missing tail must fail instead of waiting forever.
        transport.set_timeout(30.0)
        receiver = SlidingWindowReceiver(connection, window_size=8,
                                         segment_size=512)
        status_sender = SlidingWindowSender(
            connection, window_size=8, segment_size=512,
            timeout=0.5, max_retries=3,
            incoming_data_handler=receiver.acknowledge_retransmitted_data,
        )
        path = receive_file(receiver, status_sender, destination)
        print(f"Verified and saved: {path}")
    except (OSError, RuntimeError, FileTransferError) as exc:
        print(f"File transfer failed: {exc}")
        raise SystemExit(1) from exc
    finally:
        transport.close()


if __name__ == "__main__":
    main()
