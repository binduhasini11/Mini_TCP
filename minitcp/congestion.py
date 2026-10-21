from enum import Enum
import time


class CongestionState(str, Enum):
    SLOW_START = "slow_start"
    CONGESTION_AVOIDANCE = "congestion_avoidance"
    FAST_RECOVERY = "fast_recovery"


class CongestionController:
    """
    Simplified TCP Reno-style congestion controller.

    Window sizes are measured in segment equivalents.
    This class manages congestion-control decisions;
    SlidingWindowSender will perform actual transmissions.
    """

    def __init__(
        self,
        initial_cwnd: float = 1.0,
        initial_ssthresh: float = 8.0,
        duplicate_ack_threshold: int = 3,
        min_ssthresh: float = 2.0,
        event_callback=None,
    ):
        if initial_cwnd < 1:
            raise ValueError("initial_cwnd must be at least 1")

        if initial_ssthresh <= 0:
            raise ValueError("initial_ssthresh must be positive")

        if min_ssthresh < 1:
            raise ValueError("min_ssthresh must be at least 1")

        if duplicate_ack_threshold < 1:
            raise ValueError(
                "duplicate_ack_threshold must be at least 1"
            )

        self.cwnd = float(initial_cwnd)
        self.ssthresh = float(initial_ssthresh)
        self.min_ssthresh = float(min_ssthresh)

        self.duplicate_ack_threshold = duplicate_ack_threshold
        self.duplicate_ack_count = 0
        self.event_callback = event_callback
        self._started_at = time.monotonic()

        if self.cwnd < self.ssthresh:
            self.state = CongestionState.SLOW_START
        else:
            self.state = CongestionState.CONGESTION_AVOIDANCE
        self._record_event("initial")

    def _record_event(self, event):
        """Emit an optional structured observation without changing control logic."""
        if self.event_callback is not None:
            self.event_callback({
                "elapsed_seconds": time.monotonic() - self._started_at,
                "event": event,
                "state": self.state.value,
                "cwnd_segments": self.cwnd,
                "ssthresh_segments": self.ssthresh,
                "duplicate_ack_count": self.duplicate_ack_count,
            })

    def on_new_ack(self, newly_acked_segments: int = 1) -> None:
        """
        Increase cwnd when new data has been acknowledged.
        """

        if newly_acked_segments < 1:
            raise ValueError(
                "newly_acked_segments must be at least 1"
            )

        self.duplicate_ack_count = 0

        # A new ACK exits Fast Recovery.
        if self.state == CongestionState.FAST_RECOVERY:
            self.cwnd = self.ssthresh
            self.state = CongestionState.CONGESTION_AVOIDANCE
            self._record_event("new_ack_exit_fast_recovery")

            print(
                "CONGESTION: Exiting fast recovery | "
                f"cwnd={self.cwnd:.2f}"
            )
            return

        for _ in range(newly_acked_segments):

            if self.state == CongestionState.SLOW_START:
                # Simplified Slow Start:
                # increase cwnd by one segment per
                # newly acknowledged segment.
                self.cwnd += 1.0

                if self.cwnd >= self.ssthresh:
                    self.state = (
                        CongestionState.CONGESTION_AVOIDANCE
                    )
                self._record_event("new_ack")

            else:
                # Additive increase:
                # approximately one segment per RTT.
                self.cwnd += 1.0 / self.cwnd

        print(
            f"CONGESTION: {self.state.value} | "
            f"cwnd={self.cwnd:.2f} | "
            f"ssthresh={self.ssthresh:.2f}"
        )

    def on_duplicate_ack(self) -> bool:
        """
        Process a duplicate ACK.

        Returns True exactly when Fast Retransmit should
        be triggered. The sender must perform the actual
        retransmission.
        """

        if self.state == CongestionState.FAST_RECOVERY:
            # Further duplicate ACKs indicate that
            # additional data has left the network.
            self.cwnd += 1.0
            self._record_event("duplicate_ack_fast_recovery")

            print(
                "CONGESTION: Additional duplicate ACK | "
                f"cwnd={self.cwnd:.2f}"
            )
            return False

        self.duplicate_ack_count += 1

        print(
            "CONGESTION: Duplicate ACK "
            f"{self.duplicate_ack_count}/"
            f"{self.duplicate_ack_threshold}"
        )
        self._record_event("duplicate_ack")

        if (
            self.duplicate_ack_count
            < self.duplicate_ack_threshold
        ):
            return False

        # Fast Retransmit threshold reached.
        self.ssthresh = max(
            self.cwnd / 2.0,
            self.min_ssthresh,
        )

        self.cwnd = (
            self.ssthresh + self.duplicate_ack_threshold
        )

        self.state = CongestionState.FAST_RECOVERY
        self._record_event("fast_retransmit")

        print(
            "CONGESTION: Fast Retransmit triggered | "
            f"ssthresh={self.ssthresh:.2f} | "
            f"cwnd={self.cwnd:.2f}"
        )

        return True

    def on_timeout(self) -> None:
        """
        Treat a retransmission timeout as a congestion signal.
        """

        self.ssthresh = max(
            self.cwnd / 2.0,
            self.min_ssthresh,
        )

        self.cwnd = 1.0
        self.state = CongestionState.SLOW_START
        self.duplicate_ack_count = 0
        self._record_event("timeout")

        print(
            "CONGESTION: Timeout | "
            f"ssthresh={self.ssthresh:.2f} | "
            f"cwnd={self.cwnd:.2f} | "
            f"state={self.state.value}"
        )
