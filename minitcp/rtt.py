import math


class RTTEstimator:
    """Estimate RTT and calculate an adaptive retransmission timeout."""

    def __init__(
        self,
        initial_rto: float = 1.0,
        min_rto: float = 0.1,
        max_rto: float = 60.0,
        clock_granularity: float = 0.01,
    ):
        if initial_rto <= 0:
            raise ValueError("initial_rto must be positive")
        if min_rto <= 0:
            raise ValueError("min_rto must be positive")
        if max_rto < min_rto:
            raise ValueError("max_rto must be >= min_rto")
        if clock_granularity <= 0:
            raise ValueError("clock_granularity must be positive")

        self.min_rto = min_rto
        self.max_rto = max_rto
        self.clock_granularity = clock_granularity

        self.srtt = None
        self.rttvar = None
        self.rto = min(max(initial_rto, min_rto), max_rto)

    def update(self, measured_rtt: float) -> float:
        """Update estimates using a valid RTT sample, in seconds."""
        if measured_rtt <= 0 or not math.isfinite(measured_rtt):
            raise ValueError("measured_rtt must be finite and positive")

        if self.srtt is None:
            self.srtt = measured_rtt
            self.rttvar = measured_rtt / 2
        else:
            self.rttvar = (
                0.75 * self.rttvar
                + 0.25 * abs(self.srtt - measured_rtt)
            )
            self.srtt = (
                0.875 * self.srtt
                + 0.125 * measured_rtt
            )

        calculated_rto = (
            self.srtt
            + max(
                self.clock_granularity,
                4 * self.rttvar,
            )
        )

        self.rto = min(
            self.max_rto,
            max(self.min_rto, calculated_rto),
        )
        return self.rto

    def backoff(self) -> float:
        """Double the timeout after a timeout event."""
        self.rto = min(self.rto * 2, self.max_rto)
        return self.rto
