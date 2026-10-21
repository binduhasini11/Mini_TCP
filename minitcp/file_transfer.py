"""Streaming file transfer over the existing Mini-TCP sliding window.

Wire format: 4-byte big-endian JSON header length, UTF-8 JSON metadata
(``name``, ``size``, ``sha256``), then exactly ``size`` file bytes. The
receiver replies with the same length-prefixed JSON framing and an ``ok``
flag after verification. This is a single-file, single-connection protocol.
"""

import hashlib
import json
import os
import socket
import tempfile
import time
from pathlib import Path


class FileTransferError(Exception):
    """Base class for transfer framing, completeness, and integrity errors."""


class IncompleteTransferError(FileTransferError):
    """Raised when the transport stalls before the declared byte count arrives."""


class IntegrityError(FileTransferError):
    """Raised when the received bytes do not match the advertised digest."""


def _send_record(sender, record):
    encoded = json.dumps(record, separators=(",", ":")).encode("utf-8")
    sender.send(len(encoded).to_bytes(4, "big") + encoded)


def _read_exact(receiver, size, deadline=None):
    """Read exact byte counts while retaining application framing leftovers."""
    if not hasattr(receiver, "_file_transfer_buffer"):
        receiver._file_transfer_buffer = bytearray()
    buffer = receiver._file_transfer_buffer
    while len(buffer) < size:
        try:
            buffer.extend(receiver.receive())
        except socket.timeout as exc:
            if deadline is not None and time.monotonic() < deadline:
                continue
            raise IncompleteTransferError(
                f"transfer stalled after {len(buffer)} of {size} requested bytes"
            ) from exc
    result = bytes(buffer[:size])
    del buffer[:size]
    return result


def _retry_deadline(sender):
    """Bound verification waiting to the sender's exponential retry schedule."""
    timeout = getattr(sender, "timeout", None)
    retries = getattr(sender, "max_retries", None)
    if timeout is None or retries is None:
        return None
    rto = timeout
    estimator = getattr(sender, "rtt_estimator", None)
    max_rto = getattr(estimator, "max_rto", rto)
    wait_budget = 0.0
    # Include each timeout through retry exhaustion, plus one initial RTO of
    # scheduling grace so the receiver does not close just before the sender
    # reports it.
    for _ in range(retries + 1):
        wait_budget += min(rto, max_rto)
        rto = min(rto * 2, max_rto)
    wait_budget += min(timeout, max_rto)
    return time.monotonic() + wait_budget


def send_file(sender, status_receiver, source_path, chunk_size=64 * 1024):
    """Send a file in bounded chunks and wait for verified receiver status."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    path = Path(source_path)
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
            size += len(chunk)
    _send_record(sender, {
        "name": path.name,
        "size": size,
        "sha256": digest.hexdigest(),
    })
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            sender.send(chunk)

    response_deadline = _retry_deadline(sender)
    response_size = int.from_bytes(
        _read_exact(status_receiver, 4, deadline=response_deadline), "big"
    )
    if response_size <= 0 or response_size > 4096:
        raise FileTransferError("invalid receiver status record length")
    try:
        response = json.loads(_read_exact(
            status_receiver, response_size, deadline=response_deadline
        ))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FileTransferError("invalid receiver status record") from exc
    if not response.get("ok"):
        raise IntegrityError(response.get("error", "receiver rejected file"))
    return response


def _safe_filename(name):
    if (not isinstance(name, str) or not name or name in {".", ".."}
            or "/" in name or "\\" in name or "\x00" in name
            or Path(name).name != name):
        raise FileTransferError("unsafe filename in transfer metadata")
    return name


def receive_file(receiver, response_sender, destination_dir, chunk_size=64 * 1024):
    """Receive one file, verify size and SHA-256, then atomically publish it."""
    destination = Path(destination_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    header_size = int.from_bytes(_read_exact(receiver, 4), "big")
    if header_size <= 0 or header_size > 16 * 1024:
        raise FileTransferError("invalid file metadata length")
    try:
        metadata = json.loads(_read_exact(receiver, header_size))
        name = _safe_filename(metadata["name"])
        size = metadata["size"]
        expected_digest = metadata["sha256"]
        if (not isinstance(size, int) or isinstance(size, bool) or size < 0
                or not isinstance(expected_digest, str)
                or len(expected_digest) != 64
                or any(c not in "0123456789abcdef" for c in expected_digest)):
            raise ValueError("invalid size or SHA-256")
    except (KeyError, TypeError, ValueError, UnicodeDecodeError,
            json.JSONDecodeError) as exc:
        raise FileTransferError(f"invalid file metadata: {exc}") from exc

    target = (destination / name).resolve()
    if target.parent != destination:
        raise FileTransferError("filename resolves outside destination directory")

    digest = hashlib.sha256()
    remaining = size
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=destination,
                                         prefix=".minitcp-", delete=False) as out:
            temp_name = out.name
            while remaining:
                block = _read_exact(receiver, min(chunk_size, remaining))
                out.write(block)
                digest.update(block)
                remaining -= len(block)
        actual = digest.hexdigest()
        if actual != expected_digest:
            _send_record(response_sender, {
                "ok": False,
                "error": f"SHA-256 mismatch (expected {expected_digest}, got {actual})",
            })
            raise IntegrityError("received file SHA-256 does not match metadata")
        os.replace(temp_name, target)
        temp_name = None
        _send_record(response_sender, {"ok": True, "name": name,
                                       "size": size, "sha256": actual})
        return target
    finally:
        if temp_name is not None:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass
