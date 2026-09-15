"""Shared helper for talking HTTP over a unix domain socket.

Used to query local snap daemons (MicroCeph, MicroCloud, ...) that expose
their control API on a unix socket instead of a TCP port.
"""

import http.client
import json
import socket


class UnixSocketError(Exception):
    """Raised when a unix-socket HTTP request fails."""


class _UnixSocketHTTPConnection(http.client.HTTPConnection):
    """A minimal HTTPConnection that talks over a unix domain socket."""

    def __init__(self, socket_path: str, timeout: float = 5.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        """Open an AF_UNIX socket to the daemon."""
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._socket_path)


def request_json(socket_path: str, method: str, path: str, timeout: float = 5.0) -> dict:
    """Perform an HTTP request against a unix socket and return the decoded JSON body.

    Raises
    ------
    UnixSocketError
        If the socket cannot be reached, the daemon returns an error status,
        or the response cannot be parsed as JSON.
    """
    try:
        conn = _UnixSocketHTTPConnection(socket_path, timeout=timeout)
        conn.request(method, path)
        response = conn.getresponse()
        body = response.read()
        conn.close()
    except OSError as exc:
        raise UnixSocketError(f"Could not reach socket {socket_path}: {exc}") from exc

    if response.status >= 400:
        raise UnixSocketError(
            f"{socket_path} returned HTTP {response.status}: {body.decode(errors='replace')}"
        )

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise UnixSocketError(f"Invalid JSON response from {socket_path}: {exc}") from exc
