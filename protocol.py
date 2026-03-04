"""
TCP Network Protocol for communication between Master and Worker nodes.
Uses JSON serialization with length-prefixed framing.
"""

import json
import socket
import struct


HEARTBEAT_REQUEST = "__HEARTBEAT_PING__"


def send_message(sock: socket.socket, data: dict):
    """Send a JSON message over a socket with length-prefix framing."""
    payload = json.dumps(data).encode("utf-8")
    length = struct.pack(">I", len(payload))
    sock.sendall(length + payload)


def receive_message(sock: socket.socket) -> dict:
    """Receive a JSON message from a socket with length-prefix framing."""
    # Read 4-byte length header
    raw_length = _recv_exact(sock, 4)
    if not raw_length:
        raise ConnectionError("Connection closed while reading length header")
    length = struct.unpack(">I", raw_length)[0]

    # Read the payload
    raw_payload = _recv_exact(sock, length)
    if not raw_payload:
        raise ConnectionError("Connection closed while reading payload")
    return json.loads(raw_payload.decode("utf-8"))


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from a socket."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def connect_with_timeout(host: str, port: int, connect_timeout: float = 5.0,
                         read_timeout: float = 10.0) -> socket.socket:
    """Create a TCP connection with explicit connect and read timeouts."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(connect_timeout)
    sock.connect((host, port))
    sock.settimeout(read_timeout)
    return sock
