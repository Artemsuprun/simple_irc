# msg.py
# Artem Suprun
# 09/04/2026

"""
Message protocol for the IRC application.

Packet format:

    4 bytes: command
    4 bytes: payload length
    N bytes: UTF-8 encoded payload (N cannot exceed MAX_DATA)

All integers use network byte order (big endian).
"""

import struct


# Protocol configuration
# "!II" means: network byte order, two unsigned integers (4 bytes each)
HEADER = "!II"
HEADER_SIZE = struct.calcsize(HEADER)

MAX_SIZE = 1024 # Change this value to adjust the maximum message size (including header)
MAX_DATA = MAX_SIZE - HEADER_SIZE

if MAX_SIZE <= HEADER_SIZE:
    raise ValueError(f"MAX_SIZE must be greater than {HEADER_SIZE} bytes to accommodate the header.")

# Commands
CREATE = 10
ROOMS = 20
JOIN = 30
LEAVE = 40
MEMBERS = 50
MESSAGE = 60

COMMANDS = {
    CREATE,
    ROOMS,
    JOIN,
    LEAVE,
    MEMBERS,
    MESSAGE,
}


# Protocol errors
class ProtocolError(Exception):
    """Raised when a malformed protocol message is received."""
    pass


# Message
class Msg:
    """Represents a single message exchanged between the client and server."""

    def __init__(self, cmd, data=""):
        """Initialize a message with a command and optional payload."""
        self.set_msg(cmd, data)

    @property
    def data_len(self):
        """Return the payload size in bytes."""

        # len(self.data) returns the number of characters in the string, 
        # but we need the number of bytes when encoded in UTF-8.
        return len(self.data.encode("utf-8"))

    def set_msg(self, cmd, data=""):
        """Set and validate the command and payload."""

        # Reject unknown commands
        if cmd not in COMMANDS:
            raise ProtocolError(f"Unknown command: {cmd}")

        # encode the data to check its byte length
        encoded = data.encode("utf-8")
        if len(encoded) > MAX_DATA: 
            raise ProtocolError(f"Message is too large. Maximum payload is {MAX_DATA} bytes.")

        self.cmd = cmd
        self.data = data

    def pack(self):
        """Serialize the message into bytes used for sending over a socket."""

        # Encode the payload to bytes and check its length
        encoded = self.data.encode("utf-8")
        if len(encoded) > MAX_DATA:
            raise ProtocolError(f"Message is too large. Maximum payload is {MAX_DATA} bytes.")

        # Pack the header with the command and payload length, then append the encoded payload
        header = struct.pack(HEADER, self.cmd, len(encoded))

        return header + encoded

    @classmethod
    def recv(cls, sock):
        """Receive and deserialize exactly one message from a socket."""

        # TCP does not guarantee that a single recv call will return the entire message, 
        # so we must read the header first to know how many bytes to expect for the payload. 
        header = cls.recv_exact(sock, HEADER_SIZE)

        # Clean break from the socket (e.g., client disconnected) is represented by None.
        if header is None:
            return None

        # Unpack the header to get the command and payload length
        try:
            cmd, data_len = struct.unpack(HEADER, header)
        except struct.error as exc:
            raise ProtocolError("Invalid message header") from exc

        # Validate the command
        if cmd not in COMMANDS:
            raise ProtocolError(f"Unknown command: {cmd}")

        # Validate the payload length
        if data_len > MAX_DATA:
            raise ProtocolError(f"Payload exceeds maximum size: {data_len} bytes")
        if data_len == 0:
            return cls(cmd, "")

        # This will block until the entire payload is received or the connection is closed.
        # If the connection is closed before the entire payload is received, recv_exact will
        # raise a ConnectionError, which will be handled by the caller.
        payload = cls.recv_exact(sock, data_len)

        # Decode the payload from bytes to a UTF-8 string.
        try:
            data = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("Payload is not valid UTF-8") from exc

        return cls(cmd, data)


    # Socket helpers
    @staticmethod
    def recv_exact(sock, size):
        """Receive exactly 'size' bytes."""

        # The First call to recv() lets us know if the connection is closed. 
        # If it returns an empty bytes object, we return None to indicate 
        # that the connection was closed.
        first_chunk = sock.recv(size)
        if not first_chunk:
            return None  # Connection closed

        data = bytearray(first_chunk)

        # Keep receiving until we have the exact number of bytes requested
        while len(data) < size:
            chunk = sock.recv(size - len(data))

            # If the connection is closed before we receive the expected 
            # number of bytes, raise a ConnectionError.
            if not chunk:
                raise ConnectionError(f"Socket connection closed while receiving {len(data)} of {size} bytes")
            
            data.extend(chunk)

        # convert mutable bytearray to immutable bytes before returning
        return bytes(data)
