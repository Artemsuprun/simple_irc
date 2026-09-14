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

import json
import struct


# Protocol configuration
# "!II" means: network byte order, two unsigned integers (4 bytes each)
HEADER = "!II"
HEADER_SIZE = struct.calcsize(HEADER)

MAX_SIZE = 1024 # Change this value to adjust the maximum message size (including header)
MAX_DATA = MAX_SIZE - HEADER_SIZE

if MAX_SIZE <= HEADER_SIZE:
    raise ValueError(f"MAX_SIZE must be greater than {HEADER_SIZE} bytes to accommodate the header.")

# Commands:
# Client -> Server
# Room operations
CREATE_ROOM = 10
LIST_ROOMS = 11
JOIN_ROOM = 12
LEAVE_ROOM = 13
LIST_MEMBERS = 14
GET_ROOM_INFO = 15
GET_HISTORY = 16

# User operations
SET_USERNAME = 30
GET_SELF = 31
PING = 32
QUIT = 34

# Messaging
SEND_MESSAGE = 50

# Server -> Client
# Responses
PONG = 33
OK = 100
ERROR = 101
ROOM_MESSAGE = 200
ROOM_HISTORY = 201
USER_JOINED = 202
USER_LEFT = 203
USER_DISCONNECTED = 204

# quick access commands
COMMANDS = {
    CREATE_ROOM,
    LIST_ROOMS,
    JOIN_ROOM,
    LEAVE_ROOM,
    LIST_MEMBERS,
    GET_ROOM_INFO,
    GET_HISTORY,
    SET_USERNAME,
    GET_SELF,
    PING,
    QUIT,
    SEND_MESSAGE,
    PONG,
    OK,
    ERROR,
    ROOM_MESSAGE,
    ROOM_HISTORY,
    USER_JOINED,
    USER_LEFT,
    USER_DISCONNECTED,
}


# Protocol errors
class ProtocolError(Exception):
    """Raised when a malformed protocol message is received."""
    pass


# Message
class Msg:
    """Represents a single message exchanged between the client and server."""

    def __init__(self, cmd, data=None):
        """Initialize a message with a command and optional payload."""
        self.set_msg(cmd, data)

    @staticmethod
    def _encode_data(data):
        """Encode the JSON payload into UTF-8 bytes."""

        try:
            encoded = json.dumps(data, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ProtocolError("Payload is not JSON serializable") from exc

        if len(encoded) > MAX_DATA:
            raise ProtocolError(f"Message is too large. Max payload is {MAX_DATA} bytes.")

        return encoded

    @staticmethod
    def _decode_data(payload):
        """Decode UTF-8 bytes into a JSON payload."""

        # Decode the bytes to a UTF-8 text payload.
        try:
            text = bytes(payload).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("Payload is not valid UTF-8") from exc
        # Decode the text to json payload
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProtocolError("Payload is not valid JSON") from exc

        return data

    @staticmethod
    def _validate_command(cmd):
        """Validate command to ensure it's recognized by the protocol."""

        if cmd not in COMMANDS:
            raise ProtocolError(f"Unknown command: {cmd}")

    @classmethod
    def _validate_header(cls, cmd, data_len):
        """Validate command and payload length from a message header."""

        # validate cmd
        cls._validate_command(cmd)
        # validate max payload size
        if data_len > MAX_DATA:
            raise ProtocolError(f"Payload exceeds maximum size: {data_len} bytes")

    @property
    def data_len(self):
        """Return the JSON payload in bytes."""

        return len(self._encode_data(self.data))

    def set_msg(self, cmd, data=None):
        """Set and validate the command and payload."""

        # Reject unknown commands
        self._validate_command(cmd)
        # set an empty json input
        if data is None:
            data = {}

        # Validate the payload can be serialized and fits.
        _ = self._encode_data(data)

        self.cmd = cmd
        self.data = data

    def pack(self):
        """Serialize the message into bytes used for sending over a socket."""

        # Encode the json payload
        encoded = self._encode_data(self.data)
        # Pack the header with the command and payload length, then append the encoded payload
        header = struct.pack(HEADER, self.cmd, len(encoded))

        return header + encoded

    @classmethod
    def _from_parts(cls, cmd, data_len, payload):
        """Create a Msg instance from its components."""

        cls._validate_header(cmd, data_len)
        if len(payload) != data_len:
            raise ProtocolError(f"Payload length mismatch: header says {data_len}, got {len(payload)}")

        # Decode the bytes to a UTF-8 test payload.
        data = cls._decode_data(bytes(payload))

        return cls(cmd, data)

    # ----------- Blocking Method (Not in use, might remove entirely later) -------------
    @classmethod
    def recv(cls, sock):
        """Receive and deserialize exactly one message from a socket."""

        # TCP does not guarantee that a single recv call will return the entire message, 
        # so we must read the header first to know how many bytes to expect for the payload. 
        header = cls._recv_exact(sock, HEADER_SIZE)

        # Clean break from the socket (e.g., client disconnected) is represented by None.
        if header is None:
            return None

        # Unpack the header to get the command and payload length
        try:
            cmd, data_len = struct.unpack(HEADER, header)
        except struct.error as exc:
            raise ProtocolError("Invalid message header") from exc

        if data_len == 0:
            raise ProtocolError("JSON payload cannot be empty")

        # This will block until the entire payload is received or the connection is closed.
        # If the connection is closed before the entire payload is received, _recv_exact will
        # raise a ConnectionError, which will be handled by the caller.
        payload = cls._recv_exact(sock, data_len)
        if payload is None:
            raise ConnectionError("Connection closed before receiving the full payload.")

        return cls._from_parts(cmd, data_len, payload)

    # Socket helpers
    @staticmethod
    def _recv_exact(sock, size):
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


# Non-blocking message assembly
class MsgReader:
    """Assemble messages from a stream of bytes in a non-blocking manner."""

    def __init__(self):
        """Initialize an empty buffer for incoming bytes."""
        self._buf = bytearray()

    def feed(self, data):
        """Append newly-received raw bytes to the internal buffer."""
        self._buf.extend(data)

    def __iter__(self):
        """Return self to allow iteration over complete messages."""
        return self

    def __next__(self):
        """Return the next complete message from the buffer, or raise StopIteration if none are available."""
        if len(self._buf) < HEADER_SIZE:
            raise StopIteration

        cmd, data_len = struct.unpack(HEADER, self._buf[:HEADER_SIZE])

        # Validate before we decide how many more bytes to wait for, so a
        # corrupt/malicious header can't make us buffer forever.
        Msg._validate_header(cmd, data_len)

        total_size = HEADER_SIZE + data_len
        if len(self._buf) < total_size:
            raise StopIteration  # full payload hasn't arrived yet

        payload = bytes(self._buf[HEADER_SIZE:total_size])
        del self._buf[:total_size]  # consume this message, keep any leftover bytes

        return Msg._from_parts(cmd, data_len, payload)


# Non-blocking message sending for the client
class MsgWriter:
    """Buffer and send messages in a non-blocking manner."""

    def __init__(self):
        self._buf = bytearray()

    def queue(self, msg):
        """Append a Msg's wire bytes to the outgoing buffer."""
        self._buf.extend(msg.pack())

    @property
    def pending(self):
        """True if there are still unsent bytes buffered."""
        return len(self._buf) > 0

    def send(self, sock):
        """Attempt to send as many bytes as possible from the buffer to the socket."""
        if not self._buf:
            return True

        try:
            sent = sock.send(self._buf)
        except BlockingIOError:
            # Kernel send buffer is full right now; nothing went out.
            # Wait for the next write-ready notification and try again.
            return False

        del self._buf[:sent]  # drop only what actually made it out
        return len(self._buf) == 0
