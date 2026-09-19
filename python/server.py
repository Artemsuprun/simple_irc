# server.py
# Artem Suprun
# 09/04/2026

"""
IRC server.

Uses:
    - TCP sockets
    - selectors for event-driven I/O
    - custom binary message protocol
    - JSON message payloads
"""

import logging
import selectors
import socket
from collections import deque
from datetime import datetime, timezone

import msg


# Configuration
HOST = "0.0.0.0"
PORT = 12345

MAX_HISTORY = 10
MAX_USERNAME_LENGTH = 32
MAX_ROOM_NAME_LENGTH = 32
ALLOW_CHANGE_USERNAME = False


# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Client
class Client:
    """Represents one connected client"""

    def __init__(self, sock, address):
        # user network details
        self.sock = sock
        self.address = address

        self.username = None

        self.rooms = set()

        # Use non-blocking reader and writer buffers to manage incoming and outgoing messages
        self.reader = msg.MsgReader()
        self.writer = msg.MsgWriter()

    @property
    def identifier(self):
        """Return a useful identifier for logging."""

        if self.username:
            return self.username
        return f"{self.address[0]}:{self.address[1]}"

    @property
    def is_registered(self):
        """Checks if the user has selected a username."""

        return self.username is not None

    def send(self, cmd, data=None):
        """Queue a message for the client."""

        self.writer.queue(msg.Msg(cmd, data))

    def __repr__(self):
        return f"<Client {self.identifier}>"
    


# Room
class Room:
    """Represents a chat room."""
    
    def __init__(self, name):
        self.name = name

        # store client objects
        self.members = set()

        # Most recent message
        self.history = deque(maxlen=MAX_HISTORY)

    def add_member(self, client):
        """Add client to the room."""

        self.members.add(client)
        client.rooms.add(self.name)

    def remove_member(self, client):
        """Remove client from the room."""

        self.members.discard(client)
        client.rooms.discard(self.name)

    def has_member(self, client):
        """Check if a client is a member of the room."""

        return client in self.members

    def add_history(self, msg):
        """Store a message in the room history."""

        self.history.append(msg)

    def get_history(self):
        """Return room history as a list."""

        return list(self.history)

    def __len__(self):
        return len(self.members)

    def __repr__(self):
        return f"<Room {self.name} members={len(self.members)}>"



# Server
class IRCServer:
    """Main event-driven IRC server."""

    def __init__(self, host=HOST, port=PORT):
        # server host and port
        self.host = host
        self.port = port

        # Use the default selector for event-driven I/O
        self.selector = selectors.DefaultSelector()

        self.server_socket = None
        # Track whether the server is running
        self.running = False

        # Track connected clients and active rooms
        self.clients = {} # socket -> Client
        self.rooms = {}   # room name -> Room
        self.users = {}   # usernames -> Client

    # Server lifecycle
    def start(self):
        """Start the server."""

        try:
            # Create a TCP socket, bind it to the host and port, and listen for incoming connections.
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set socket options to allow reusing the address to avoid "Address already in use" errors on restart.
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind( (self.host, self.port) )
            # Start listening for incoming connections
            self.server_socket.listen()
            # Set the server socket to non-blocking mode so selector can manage it without blocking the main thread.
            self.server_socket.setblocking(False)

            # Register the server socket with the selector to handle incoming connections.
            self.selector.register(self.server_socket, selectors.EVENT_READ, data=None)

            self.running = True

            logger.info("IRC server listening on %s:%s", self.host, self.port)

            # Run the main event loop in a try/finally block to ensure graceful shutdown on exit.
            # Note: We catch KeyboardInterrupt to allow the server to be stopped with Ctrl+C,
            # and log any unexpected exceptions. final block ensures that shutdown is called to 
            # clean up resources. Might add signal handling in the future for more graceful shutdowns.
            self.run()
        except KeyboardInterrupt: # Handle Ctrl+C gracefully to stop the server.
            logger.info("Server interrupted by user.")
        except Exception:
            logger.exception("Unexpected error occurred. Shutting down server.")
            raise
        finally:
            self.shutdown()

    def run(self):
        """Main event loop."""

        while self.running:
            events = self.selector.select(timeout=1)

            for key, mask in events:
                if key.data is None:
                    self.accept_client()
                else:
                    client = key.data
                    self.handle_client(client, mask)

    def shutdown(self):
        """Gracefully shut down the server."""

        # If the server is already stopped, do nothing. Don't attempt cleanup again.
        if not self.running and self.server_socket is None:
            return
        logger.info("Shutting down server...")
        self.running = False

        # Disconnect all clients and close their sockets
        for client in list(self.clients.values()):
            self.disconnect(client)

        # Close the server socket and unregister it from the selector
        if self.server_socket:
            try:
                self.selector.unregister(self.server_socket)
            except Exception:
                pass
            self.server_socket.close()
            self.server_socket = None
        # Close the selector to free up resources
        self.selector.close()

        logger.info("Server stopped.")

    # Client handling
    def accept_client(self):
        """Accept a new client connection."""

        # Accept the incoming connection and get the client socket and address.
        try:
            client_socket, address = self.server_socket.accept()
        except BlockingIOError:
            return
        # Set the client socket to non-blocking so that the selector can manage it.
        client_socket.setblocking(False)
        # Create a new Client instance to track the connected client.
        client = Client(client_socket, address)
        # Store the client in the clients dictionary using the socket as the key.
        self.clients[client_socket] = client
        # Add the client socket to the selector.
        self.selector.register(client_socket, selectors.EVENT_READ, data=client)

        logger.info("Client connected: %s", address)

    def handle_client(self, client, mask):
        """Handle read/write readiness for a client."""

        if mask & selectors.EVENT_READ:
            self.read_client(client)

        # Client disconnects during read handling
        if client.sock not in self.clients:
            return

        if mask & selectors.EVENT_WRITE:
            self.write_client(client)

    def read_client(self, client):
        """Read available bytes and process complete messages."""

        # Use the MsgReader to read and assemble a complete message from the socket. 
        try:
            chunk = client.sock.recv(msg.MAX_SIZE * 4)

        except ConnectionResetError:
            self.disconnect(client)
            return
        
        except OSError as exc:
            logger.warning("Socket read failed for %s: %s", client, exc)
            self.disconnect(client)
            return

        # If the socket is closed, disconnect the client.
        if not chunk:
            self.disconnect(client)
            return

        # Feed the received chunk to the MsgReader to assemble a complete message.
        client.reader.feed(chunk)
        try:
            # Process all complete messages that have been assembled from the incoming data.
            for message in client.reader:
                self.handle_message(client, message)

        except msg.ProtocolError as exc:
            logger.warning("Protocol error from %s: %s", client, exc)
            self.send_error(client, str(exc))
        return
    
    def write_client(self, client):
        """Send buffered outgoing bytes."""

        try:
            is_done = client.writer.send(client.sock)

        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            logger.warning("Socket write failed for %s: %s", client, exc)
            self.disconnect(client)
            return

        if is_done:
            self.set_events(client, read=True, write=False)

    def set_events(self, client, read=True, write=False):
        """Update selector events for a client socket."""

        events = 0
        if read:
            events |= selectors.EVENT_READ
        if write:
            events |= selectors.EVENT_WRITE

        self.selector.modify(client.sock, events, data=client)

    def queue_message(self, client, cmd, data=None):
        """Queue a message and enable write readiness."""

        client.send(cmd, data)
        self.set_events(client, read=True, write=True)

    def broadcast_to_room(self, room, cmd, data, exclude=None):
        """Send a message to everyone part of a room."""

        for client in room.members:
            if client is not exclude:
                self.queue_message(client, cmd, data)

    def disconnect(self, client):
        """Remove the client from the server and all rooms."""

        # if the client doesn't exist, exit
        if client.sock not in self.clients:
            return

        logger.info("Disconnecting %s", client)

        # remove username from users
        if self.users.get(client.username) is client:
            del self.users[client.username]

        # Inform the room the user left
        for room_name in list(client.rooms):
            room = self.rooms.get(room_name)
            if room is None:
                continue

            self.broadcast_to_room(
                room,
                msg.USER_DISCONNECTED,
                {
                    "username": client.username,
                    "room": room.name,
                },
                exclude=client
            )
            room.remove_member(client)

            # if no more members, remove room.
            self.remove_empty_room(room)

        # remove from selector
        try:
            self.selector.unregister(client.sock)
        except (KeyError, ValueError):
            pass
        # close socket 
        try:
            client.sock.close()
        except OSError:
            pass

        del self.clients[client.sock]

    # Message handling
    def handle_message(self, client, message):
        """Send the message to the appropriate handler based on its command."""

        handlers = {
            msg.CREATE_ROOM: self.handle_create_room,
            msg.LIST_ROOMS: self.handle_list_rooms,
            msg.JOIN_ROOM: self.handle_join_room,
            msg.LEAVE_ROOM: self.handle_leave_room,
            msg.LIST_MEMBERS: self.handle_list_members,
            msg.GET_ROOM_INFO: self.handle_get_room_info,
            msg.GET_HISTORY: self.handle_get_history,

            msg.SET_USERNAME: self.handle_set_username,
            msg.GET_SELF: self.handle_get_self,
            msg.PING: self.handle_ping,
            msg.QUIT: self.handle_quit,

            msg.SEND_MESSAGE: self.handle_send_message,
        }

        handler = handlers.get(message.cmd)

        # If no handler is found for the command, send an error message back to the client.
        if handler is None:
            self.send_error(client, "Unknown command.")
            return
        
        # Call the handler function with the client and message data.
        handler(client, message.data)

    @staticmethod
    def require_dict(data):
        """Ensure the payload is a JSON object."""

        if not isinstance(data, dict):
            raise ValueError("Payload must be a JSON object.")
        return data

    @staticmethod
    def require_field(data, field):
        """Retrieve a required field."""

        value = data.get(field)

        if value is None:
            raise ValueError(f"Missing required field: {field}")
        return value

    def get_room(self, data):
        """Retrieve a room from a payload."""

        room_name = self.require_field(data, "room")

        if not isinstance(room_name, str):
            raise ValueError("Room name must be a string")

        room = self.rooms.get(room_name)
        if room is None:
            raise ValueError("Room does not exist.")

        return room

    def send_ok(self, client, data=None):
        """Send a successful response."""

        if data is None:
            data = {}
        self.queue_message(client, msg.OK, data)

    def send_error(self, client, error):
        """Send an error message."""

        self.queue_message(
            client, 
            msg.ERROR,
            {
                "error": error,
            }
        )
    
    def remove_empty_room(self, room):
        """Removes a room if it has no members."""

        if len(room) <= 0:
            self.rooms.pop(room.name, None)

    def handle_create_room(self, client, data):
        """Create a new room."""

        try:
            data = self.require_dict(data)
            room_name = self.require_field(data, "room")

            if not isinstance(room_name, str):
                raise ValueError("Room name must be a string")
            if not room_name:
                raise ValueError("Room name cannot be empty")
            if len(room_name) > MAX_ROOM_NAME_LENGTH:
                raise ValueError("Room name is too long")
            if room_name in self.rooms:
                raise ValueError("Room already exists")

            self.rooms[room_name] = Room(room_name)

            self.send_ok(
                client,
                {
                    "room": room_name,
                }
            )

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_list_rooms(self, client, data):
        """Return all available rooms."""

        self.send_ok(
            client,
            {
                "rooms": list(self.rooms.keys()),
            }
        )

    def handle_join_room(self, client, data):
        """Join an existing room."""

        try:
            data = self.require_dict(data)
            room = self.get_room(data)
            if room.has_member(client):
                raise ValueError("Already in room")

            room.add_member(client)
            self.send_ok(
                client,
                {
                    "room": room.name,
                }
            )

            self.broadcast_to_room(
                room,
                msg.USER_JOINED,
                {
                    "username": client.username,
                    "room": room.name,
                },
                exclude=client
            )

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_leave_room(self, client, data):
        """Client leaves a room."""

        try:
            data = self.require_dict(data)
            room = self.get_room(data)

            if not room.has_member(client):
                raise ValueError("You are not in this room")

            self.broadcast_to_room(
                room,
                msg.USER_LEFT,
                {
                    "username": client.username,
                    "room": room.name,
                },
                exclude=client
            )

            room.remove_member(client)
            self.send_ok(
                client,
                {
                    "room": room.name,
                }
            )

            self.remove_empty_room(room)

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_list_members(self, client, data):
        """Return the members of a room."""

        try:
            data = self.require_dict(data)
            room = self.get_room(data)

            self.send_ok(
                client,
                {
                    "room": room.name,
                    "members": [ member.username for member in room.members ],
                }
            )

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_get_room_info(self, client, data):
        """Return info about the room."""

        try:
            data = self.require_dict(data)
            room = self.get_room(data)

            self.send_ok(
                client,
                {
                    "room": room.name,
                    "members": len(room.members),
                    "history_size": len(room.history),
                },
            )

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_get_history(self, client, data):
        """Return room message history."""

        try:
            data = self.require_dict(data)
            room = self.get_room(data)

            self.send_ok(
                client,
                {
                    "room": room.name,
                    "history": room.get_history(),
                }
            )

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_set_username(self, client, data):
        """Set a client's username."""

        try:
            data = self.require_dict(data)
            username = self.require_field(data, "username")

            if not isinstance(username, str):
                raise ValueError("Username must be a string")
            if not username:
                raise ValueError("Username cannot be empty")
            if len(username) > MAX_USERNAME_LENGTH:
                raise ValueError("Username is too long")

            if username in self.users and self.users[username] is not client:
                raise ValueError("Username is already taken")
            
            if client.username is not None:
                if not ALLOW_CHANGE_USERNAME:
                    raise ValueError("User already has a username")

                self.users.pop(client.username, None)

            # update both client and user name lookup
            client.username = username
            self.users[username] = client
            
            self.send_ok(
                client,
                {
                    "username": username,
                }
            )

        except ValueError as exc:
            self.send_error(client, str(exc))

    def handle_get_self(self, client, data):
        """Return info about the client."""

        self.send_ok(
            client,
            {
                "username": client.username,
                "address": client.address[0],
                "port": client.address[1],
                "rooms": list(client.rooms),
            }
        )

    def handle_ping(self, client, data):
        """Responds to a ping."""

        self.queue_message(client, msg.PONG, {})

    def handle_quit(self, client, data):
        """Disconnects the client."""

        self.disconnect(client)

    def handle_send_message(self, client, data):
        """Sends a message to a specific room."""

        try:
            data = self.require_dict(data)
            room = self.get_room(data)
            text = self.require_field(data, "msg")

            if not isinstance(text, str):
                raise ValueError("Message must be a string")
            if not text:
                raise ValueError("Message cannot be empty")
            if not room.has_member(client):
                raise ValueError("You are not in this room")

            msg_data = {
                "room": room.name,
                "username": client.username,
                "msg": text,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.broadcast_to_room(
                room,
                msg.ROOM_MESSAGE,
                msg_data
            )
            room.add_history(msg_data)

        except ValueError as exc:
            self.send_error(client, str(exc))


# Entry point
if __name__ == "__main__":
    server = IRCServer()
    server.start()
