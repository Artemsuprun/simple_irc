# server.py
# Artem Suprun
# 09/04/2026

"""
IRC server.

Uses:
    - TCP sockets
    - selectors for event-driven I/O
    - custom binary message protocol
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


# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Client
class Client:
    """
    Represents one connected client which tracks the client's socket, 
    address, username, rooms, and outgoing messages.
    """

    def __init__(self, sock, address):
        self.sock = sock
        self.address = address
        # Use a string to store the client's username (if set)
        self.username = None
        # Use a set to track the rooms the client has joined
        self.rooms = set()
        # Use non-blocking reader and writer buffers to manage incoming and outgoing messages
        self.reader = msg.MsgReader()
        self.writer = msg.MsgWriter()

    @property
    def fileno(self):
        """Return the file descriptor of the client's socket."""

        return self.sock.fileno()

    def queue(self, message):
        """Queue a message to be sent to the client."""

        self.writer.queue(message.pack())


# Room
class Room:
    """Represents a chat room."""

    def __init__(self, name, creator):
        # Store the room name and the creator of the room
        self.name = name
        self.members = {creator}
        # Use a deque to store the last MAX_HISTORY messages in the room
        self.history = deque(maxlen=MAX_HISTORY)

    def add_member(self, client):
        """Add a client to the room and update the client's room list."""

        self.members.add(client)
        client.rooms.add(self)

    def remove_member(self, client):
        """Remove a client from the room and update the client's room list."""

        self.members.discard(client)
        client.rooms.discard(self)

    def has_member(self, client):
        """Check if a client is a member of the room."""

        return client in self.members

    def add_message(self, sender, message):
        """Add a message to the room's history with a timestamp."""

        timestamp = datetime.now(timezone.utc)
        self.history.append( (sender, timestamp, message) )


# Server
class IRCServer:

    def __init__(self, host=HOST, port=PORT):
        # server host and port
        self.host = host
        self.port = port

        # Use the default selector for event-driven I/O
        self.selector = selectors.DefaultSelector()

        self.server_socket = None

        # Track connected clients and active rooms
        self.clients = {}
        self.rooms = {}

        # Track whether the server is running
        self.running = False

    # Server lifecycle
    def start(self):
        """Start the server."""

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
        self.selector.register(self.server_socket, selectors.EVENT_READ, data=self.accept_client)

        self.running = True

        logger.info("IRC server listening on %s:%s", self.host, self.port)

        # Run the main event loop in a try/finally block to ensure graceful shutdown on exit.
        # Note: We catch KeyboardInterrupt to allow the server to be stopped with Ctrl+C,
        # and log any unexpected exceptions. final block ensures that shutdown is called to 
        # clean up resources. Might add signal handling in the future for more graceful shutdowns.
        try:
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
            # block until there are socket events to process, with a timeout of 1 second.
            events = self.selector.select(timeout=1)
            for key, mask in events:
                callback = key.data
                try:
                    callback(key.fileobj, mask)
                except Exception:
                    logger.exception("Error processing socket event")

    def shutdown(self):
        """Gracefully shut down the server."""

        # If the server is already stopped, do nothing. Don't attempt cleanup again.
        if not self.running:
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
        # Close the selector to free up resources
        self.selector.close()

        logger.info("Server stopped.")

    # Client handling
    def accept_client(self, server_socket, mask):
        """Accept a new client connection."""

        # Accept the incoming connection and get the client socket and address.
        client_socket, address = server_socket.accept()
        # Set the client socket to non-blocking so that the selector can manage it.
        client_socket.setblocking(False)
        # Create a new Client instance to track the connected client.
        client = Client(client_socket, address)
        # Store the client in the clients dictionary using the socket as the key.
        self.clients[client_socket] = client
        # Add the client socket to the selector.
        self.selector.register(client_socket, selectors.EVENT_READ, data=self.read_client)

        logger.info("Client connected: %s", address)

    def read_client(self, sock, mask):
        """
        Read a message from a client. Specify, read any currently available bytes from 
        the socket and attempt to assemble a complete message.
        """

        # Get the Client instance associated with the socket. 
        client = self.clients.get(sock)
        if client is None: # If the client is not found, return early.
            return

        # Use the MsgReader to read and assemble a complete message from the socket.
        try:
            chunk = sock.recv(msg.MAX_SIZE * 4)
        except BlockingIOError:
            return  # No data available to read right now
        except OSError as exc:
            logger.warning("Error reading from %s: %s", client.address, exc)
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
                logger.info("Received command %s from %s", message.cmd, client.address)
                self.handle_message(client, message)

                if sock not in self.clients:
                    return  # Client may have been disconnected during message handling
        except msg.ProtocolError as exc:
            # kicks the client if they send a malformed message, and logs the error.
            logger.warning("Protocol error from %s: %s", client.address, exc)
            self.disconnect(client)

    # Message handling
    def handle_message(self, client, message):
        """Send the message to the appropriate handler based on its command."""

        handlers = {
            msg.CREATE: self.create_room,
            msg.ROOMS: self.list_rooms,
            msg.JOIN: self.join_room,
            msg.LEAVE: self.leave_room,
            msg.MEMBERS: self.list_members,
            msg.MESSAGE: self.send_message,
        }
        handler = handlers.get(message.cmd)

        # If no handler is found for the command, send an error message back to the client.
        if handler is None:
            self.send_error(client, "Unknown command.")
            return
        
        # Call the handler function with the client and message data.
        handler(client, message.data)

    # CREATE
    def create_room(self, client, room_name):
        """Create a new chat room with the given name."""

        # Validate the room name to ensure it is not empty, does not contain whitespace 
        # or commas, and does not already exist.
        room_name = room_name.strip()
        if not room_name:
            self.send_error(client, "Room name cannot be empty.")
            return

        if any(char.isspace() for char in room_name):
            self.send_error(client, "Room names cannot contain whitespace.")
            return

        if "," in room_name:
            self.send_error(client, "Room names cannot contain ',' characters.")
            return

        if room_name in self.rooms:
            self.send_error(client, "A room with that name already exists.")
            return

        # Create the room and add the client as its first member.
        room = Room(room_name, client)
        self.rooms[room_name] = room
        client.rooms.add(room)

        # Send a confirmation message to the client and log the creation of the room.
        self.send_text(client, msg.CREATE, f"Successfully created and joined '{room_name}'.")

        logger.info("%s created room '%s'", self.client_name(client), room_name)

    # ROOMS
    def list_rooms(self, client, data):
        """Send a list of all active rooms to the client."""

        if not self.rooms:
            self.send_text(client, msg.ROOMS, "No rooms currently exist.")
            return

        lines = ["Room Names:"]

        for room in sorted(self.rooms.values(), key=lambda r: r.name):
            lines.append(f"  {room.name} ({len(room.members)} members)")

        self.send_text(client, msg.ROOMS, "\n".join(lines))

    # JOIN
    def join_room(self, client, room_name):
        """Add the client to an existing room and send them the room's message history."""

        # Validate the room name and check if the room exists. If it does, 
        # add the client to the room and send them the room's message history. 
        room_name = room_name.strip()
        room = self.rooms.get(room_name)
        if room is None:
            self.send_error(client, f"Room '{room_name}' does not exist.")
            return
        if room.has_member(client):
            self.send_error(client, f"You are already a member of '{room_name}'.")
            return
        # add client
        room.add_member(client)
        self.send_text(client, msg.JOIN, f"Successfully joined '{room_name}'.")

        # Send room history
        for sender, timestamp, message in room.history:
            self.send_text(
                client,
                msg.MESSAGE,
                self.format_message(sender, room.name, timestamp, message)
            )

        # Notify existing members
        self.broadcast(room, f"{self.client_name(client)} joined the room.", exclude=client)

        logger.info("%s joined '%s'", self.client_name(client), room_name)

    # LEAVE
    def leave_room(self, client, room_name):
        """Remove the client from a room and notify other members."""

        # Validate the room name and check if the client is a member of the room. 
        # If so, remove the client from the room and notify other members. 
        room_name = room_name.strip()
        room = self.rooms.get(room_name)
        if room is None or not room.has_member(client):
            self.send_error(client, f"You are not a member of '{room_name}'.")
            return
        # remove client from room
        room.remove_member(client)
        self.send_text(client, msg.LEAVE, f"Successfully left '{room_name}'.")
        self.broadcast(room, f"{self.client_name(client)} left the room.")

        # If the room becomes empty, delete it.
        if not room.members:
            del self.rooms[room_name]
            logger.info("Removed empty room '%s'", room_name)

    # MEMBERS
    def list_members(self, client, room_name):
        """Send a list of all members in a room to the client."""

        # Validate the room name and check if the room exists. 
        room_name = room_name.strip()
        room = self.rooms.get(room_name)
        if room is None:
            self.send_error(client, f"Room '{room_name}' does not exist.")
            return

        # sort the members by their display names and send the list to the client.
        members = sorted( self.client_name(member) for member in room.members )
        response = "Members:\n"
        response += "\n".join(f"  {member}" for member in members )
        self.send_text(client, msg.MEMBERS, response)

    # MESSAGE
    def send_message(self, client, data):
        """Send a message to a room the client has joined."""

        # Split the data into room name and message text.
        room_name, separator, message_text = data.partition(" ")
        if not separator:
            self.send_error(client, "Usage: MESSAGE <room1> <message>")
            return

        room_name = room_name.strip()
        message_text = message_text.strip()

        if not room_name:
            self.send_error(client, "No room specified.")
            return
        if not message_text:
            self.send_error(client, "Message cannot be empty.")
            return

        room = self.rooms.get(room_name)
        if room is None:
            self.send_error(client, f"Room '{room_name}' does not exist.")
            return
        if not room.has_member(client):
            self.send_error(client, f"You are not a member of '{room_name}'.")
            return

        # Add the message to the room's history and broadcast it to all members.
        timestamp = datetime.now(timezone.utc)
        room.add_message(client, message_text)
        formatted_message = self.format_message(client, room.name, timestamp, message_text)
        self.broadcast(room, formatted_message)

    # Broadcasting
    def broadcast(self, room, text, exclude=None):

        for client in list(room.members):
            if client is exclude:
                continue

            self.send_text(client, msg.MESSAGE, text)

    # Sending
    def send_text(self, client, command, text):

        try:
            message = msg.Msg(command, text)
            client.queue(message)
            self.enable_write(client)

        except msg.ProtocolError as exc:
            logger.warning("Unable to send message: %s", exc)

    def send_error(self, client, text):
        self.send_text(client, msg.MESSAGE, f"ERROR: {text}")

    # Write handling
    def enable_write(self, client):

        try:
            self.selector.modify(
                client.sock,
                selectors.EVENT_READ | selectors.EVENT_WRITE,
                self.handle_client_io
            )
        except (KeyError, ValueError):
            pass

    def handle_client_io(self, sock, mask):

        client = self.clients.get(sock)
        if client is None:
            return

        if mask & selectors.EVENT_READ:
            self.read_client(sock, mask)

        if client not in self.clients.values():
            return

        if mask & selectors.EVENT_WRITE:
            self.write_client(client)

    def write_client(self, client):

        try:
            # Attempt to send as many bytes as possible from the client's outgoing buffer to the socket.
            done = client.writer.send(client.sock)
            events = selectors.EVENT_READ

            # If the outgoing buffer is not fully drained, keep watching 
            # for write events to send remaining data.
            if not done:
                events |= selectors.EVENT_WRITE

            self.selector.modify(client.sock, events, self.handle_client_io)

        except (ConnectionError, OSError):
            self.disconnect(client)

    # Disconnect
    def disconnect(self, client):
        """Disconnects the client from the server, does the clean up."""

        sock = client.sock
        if sock not in self.clients:
            return

        logger.info("Disconnecting %s", self.client_name(client))

        for room in list(client.rooms):
            room.remove_member(client)

            # remove room if empty
            if not room.members:
                self.rooms.pop(room.name, None)

        # remove from selector
        try:
            self.selector.unregister(sock)
        except (KeyError, ValueError):
            pass

        # remove client from client registry
        self.clients.pop(client.sock, None)

        # close socket 
        try:
            client.sock.close()
        except OSError:
            pass

    # Utilities
    @staticmethod
    def client_name(client):
        if client.username:
            return client.username

        return f"{client.address[0]}:{client.address[1]}"

    @staticmethod
    def format_message(sender, room, timestamp, message):

        if isinstance(sender, Client):
            sender = IRCServer.client_name(sender)

        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")

        return (
            f"From: {sender}\n"
            f"Room: {room}\n"
            f"Time: {timestamp}\n"
            f"{message}"
        )


# Entry point
if __name__ == "__main__":
    server = IRCServer()
    server.start()
