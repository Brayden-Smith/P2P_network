import os
import socket
import threading
import time
from datetime import datetime
from message import Message
import random
import math
import struct

class Peer:
    def __init__(self, id):
        self.id = id

        # Dictionary that takes a peer ID and returns the current connection socket with that peer
        self.connections = {}
        self.connection_lock = threading.Lock()

        # Current Peer's server that accepts connections from other peers
        self.server_socket = None
        self.server_thread = None

        # List of dictionaries that contain all data of possible peers, loaded from Peerinfo.cfg
        self.all_peers = []
        # List that contains possible peers, only id
        self.peers = []

        self.running = True
        config = open('Common.cfg')
        setup = config.read().split()
        config.close()

        # Number of peers to send data to and unchoking details
        self.neighbor_count = int(setup[1])
        self.unchoking_interval = float(setup[3])
        self.optimistic_unchoking_interval = float(setup[5])

        # Attributes of file being transferred
        self.file_name = setup[7]
        self.file_size = int(setup[9])
        self.piece_size = int(setup[11])


        # Sets up identifying information for the peer
        config = open('PeerInfo.cfg')
        setup = config.read().splitlines()
        selected_line = ''
        for line in setup:
            info = line.split()
            if int(info[0]) == self.id:
                selected_line = line.split()
        config.close()

        self.host_name = selected_line[1]
        self.port = int(selected_line[2])
        self.file_complete = bool(int(selected_line[3]))

        if not os.path.exists("peer_" + str(self.id)):
            os.mkdir("peer_" + str(self.id))

        # Create empty file as placeholder of data (if not already there)
        if not os.path.exists("peer_" + str(self.id) + "\\" + self.file_name):
            file = open("peer_" + str(self.id) + "\\" + self.file_name, "wb")
            file.truncate(self.file_size)

        # Overwrites log, makes it better for testing. Check if it needs to be appended in docs later.
        self.log_file = open("log_peer_" + str(self.id) + ".log", "w")

        self.received_bytes = {}
        self.download_speeds = {}
        # List of peers that are interested in this peers file data
        self.interested_neighbors = []
        self.optimistic_neighbor = -1
        self.preferred_neighbors = []
        # Dictionary that contains the bitfields of other peers [peer_id] -> list (bitfield)
        self.peer_bitfields = {}
        # Dictionary that describes peers that the current is interested in [peer_id] -> bool
        self.peer_interest_status = {}
        # Dictionary that describes peers that are choking this one
        self.choke_status = {}
        # List of pieces that are currently being retrieved, stops redundancy
        self.pieces_requested = []

        self.bitfield = []
        self.piece_count = 0
        self.hasPieces = True #to make it so we don't check massive arrays for a 1
        if(self.file_complete):
            self.bitfield = [1] * math.ceil(self.file_size / self.piece_size)
            self.hasPieces = True
            self.piece_count = math.ceil(self.file_size / self.piece_size)
        else:
            self.bitfield = [0] * math.ceil(self.file_size / self.piece_size)
            self.hasPieces = False

        self._read_all_peers()
        self._start_server()
        time.sleep(0.5)
        self._connect_to_previous_peers()

    def _read_all_peers(self):
        """Read all peer information from PeerInfo.cfg"""
        try:
            with open('PeerInfo.cfg', 'r') as config:
                lines = config.read().splitlines()
                for line in lines:
                    if line.strip():
                        parts = line.split()
                        peer_info = {
                            'id': int(parts[0]),
                            'host': parts[1],
                            'port': int(parts[2]),
                            'has_file': bool(int(parts[3]))
                        }
                        self.all_peers.append(peer_info)
                        self.peers.append(parts[0])
            print(f"[Peer {self.id}] Loaded {len(self.all_peers)} peers from config")
        except Exception as e:
            print(f"[Peer {self.id}] Error reading peer info: {e}")

    def _start_server(self):
        """Start listening socket in background thread"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host_name, self.port))
            self.server_socket.listen(len(self.all_peers))

            print(f"[Peer {self.id}] Server listening on {self.host_name}:{self.port}")

            self.server_thread = threading.Thread(target=self._accept_connections, daemon=True)
            self.server_thread.start()

        except Exception as e:
            print(f"[Peer {self.id}] Error starting server: {e}")

    # TODO: Timeouts have been disabled for the sake of testing
    def _accept_connections(self):
        """Accept incoming connections (runs in thread)"""
        print(f"[Peer {self.id}] Waiting for incoming connections...")

        while self.running:
            try:
                #self.server_socket.settimeout(1.0)

                try:
                    client_socket, client_address = self.server_socket.accept()
                    print(f"[Peer {self.id}] Accepted connection from {client_address}")

                    handler_thread = threading.Thread(
                        target=self._handle_incoming_connection,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    handler_thread.start()

                except socket.timeout:
                    continue

            except Exception as e:
                if self.running:
                    print(f"[Peer {self.id}] Error accepting connection: {e}")

    def _handle_incoming_connection(self, client_socket, client_address):
        """Handle an incoming peer connection (runs in thread)"""
        try:
            print(f"[Peer {self.id}] Handling connection from {client_address}")

            # Step 1: Receive handshake
            handshake_data = client_socket.recv(32)
            if len(handshake_data) != 32:
                print(f"[Peer {self.id}] Invalid handshake length from {client_address}")
                return

            peer_id = Message.parse_handshake(handshake_data)
            if peer_id is None:
                print(f"[Peer {self.id}] Invalid handshake from {client_address}")
                return

            print(f"[Peer {self.id}] Received handshake from Peer {peer_id}")
            self._log_event(f"Peer {self.id} is connected from Peer {peer_id}.")

            # Step 2: Send handshake response
            response = Message.create_handshake(self.id)
            client_socket.sendall(response)
            print(f"[Peer {self.id}] Sent handshake to Peer {peer_id}")

            # Store connection
            with self.connection_lock:
                self.connections[peer_id] = client_socket

            # Exchange bitfield messages
            self._send_bitfield(client_socket)

            incoming = self._recv_message(client_socket)
            if not incoming:
                raise ValueError(f"{peer_id} closed connection before sending bitfield")

            msg_type, payload = incoming
            if msg_type != "bitfield":
                raise ValueError(f"{peer_id} sent non-bitfield as first message")

            self._handle_bitfield(peer_id, payload, client_socket)

            # Assume connection starts off as choked
            self.choke_status[peer_id] = True

            self._listen_for_messages(client_socket, peer_id)

        except Exception as e:
            print(f"[Peer {self.id}] Error handling incoming connection: {e}")
        finally:
            if 'peer_id' in locals() and peer_id:
                with self.connection_lock:
                    if peer_id in self.connections:
                        del self.connections[peer_id]
                if peer_id in self.peer_bitfields:
                    del self.peer_bitfields[peer_id]
                if peer_id in self.peer_interest_status:
                    del self.peer_interest_status[peer_id]
            client_socket.close()

    def _connect_to_previous_peers(self):
        """Connect to all peers that started before this one"""
        print(f"[Peer {self.id}] Connecting to previous peers...")

        # TODO: Verify peers are in order of ID number, or we have to do it based on order in cfg file
        for peer_info in self.all_peers:
            if peer_info['id'] < self.id:
                self._connect_to_peer(peer_info)

    def _connect_to_peer(self, peer_info):
        """Initiate connection to a specific peer"""
        peer_id = peer_info['id']
        peer_host = peer_info['host']
        peer_port = peer_info['port']

        try:
            print(f"[Peer {self.id}] Connecting to Peer {peer_id} at {peer_host}:{peer_port}")

            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #peer_socket.settimeout(10.0)
            peer_socket.connect((peer_host, peer_port))

            print(f"[Peer {self.id}] Successfully connected to Peer {peer_id}")

            with self.connection_lock:
                self.connections[peer_id] = peer_socket

            handler_thread = threading.Thread(
                target=self._handle_peer_connection,
                args=(peer_socket, peer_id),
                daemon=True
            )
            handler_thread.start()

        except Exception as e:
            print(f"[Peer {self.id}] Failed to connect to Peer {peer_id}: {e}")

    def _handle_peer_connection(self, peer_socket, peer_id):
        """Handle communication with a connected peer (runs in thread)"""
        try:
            print(f"[Peer {self.id}] Managing connection with Peer {peer_id}")

            # Step 1: Send handshake
            handshake = Message.create_handshake(self.id)
            peer_socket.sendall(handshake)
            print(f"[Peer {self.id}] Sent handshake to Peer {peer_id}")

            # Step 2: Receive handshake response
            response_data = peer_socket.recv(32)
            if len(response_data) != 32:
                print(f"[Peer {self.id}] Invalid handshake response from Peer {peer_id}")
                return

            received_peer_id = Message.parse_handshake(response_data)
            if received_peer_id != peer_id:
                print(f"[Peer {self.id}] Peer ID mismatch! Expected {peer_id}, got {received_peer_id}")
                return

            print(f"[Peer {self.id}] Received handshake response from Peer {peer_id}")
            self._log_event(f"Peer {self.id} makes a connection to Peer {peer_id}.")

            self._send_bitfield(peer_socket)

            incoming = self._recv_message(peer_socket)
            if not incoming:
                raise ValueError(f"{peer_id} closed connection before sending bitfield")

            msg_type, payload = incoming
            if msg_type != "bitfield":
                raise ValueError(f"{peer_id} sent non-bitfield as first message")

            self._handle_bitfield(peer_id, payload, peer_socket)

            # Assume connection starts off as choked
            self.choke_status[peer_id] = True

            self._listen_for_messages(peer_socket, peer_id)

        except Exception as e:
            print(f"[Peer {self.id}] Error in connection with Peer {peer_id}: {e}")
        finally:
            with self.connection_lock:
                if peer_id in self.connections:
                    del self.connections[peer_id]
            if peer_id in self.peer_bitfields:
                del self.peer_bitfields[peer_id]
            if peer_id in self.peer_interest_status:
                del self.peer_interest_status[peer_id]
            peer_socket.close()
            print(f"[Peer {self.id}] Closed connection with Peer {peer_id}")

    def _log_event(self, message):
        """Write a timestamped event to this peer's log file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_file.write(f"{timestamp}: {message}\n")
        self.log_file.flush()

    def _send_bitfield(self, peer_socket):
        """Send this peer's bitfield to a neighbor"""
        encoded = self._encode_bitfield()
        message = Message.create_message("bitfield", encoded)
        peer_socket.sendall(message)

    def _handle_bitfield(self, peer_id, payload, peer_socket):
        """Process a received bitfield and express interest if needed"""
        piece_count = len(self.bitfield)
        remote_bitfield = self._decode_bitfield(payload, piece_count)
        self.peer_bitfields[peer_id] = remote_bitfield

        self._evaluate_interest(peer_id, peer_socket)

    def _should_send_interested(self, remote_bitfield):
        """Determine if the remote peer has pieces we need"""
        for index, has_piece in enumerate(remote_bitfield):
            if has_piece and not self.bitfield[index]:
                return True
        return False

    def _encode_bitfield(self):
        """Convert the local bitfield list into the protocol byte format"""
        if not self.bitfield:
            return b""

        byte_count = math.ceil(len(self.bitfield) / 8)
        encoded = bytearray(byte_count)

        for index, bit in enumerate(self.bitfield):
            if bit:
                byte_index = index // 8
                bit_index = index % 8
                encoded[byte_index] |= 1 << (7 - bit_index)

        return bytes(encoded)

    def _decode_bitfield(self, payload, piece_count):
        """Convert a received bitfield payload into a list of piece flags"""
        bits = []
        for index in range(piece_count):
            byte_index = index // 8
            bit_index = index % 8
            if byte_index >= len(payload):
                bits.append(0)
            else:
                bits.append((payload[byte_index] >> (7 - bit_index)) & 1)
        return bits

    def _evaluate_interest(self, peer_id, peer_socket):
        """Send interested/not interested message if our need state changed"""
        remote = self.peer_bitfields.get(peer_id, [])
        interested = self._should_send_interested(remote)
        previous = self.peer_interest_status.get(peer_id)

        if interested and previous is not True:
            peer_socket.sendall(Message.create_message("interested"))
            self.peer_interest_status[peer_id] = True
        elif not interested and previous is not False:
            peer_socket.sendall(Message.create_message("not interested"))
            self.peer_interest_status[peer_id] = False

    def _recv_exact(self, peer_socket, num_bytes):
        """Receive an exact number of bytes or return None on disconnect"""

        data = b""
        while len(data) < num_bytes:
            chunk = peer_socket.recv(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _recv_message(self, peer_socket):
        """Receive and parse a single protocol message"""
        length_bytes = self._recv_exact(peer_socket, 4)
        if not length_bytes or len(length_bytes) < 4:
            return None

        message_length = struct.unpack('>I', length_bytes)[0]
        body = self._recv_exact(peer_socket, message_length)
        if body is None:
            return None

        parsed = Message.parse_message(length_bytes + body)
        return parsed

    def _listen_for_messages(self, peer_socket, peer_id):
        """Continuously read messages from a connected peer"""
        while self.running:
            incoming = self._recv_message(peer_socket)
            if not incoming:
                break

            msg_type, payload = incoming
            try:
                self._handle_message(peer_id, msg_type, payload, peer_socket)
            except Exception as exc:
                print(f"[Peer {self.id}] Error handling {msg_type} from Peer {peer_id}: {exc}")
                break

    def _handle_message(self, peer_id, msg_type, payload, peer_socket):
        """Dispatch a message to the appropriate handler"""
        if msg_type == "interested":
            self._handle_interested(peer_id)
        elif msg_type == "not interested":
            self._handle_not_interested(peer_id)
        elif msg_type == "have":
            self._handle_have(peer_id, payload, peer_socket)
        elif msg_type == "choke":
            self._handle_choke(peer_id)
        elif msg_type == "unchoke":
            self._handle_unchoke(peer_id)
        elif msg_type == "request":
            self._handle_request(peer_id, payload)
        elif msg_type == "piece":
            self._handle_piece(peer_id, payload)
        else:
            # Future handlers will cover other message types
            pass

    def _handle_interested(self, peer_id):
        """Record that a neighbor is interested in our pieces"""
        if peer_id not in self.interested_neighbors:
            self.interested_neighbors.append(peer_id)
        self._log_event(f"Peer {self.id} received the 'interested' message from {peer_id}.")

    def _handle_not_interested(self, peer_id):
        """Record that a neighbor is no longer interested"""
        if peer_id in self.interested_neighbors:
            self.interested_neighbors.remove(peer_id)
        self._log_event(f"Peer {self.id} received the 'not interested' message from {peer_id}.")

    def _handle_have(self, peer_id, payload, peer_socket):
        """Update neighbor bitfield information upon receiving a have message"""
        if len(payload) != 4:
            raise ValueError("Invalid 'have' payload length")

        piece_index = struct.unpack('>I', payload)[0]
        self._log_event(f"Peer {self.id} received the 'have' message from {peer_id} for the piece {piece_index}.")

        remote_bitfield = self.peer_bitfields.get(peer_id)
        if remote_bitfield and 0 <= piece_index < len(remote_bitfield):
            remote_bitfield[piece_index] = 1
        else:
            # If we didn't get an initial bitfield, create a sparse representation
            piece_count = len(self.bitfield)
            remote_bitfield = [0] * piece_count
            if 0 <= piece_index < piece_count:
                remote_bitfield[piece_index] = 1
            self.peer_bitfields[peer_id] = remote_bitfield

        self._evaluate_interest(peer_id, peer_socket)

    def _handle_choke(self, peer_id):
        self.choke_status[peer_id] = True
        self._log_event(f"Peer {self.id} received the 'choke' message from {peer_id}.")

    def _handle_unchoke(self, peer_id):
        self.choke_status[peer_id] = False
        self._log_event(f"Peer {self.id} received the 'unchoke' message from {peer_id}.")
        request_thread = threading.Thread(
            target=self.request_pieces(peer_id),
            args=[peer_id],
            daemon=True
        )
        request_thread.start()

    def _handle_request(self, peer_id, payload):
        """Send the requested file data to the peer"""
        if len(payload) != 4:
            raise ValueError("Invalid 'have' payload length")

        piece_index = struct.unpack('>I', payload)[0]
        data = self.read_file(piece_index, self.piece_size)
        message = Message.create_message("piece", struct.pack(">I", piece_index) + data)
        self.connections[peer_id].send(message)

    def _handle_piece(self, peer_id, payload):
        """Download the received piece and update"""
        piece_index = struct.unpack(">I", payload[:4])[0]
        data = payload[4:]
        self.write_file(piece_index, data)

        self._log_event(f"Peer {self.id}  has downloaded the piece {piece_index} from {peer_id}. Now the number of"
                        f" pieces it has is {self.piece_count}.")

        # Update attributes after obtaining new piece
        self.piece_count += 1
        self.bitfield[piece_index] = 1
        if self.received_bytes.get(peer_id):
            self.received_bytes[peer_id] += len(data)
        else:
            self.received_bytes[peer_id] = len(data)
        self.pieces_requested.remove(piece_index)

        # Broadcast new find to peers
        for peer, psocket in self.connections.items():
            psocket.send(Message.create_message("have", struct.pack(">I", piece_index)))
            self._evaluate_interest(peer, psocket)

    def get_connected_peers(self):
        """Return list of currently connected peer IDs"""
        with self.connection_lock:
            return list(self.connections.keys())

    def calculate_download(self):
        """Calculate the download speed of every peer that has sent data in this interval """
        self.download_speeds = {}
        for peer_id in self.received_bytes:
            self.download_speeds[peer_id] = self.received_bytes[peer_id] / self.unchoking_interval

        # reset received_bytes for next interval
        self.received_bytes = {}

    def choose_preferred_neighbor(self):
        # Choke previous preferred neighbors
        old_neighbors = self.preferred_neighbors

        self.preferred_neighbors = []
        # If current peer has all the file, then it randomly chooses from those interested
        if self.hasPieces:
            neighbors = self.interested_neighbors[:]
            random.shuffle(neighbors)
            count = min(len(neighbors), self.neighbor_count)
            self.preferred_neighbors = neighbors[:count]
        else:
            self.calculate_download()

            neighbors = list(self.download_speeds.items())
            random.shuffle(neighbors)  # Shuffle to handle ties randomly
            neighbors.sort(key=lambda x: x[1], reverse=True)  # Sort by download speed descending

            # Only the top interested neighbors are chosen as preferred
            interested_neighbors = [peer_id for peer_id, _ in neighbors if peer_id in self.interested_neighbors]
            count = min(len(interested_neighbors), self.neighbor_count)
            top_neighbors = interested_neighbors[:count]
            self.preferred_neighbors = [peer_id for peer_id in top_neighbors]

        # Unchokes new neighbors
        for peer_id in self.preferred_neighbors:
            if peer_id not in old_neighbors and peer_id != self.optimistic_neighbor:
                self.connections[peer_id].send(Message.create_message("unchoke"))

        # Chokes old neighbors not currently preferred or optimistic
        for peer_id in old_neighbors:
            if peer_id not in self.preferred_neighbors and peer_id != self.optimistic_neighbor:
                self.connections[peer_id].send(Message.create_message("choke"))

    def choose_optimistic_neighbor(self):
        candidates = []
        old_optimistic_neighbor = self.optimistic_neighbor
        for peer in self.interested_neighbors:
            # if it is not a preferred neighbor it is choked
            if peer not in self.preferred_neighbors:
                candidates.append(peer)

        if len(candidates) == 0:
            self.optimistic_neighbor = -1
        else:
            self.optimistic_neighbor = random.choice(candidates)

        if self.optimistic_neighbor != -1 and self.optimistic_neighbor != old_optimistic_neighbor:
            self.connections[self.optimistic_neighbor].send(Message.create_message("unchoke"))
        if (old_optimistic_neighbor != -1 and old_optimistic_neighbor != self.optimistic_neighbor and
                old_optimistic_neighbor not in self.preferred_neighbors):
            self.connections[old_optimistic_neighbor].send(Message.create_message("choke"))

    def request_pieces(self, peer_id):
        while not self.choke_status[peer_id] and self.peer_interest_status[peer_id]:
            new_pieces = []
            for index, has_piece in enumerate(self.peer_bitfields[peer_id]):
                if has_piece and (not self.bitfield[index]) and (index not in self.pieces_requested):
                    new_pieces.append(index)
            # TODO: Slow down the rate of request?
            if not new_pieces:
                break
            new_piece = random.choice(new_pieces)

            # TODO: needs to remove from pieces_requested if the server refuses to send a piece
            self.connections[peer_id].send(Message.create_message("request", struct.pack(">I", new_piece)))
            self.pieces_requested.append(new_piece)

    def read_file(self, piece_index, size):
        offset = piece_index * self.piece_size
        file = open("peer_" + str(self.id) + "//" + self.file_name, 'rb')
        file.seek(offset)
        data = file.read(size)
        file.close()
        return data

    def write_file(self, piece_index, data):
        offset = piece_index * self.piece_size
        file = open("peer_" + str(self.id) + "//" + self.file_name, 'r+b')
        file.seek(offset)
        file.write(data)
        file.close()


    def shutdown(self):
        """Gracefully shutdown all connections"""
        print(f"[Peer {self.id}] Shutting down...")
        self.running = False

        with self.connection_lock:
            for peer_id, peer_socket in self.connections.items():
                try:
                    peer_socket.close()
                    print(f"[Peer {self.id}] Closed connection to Peer {peer_id}")
                except:
                    pass
            self.connections.clear()

        if self.server_socket:
            try:
                self.server_socket.close()
                print(f"[Peer {self.id}] Server socket closed")
            except:
                pass

        print(f"[Peer {self.id}] Shutdown complete")

        # Close log file to save it
        self.log_file.close()
