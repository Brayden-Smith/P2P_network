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
        self.bitfield_condition = threading.Condition()
        self.data_lock = threading.Lock()

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

        self.peer_dir = os.path.join("peer_" + str(self.id))
        self.file_path = os.path.join(self.peer_dir, self.file_name)

        if not os.path.exists(self.peer_dir):
            os.mkdir(self.peer_dir)

        # Create empty file as placeholder of data (if not already there)
        if not os.path.exists(self.file_path):
            with open(self.file_path, "wb") as file:
                file.truncate(self.file_size)

        self.log_file = open("log_peer_" + str(self.id) + ".log", "w")

        self.received_bytes = {}
        self.download_speeds = {}
        # List of peers that are interested in this peers file data
        self.interested_neighbors = []
        self.optimistic_neighbor = -1
        self.preferred_neighbors = []
        # Dictionary that contains the bitfields of other peers [peer_id] -> bytearray (bitfield)
        self.peer_bitfields = {}
        # Dictionary that contains the interesting bits a peer has [peer_id] -> bytearray (bitfield)
        self.peer_interesting_bits = {}
        # Dictionary that describes peers that the self is interested in [peer_id] -> bool
        self.peer_interest_status = {}
        # Dictionary that describes peers that are choking this one
        self.choke_status = {}
        # List of pieces that are currently being retrieved, stops redundancy
        self.pieces_requested = []
        # Total number of pieces that make up the target file
        self.num_of_pieces = math.ceil(self.file_size / self.piece_size)
        # bitfield is stored as bytearray - each bit represents a piece
        self.bitfield_size = math.ceil(self.num_of_pieces / 8)
        self.bitfield = bytearray(self.bitfield_size)

        # bitfield already initialized to all zeros
        self.piece_count = 0

        self.ideal_bitfield =bytearray(self.bitfield_size)
        full_bytes = self.num_of_pieces // 8  # number of complete bytes
        remaining_bits = self.num_of_pieces % 8  # leftover bits in next byte

        # Set full bytes to 1111 1111
        for i in range(full_bytes):
            self.ideal_bitfield[i] = 0xFF

        # Set remaining bits in the next byte
        if remaining_bits > 0:
            mask = 0b11111111
            self.ideal_bitfield[full_bytes] = ((1 << (8 - remaining_bits)) - 1) ^ mask

        if self.file_complete:
            self.bitfield = self.ideal_bitfield
            self.piece_count = self.num_of_pieces

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
                        self.peers.append(int(parts[0]))
                        self.received_bytes[int(parts[0])] = 0
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
            if(self.piece_count > 0):
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
                if peer_id in self.choke_status:
                    self.choke_status[peer_id] = True
            client_socket.close()

    def _connect_to_previous_peers(self):
        """Connect to all peers that started before this one"""
        print(f"[Peer {self.id}] Connecting to previous peers...")

        for peer_info in self.all_peers:
            if peer_info['id'] != self.id:
                self._connect_to_peer(peer_info)
            else:
                break

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



            # Assume connection starts off as choked
            self.choke_status[peer_id] = True

            self._listen_for_messages(peer_socket, peer_id)

        except Exception as e:
            print(f"[Peer {self.id}] Error in connection with Peer {peer_id}: {e}")
        finally:
            with self.connection_lock:
                if peer_id in self.connections:
                    del self.connections[peer_id]
            if peer_id in self.choke_status:
                self.choke_status[peer_id] = True
            peer_socket.close()
            print(f"[Peer {self.id}] Closed connection with Peer {peer_id}")

    def _log_event(self, message):
        """Write a timestamped event to this peer's log file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_file.write(f"{timestamp}: {message}\n")
        self.log_file.flush()

    def _send_bitfield(self, peer_socket):
        """Send this peer's bitfield to a neighbor"""
        message = Message.create_message("bitfield", self.bitfield)
        peer_socket.sendall(message)

    def _set_bit(self, piece_index, bitfield):
        """Set a bit in the bitfield bytearray"""
        byte_index = piece_index // 8
        bit_offset = 7 - (piece_index % 8)
        bitfield[byte_index] |= (1 << bit_offset)

    def _get_bit(self, piece_index, bitfield):
        """Get a bit from a bitfield bytearray"""
        byte_index = piece_index // 8
        bit_offset = 7 - (piece_index % 8)
        return (bitfield[byte_index] >> bit_offset) & 1

    def _handle_bitfield(self, peer_id, payload, peer_socket):
        """Process a received bitfield and express interest if needed"""
        remote_bitfield = bytearray(payload)
        self.peer_bitfields[peer_id] = remote_bitfield
        self._evaluate_interest(peer_id, peer_socket)

    def _should_send_interested(self, remote_bitfield, peer_id):
        """Determine if the remote peer has pieces we need"""
        # Check if remote has any piece we don't have
        pieces_downloadable = bytearray(remote_bitfield)
        for byte_index in range(0, len(pieces_downloadable)):
            pieces_downloadable[byte_index] = remote_bitfield[byte_index] & ~self.bitfield[byte_index]

        self.peer_interesting_bits[peer_id] = pieces_downloadable

        if any(pieces_downloadable):
            return True
        return False

    def _evaluate_interest(self, peer_id, peer_socket):
        """Send interested/not interested message if our need state changed"""
        remote = self.peer_bitfields.get(peer_id, [])
        interested = self._should_send_interested(remote, peer_id)
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
        if msg_type == "bitfield":
            self._handle_bitfield(peer_id, payload, peer_socket)
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

        with self.data_lock:
            remote_bitfield = self.peer_bitfields.get(peer_id)
            if remote_bitfield:
                self._set_bit(piece_index, remote_bitfield)
            else:
                # If we didn't get an initial bitfield, create a sparse representation
                remote_bitfield = bytearray(self.bitfield_size)
                self._set_bit(piece_index, remote_bitfield)
                self.peer_bitfields[peer_id] = remote_bitfield

        self._evaluate_interest(peer_id, peer_socket)

    def _handle_choke(self, peer_id):
        self.choke_status[peer_id] = True
        self._log_event(f"Peer {self.id} is choked by {peer_id}.")

    def _handle_unchoke(self, peer_id):
        self.choke_status[peer_id] = False
        self._log_event(f"Peer {self.id} is unchoked by {peer_id}.")
        request_thread = threading.Thread(
            target=self.request_pieces,
            args=(peer_id,),
            daemon=True
        )
        request_thread.start()

    def _handle_request(self, peer_id, payload):
        """Send the requested file data to the peer"""
        #only upload to optimistic neighbor and preferred neighbors
        if peer_id != self.optimistic_neighbor and peer_id not in self.interested_neighbors:
            return

        if len(payload) != 4:
            raise ValueError("Invalid 'request' payload length")

        piece_index = struct.unpack('>I', payload)[0]
        data = self.read_file(piece_index, self.piece_size)
        message = Message.create_message("piece", struct.pack(">I", piece_index) + data)
        with self.connection_lock:
            if peer_id in self.connections:
                self.connections[peer_id].sendall(message)

    def _handle_piece(self, peer_id, payload):
        """Download the received piece and update"""
        piece_index = struct.unpack(">I", payload[:4])[0]
        data = payload[4:]
        self.write_file(piece_index, data)

        if piece_index >= self.num_of_pieces:
            raise ValueError(f"Peer {peer_id} sent invalid piece index {piece_index}")

        # Update attributes after obtaining new piece
        with self.data_lock:
            self.piece_count += 1
            self._set_bit(piece_index, self.bitfield)
            if piece_index in self.pieces_requested:
                self.pieces_requested.remove(piece_index)
        if self.received_bytes.get(peer_id):
            self.received_bytes[peer_id] += len(data)
        else:
            self.received_bytes[peer_id] = len(data)

        with self.bitfield_condition:
            self.bitfield_condition.notify_all()

        self._log_event(f"Peer {self.id} has downloaded the piece {piece_index} from {peer_id}. Now the number of"
                        f" pieces it has is {self.piece_count}.")

        self.file_complete = self.has_complete_file()
        #this can only run once as we will stop requesting once downloaded
        if self.file_complete:
            self._log_event(f"Peer {self.id} has downloaded the complete file.")

        # Broadcast new find to peers
        with self.connection_lock:
            for peer, psocket in list(self.connections.items()):
                try:
                    psocket.sendall(Message.create_message("have", struct.pack(">I", piece_index)))
                except:
                    pass
        # Evaluate interest after releasing lock
        for peer in list(self.peer_bitfields.keys()):
            if peer in self.connections:
                self._evaluate_interest(peer, self.connections[peer])

    def get_connected_peers(self):
        """Return list of currently connected peer IDs"""
        with self.connection_lock:
            return list(self.connections.keys())

    def has_complete_file(self):
        """Check if this peer has all pieces"""
        if self.bitfield == self.ideal_bitfield:
            return True
        return False

    def _peer_has_complete_file(self, peer_id):
        """Check if a specific peer has all pieces based on their bitfield"""
        bitfield = self.peer_bitfields.get(peer_id)
        if not bitfield:
            return False
        if bitfield == self.ideal_bitfield: #ideal starts complete with padding
            return True
        return False


    def all_peers_complete(self):
        """Check if ALL peers (including self) have the complete file"""
        # Check if we have complete file
        if not self.file_complete:
            return False

        # Check all known peers from config

        for peer_info in self.all_peers:
            peer_id = peer_info['id']
            if peer_id == self.id:
                continue  # Already checked self

            # If peer started with file, they're complete
            if peer_info['has_file']:
                continue

            # Check if we have their bitfield and if it's complete
            if peer_id in self.peer_bitfields:
                if not self._peer_has_complete_file(peer_id):
                    return False
            else:
                # We don't know their status - assume not complete
                # This handles peers we haven't connected to yet
                return False

        return True

    def calculate_download(self):
        """Calculate the download speed of every peer that has sent data in this interval """
        self.download_speeds = {}
        for peer_id in self.received_bytes:
            self.download_speeds[peer_id] = self.received_bytes[peer_id] / self.unchoking_interval

        # reset received_bytes for next interval
        self.received_bytes = dict.fromkeys(self.received_bytes, 0)

    def choose_preferred_neighbor(self):
        # Choke previous preferred neighbors
        old_neighbors = self.preferred_neighbors

        self.preferred_neighbors = []
        # If current peer has all the file, then it randomly chooses from those interested
        if self.file_complete:
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

        preferred_neigh_str = ""
        for peer_id in self.preferred_neighbors:
            preferred_neigh_str += str(peer_id) +", "
        preferred_neigh_str = preferred_neigh_str[:-2]

        # Neighbor order shouldn't matter if its still the same ones
        sorted(self.preferred_neighbors)
        sorted(old_neighbors)
        if old_neighbors != self.preferred_neighbors:
            self._log_event(f"Peer {self.id} has the preferred neighbors {preferred_neigh_str}")

        # Unchokes new neighbors
        with self.connection_lock:
            for peer_id in self.preferred_neighbors:
                if peer_id not in old_neighbors and peer_id != self.optimistic_neighbor:
                    if peer_id in self.connections:
                        try:
                            self.connections[peer_id].sendall(Message.create_message("unchoke"))
                        except:
                            pass

            # Chokes old neighbors not currently preferred or optimistic
            for peer_id in old_neighbors:
                if peer_id not in self.preferred_neighbors and peer_id != self.optimistic_neighbor:
                    if peer_id in self.connections:
                        try:
                            self.connections[peer_id].sendall(Message.create_message("choke"))
                        except:
                            pass

    def choose_optimistic_neighbor(self):
        candidates = []
        old_optimistic_neighbor = self.optimistic_neighbor
        for peer in self.interested_neighbors:
            # if it is not a preferred neighbor or the old optimistic it is choked
            if peer not in self.preferred_neighbors and not old_optimistic_neighbor:
                candidates.append(peer)

        if len(candidates) == 0:
            self.optimistic_neighbor = -1
        else:
            self.optimistic_neighbor = random.choice(candidates)

        if self.optimistic_neighbor != old_optimistic_neighbor:
            self._log_event(f"Peer {self.id} has the optimistically unchoked neighbor {self.optimistic_neighbor}")

        with self.connection_lock:
            if self.optimistic_neighbor != -1 and self.optimistic_neighbor != old_optimistic_neighbor:
                if self.optimistic_neighbor in self.connections:
                    try:
                        self.connections[self.optimistic_neighbor].sendall(Message.create_message("unchoke"))
                    except:
                        pass
            if (old_optimistic_neighbor != -1 and old_optimistic_neighbor != self.optimistic_neighbor and
                    old_optimistic_neighbor not in self.preferred_neighbors):
                if old_optimistic_neighbor in self.connections:
                    try:
                        self.connections[old_optimistic_neighbor].sendall(Message.create_message("choke"))
                    except:
                        pass

    def request_pieces(self, peer_id):
        while not self.choke_status.get(peer_id, True) and self.peer_interest_status.get(peer_id, False):

            bitfield = self.peer_interesting_bits.get(peer_id)
            new_piece = None
            count = 0
            with self.connection_lock:
                for index, byte in enumerate(bitfield):
                    if byte == 0:
                        continue

                    for bit_offset in range(8):
                        if byte & (1 << (7 - bit_offset)):
                            piece_index = index * 8 + bit_offset

                            if piece_index in self.pieces_requested:
                                continue

                            count += 1
                            if random.randrange(count) == 0:
                                new_piece = piece_index

                if peer_id in self.connections:
                    try:
                        self.connections[peer_id].sendall(Message.create_message("request", struct.pack(">I", new_piece)))
                        self.pieces_requested.append(new_piece)
                    except:
                        break
                else:
                    break

            with self.bitfield_condition:
                while self._get_bit(new_piece, self.bitfield) == 0:
                    self.bitfield_condition.wait()

            # Small delay to prevent CPU spin
            time.sleep(0.01)

    def read_file(self, piece_index, size):
        offset = piece_index * self.piece_size
        with open(self.file_path, 'rb') as file:
            file.seek(offset)
            data = file.read(size)
        return data

    def write_file(self, piece_index, data):
        offset = piece_index * self.piece_size
        with open(self.file_path, 'r+b') as file:
            file.seek(offset)
            file.write(data)


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
