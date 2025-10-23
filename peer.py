import os
import socket
import threading
import time
from datetime import datetime
from message import Message
import random

class Peer:
    def __init__(self, id):
        self.id = id
        self.peers = []
        
        self.connections = {}
        self.connection_lock = threading.Lock()
        self.server_socket = None
        self.server_thread = None
        self.all_peers = []
        self.running = True
        config = open('Common.cfg')
        setup = config.read().split()
        config.close()

        self.neighbor_count = int(setup[1])
        self.unchoking_interval = float(setup[3])
        self.optimistic_unchoking_interval = float(setup[5])
        self.file_name = setup[7]
        self.file_size = float(setup[9])
        self.piece_size = float(setup[11])

        # Sets up identifying information for the peer
        config = open('PeerInfo.cfg')
        setup = config.read().splitlines()
        selected_line = ''
        for line in setup:
            info = line.split()
            if (int(info[0]) == self.id):
                selected_line = line.split()
        config.close()

        self.host_name = selected_line[1]
        self.port = int(selected_line[2])
        self.file_complete = bool(int(selected_line[3]))

        if not os.path.exists("peer_" + str(self.id)):
            os.mkdir("peer_" + str(self.id))

        # Overwrites log, makes it better for testing. Check if it needs to be appended in docs later.
        self.log_file = open("log_peer_" + str(self.id) + ".log", "w")

        self.received_bytes = {}
        self.download_speeds = {}
        self.interested_neighbors = []
        self.optimistic_neighbor = 0
        self.preferred_neighbors = []
        
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
            print(f"[Peer {self.id}] Loaded {len(self.all_peers)} peers from config")
        except Exception as e:
            print(f"[Peer {self.id}] Error reading peer info: {e}")
    
    def _start_server(self):
        """Start listening socket in background thread"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host_name, self.port))
            self.server_socket.listen(10)
            
            print(f"[Peer {self.id}] Server listening on {self.host_name}:{self.port}")
            
            self.server_thread = threading.Thread(target=self._accept_connections, daemon=True)
            self.server_thread.start()
            
        except Exception as e:
            print(f"[Peer {self.id}] Error starting server: {e}")
    
    def _accept_connections(self):
        """Accept incoming connections (runs in thread)"""
        print(f"[Peer {self.id}] Waiting for incoming connections...")
        
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                
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
            self.log_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Peer " + str(self.id) + " is connected from Peer " + str(peer_id))
            
            # Step 2: Send handshake response
            response = Message.create_handshake(self.id)
            client_socket.sendall(response)
            print(f"[Peer {self.id}] Sent handshake to Peer {peer_id}")
            
            # Store connection
            with self.connection_lock:
                self.connections[peer_id] = client_socket
            
            # TODO: Exchange bitfield messages next
            
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            print(f"[Peer {self.id}] Error handling incoming connection: {e}")
        finally:
            if 'peer_id' in locals() and peer_id:
                with self.connection_lock:
                    if peer_id in self.connections:
                        del self.connections[peer_id]
            client_socket.close()
    
    def _connect_to_previous_peers(self):
        """Connect to all peers that started before this one"""
        print(f"[Peer {self.id}] Connecting to previous peers...")
        
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
            peer_socket.settimeout(10.0)
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
            self.log_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Peer " + str(self.id) + " makes a connection to Peer " + str(peer_id))
            
            # TODO: Exchange bitfield messages next
            
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            print(f"[Peer {self.id}] Error in connection with Peer {peer_id}: {e}")
        finally:
            with self.connection_lock:
                if peer_id in self.connections:
                    del self.connections[peer_id]
            peer_socket.close()
            print(f"[Peer {self.id}] Closed connection with Peer {peer_id}")
    
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
        self.preferred_neighbors = []

        self.calculate_download()

        neighbors = list(self.download_speeds.items())
        random.shuffle(neighbors)  # Shuffle to handle ties randomly
        neighbors.sort(key=lambda x: x[1], reverse=True)  # Sort by download speed descending

        top_neighbors = neighbors[:self.neighbor_count]
        self.preferred_peers = [peer_id for peer_id, _ in top_neighbors]

    def choose_optimistic_neighbor(self):
        candidates = []
        for peer in self.interested_neighbors:
            # if it is not a preferred neighbor it is choked
            if peer not in self.preferred_neighbors:
                candidates.append(peer)

        if len(candidates) == 0:
            self.optimistic_neighbor = 0
        else:
            self.optimistic_neighbor = random.choice(candidates)


    
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
