import sys
import socket
import message
import time
import signal
from peer import Peer


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\nShutting down peer...")
    if 'peer' in globals():
        peer.shutdown()
    sys.exit(0)


def print_hi(name):
    print(f'Hi, {name}')


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    
    print_hi('PyCharm')
    
    if len(sys.argv) < 2:
        print("Usage: python main.py <peer_id>")
        print("Example: python main.py 1001")
        sys.exit(1)
    
    peer = Peer(int(sys.argv[1]))

    print("\n" + "="*50)
    print(f"Peer {peer.id} Started Successfully!")
    print("="*50)
    print(f"Host: {peer.host_name}")
    print(f"Port: {peer.port}")
    print(f"Has complete file: {peer.file_complete}")
    print(f"File: {peer.file_name} ({peer.file_size} bytes)")
    print(f"Piece size: {peer.piece_size} bytes")
    print("="*50)
    
    print("\nWaiting for connections to establish...")
    time.sleep(2)
    
    connected_peers = peer.get_connected_peers()
    print(f"\n[Peer {peer.id}] Connected to {len(connected_peers)} peer(s): {connected_peers}")
    
    print(f"\n[Peer {peer.id}] Running... Press Ctrl+C to exit")
    try:
        while True:
            last_unchoke_time = time.time()
            last_optimistic_time = time.time()

            current_time = time.time()
            if current_time - last_unchoke_time >= peer.unchoking_interval:
                last_unchoke_time = current_time

            if current_time - last_optimistic_time >= peer.optimistic_unchoking_interval:
                peer.choose_optimistic_neighbor()
                last_optimistic_time = current_time

            connected = peer.get_connected_peers()
            print(f"[Peer {peer.id}] Active connections: {connected}")
    except KeyboardInterrupt:
        peer.shutdown()