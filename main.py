import sys
import time
import signal
from peer import Peer


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\nShutting down peer...")
    if 'peer' in globals():
        peer.shutdown()
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    
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

    last_unchoke_time = time.time()
    last_optimistic_time = time.time()
    unchoke = False
    unchoke_optimistically = False

    try:
        while True:
            time.sleep(1)

            current_time = time.time()

            # Use config values for intervals (not hardcoded)
            if current_time - last_unchoke_time >= peer.unchoking_interval:
                last_unchoke_time = current_time
                unchoke = True

            if current_time - last_optimistic_time >= peer.optimistic_unchoking_interval:
                last_optimistic_time = current_time
                unchoke_optimistically = True

            # Calculate new preferred neighbors
            if unchoke:
                unchoke = False
                peer.choose_preferred_neighbor()
                peer.received_bytes.clear()

            # Pick a neighbor to unchoke optimistically
            if unchoke_optimistically:
                unchoke_optimistically = False
                peer.choose_optimistic_neighbor()

            connected = peer.get_connected_peers()
            print(f"[Peer {peer.id}] Active connections: {connected}, Pieces: {peer.piece_count}/{peer.num_of_pieces}")

            # Check termination condition: all peers have complete file
            if peer.all_peers_complete():
                print(f"\n[Peer {peer.id}] All peers have complete file. Terminating...")
                peer.shutdown()
                break

    except KeyboardInterrupt:
        peer.shutdown()
