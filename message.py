import struct

type2number = {
    "choke": 0,
    "unchoke": 1,
    "interested": 2,
    "not interested": 3,
    "have": 4,
    "bitfield": 5,
    "request": 6,
    "piece": 7
}

number2type = {v: k for k, v in type2number.items()}


class Message:
    """Handles P2P protocol messages"""
    
    @staticmethod
    def create_handshake(peer_id):
        """
        Create handshake message
        Format: 18-byte header "P2PFILESHARINGPROJ" + 10 zero bytes + 4-byte peer ID
        Total: 32 bytes
        """
        header = b"P2PFILESHARINGPROJ"  # 18 bytes
        zero_bits = b"\x00" * 10          # 10 bytes
        peer_id_bytes = struct.pack('>I', peer_id)  # 4 bytes, big-endian
        
        return header + zero_bits + peer_id_bytes
    
    @staticmethod
    def parse_handshake(data):
        """
        Parse handshake message
        Returns: peer_id if valid, None if invalid
        """
        if len(data) != 32:
            return None
        
        header = data[:18]
        if header != b"P2PFILESHARINGPROJ":
            return None
        
        peer_id = struct.unpack('>I', data[28:32])[0]
        return peer_id
    
    @staticmethod
    def create_message(msg_type, payload=b""):
        """
        Create actual message (after handshake)
        Format: 4-byte length + 1-byte type + payload
        """
        if isinstance(msg_type, str):
            msg_type = type2number[msg_type]
        
        message_length = 1 + len(payload)

        length_bytes = struct.pack('>I', message_length)
        type_byte = struct.pack('B', msg_type)
        
        return length_bytes + type_byte + payload
    
    @staticmethod
    def parse_message(data):
        """
        Parse actual message
        Returns: (message_type, payload) or None if invalid
        """
        if len(data) < 5:
            return None
        
        message_length = struct.unpack('>I', data[:4])[0]
        msg_type = struct.unpack('B', data[4:5])[0]
        
        if msg_type in number2type:
            payload = data[5:5+message_length-1]
            return (number2type[msg_type], payload)
        
        return None
