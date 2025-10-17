import os

class Peer:
    def __init__(self, id):
        self.id = id
        self.peers = []

        # Reads config file to set up behavior (assuming structure of it doesn't change)
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

        # Creates directory to download file to
        os.mkdir("peer_" + str(self.id))


