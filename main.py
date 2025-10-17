import sys
import socket
import message
from peer import Peer



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Make sure you add an argument (peer id) when running!
if __name__ == '__main__':
    print_hi('PyCharm')
    peer = Peer(int(sys.argv[1]))

    # test
    print("Hello User from " + peer.host_name + " connecting through port " + str(peer.port))
    print("Are you ready to download " + peer.file_name + "?")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
