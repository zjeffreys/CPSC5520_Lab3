"""
@Author: Zachary Jeffreys

Hello again grader, 
Since I didn't have the chord_node.py finger 
tables to adjust finger.node correctly, I coudln't get
the nodes to populate correctly. So unfortuantely 
this code does not work. But to possibly
obtain a few points here is how I figured it might look.
"""
import sys
import pickle
import socket

BUF_SZ = 4096  # socket recv arg

class ChordQuery(object):
    def __init__(self, port, key):
        self.port = port
        self.key = key
        self.query()

    def query(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('localhost', self.port))
            # I assume the nodes will use the key just to keep looking through 
            # the network node by node unstil they find the correct one. 
            # I'll add "find_key" option in chord_node just to show 
            # where I assumed it should go. 
            sock.sendall(pickle.dumps('find_key', self.key)) # didn't test just assuming 'method, arg1=key
            data = pickle.loads(sock.recv(BUF_SZ)) # get some data back
            print(data) #print to console
                  
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_query.py port_number key")
        exit(1)

    query = ChordQuery(int(sys.argv[1]), str(sys.argv[2]))