"""
@Author: Zachary Jeffreys
This module creates a new chord_node and adds it to the network
"""
import sys
import hashlib
import csv
import threading
import pickle 
import socket
import time

M = 4  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
BASE_MIN = 50000
BASE_MAX = BASE_MIN + NODES # basically only allow dynamic ports within this range
HOST = 'localhost'

class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    <mrange [1,4)%100>
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    >>> [i for i in ModRange(0, 0, 5)]
    [0, 1, 2, 3, 4]
    """

    def __init__(self, start, stop, divisor=NODES):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe
    
    >>> fe.node = 1
    >>> fe
    
    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    @param n: current node id
    @param k: 1,2,4,8,16 .... 
    @param node: node to direct to 
    """
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node
        
    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        # return ''.format(self.start, self.next_start, self.node) #professors code wasn't showing anything
        formatted = "[{start} | {start}, {next_start} | {node}]".format(start = self.start, next_start = self.next_start, node = self.node)
        return formatted

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval

class ChordNode(object):
    """Use port number of previous to get node's id. If zero create new network"""
    def __init__(self, n, other = -1):
        self.node = n
        if(other == -1):
            self.finger = [None] + [FingerEntry(n, k, self.node) for k in range(1, M+1)]  # indexing starts at 1
            self.predecessor = self.node
        else:
            self.finger = [None] + [FingerEntry(n, k) for k in range(1, M+1)]  # indexing starts at 1
            self.predecessor = None
        self.keys = {} 
        
        if(other != -1):
            self.join(other)

        #just for debugging
        if(other == -1):
            print(self.finger)
        
        # Create a thread to handle listening for messages
        threading.Thread(target=self.start_dispatch, args=(n,)).start()  

        
    def join(self,node_in_network):
        self.initialize_finger_table(node_in_network) # init_finger_table(n')
        self.update_others()# update_others()

    def initialize_finger_table(self, node_in_network):
        print("Initializing Finger Table:")

        self.finger[1].node = self.call_rpc(node_in_network, 'find_successor', self.finger[1].start) 
        print("Node", self.node, "finger[1].node =", self.finger[1].node)

        self.predecessor = self.call_rpc(self.successor, 'predecessor') 
        print("Node", self.node, "predecessor =", self.predecessor)

        self.call_rpc(self.successor, 'predecessor', self.node) # Set successors predecessor to current node
        print("Node", self.node, "")

        for i in range(1,M ):
            if(self.finger[i + 1].start in ModRange(self.node, self.finger[i].node)):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = self.call_rpc(node_in_network, 'find_successor', self.finger[1].start) 
        print("After initialization: ", self.finger)

    def call_rpc(self, id, procedure, arg1=None, arg2=None):
        print("Calling RPC", id, procedure, arg1, arg2)
        if(self.node == id): # in case nodes successor to contact is itself
            return self.node
        address = ('localhost', BASE_MIN+id)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(address)
                sock.sendall(pickle.dumps((procedure, arg1, arg2)))
                return pickle.loads(sock.recv(BUF_SZ))
            except Exception as e:
                return "SOCKET ERROR"


    def start_dispatch(self, n):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind((HOST, BASE_MIN + n))
        listener.listen(BACKLOG)
        while True:
            client, addr = listener.accept()
            threading.Thread(target=self.handle_rpc, args=(client,)).start()   

    @property
    def successor(self):
        # print("returning successor for node ", self.node, ", Equals:", self.finger[1].node)
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    def find_successor(self, id):
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'successor')
    
    def find_predecessor(self, id):
        # while the id is not in the range from current node to successor 
        # where id is the node we are looking fors r pred
        np = int(self.node)
        while id not in ModRange(np+1, self.call_rpc(np, 'successor')+1):
            np = self.call_rpc(np, 'closest_preceding_finger', id)
        return np
    
    def handle_rpc(self, client):
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        
        print(self.finger)
        client.sendall(pickle.dumps(result))

    def dispatch_rpc(self, method, arg1, arg2):
        if method == "find_successor":
            succ = self.find_successor(arg1)
            return succ
        if method == "successor":
            return self.finger[1].node
        if method == 'predecessor':
            print("RPC=>", method, " called, arg1:", arg1, ", arg2: ", arg2)
            if arg1:
                self.predecessor = arg1
                print("updated node ", self.node, "to predecessor ", self.predecessor)
                return "OK"
            else:
                return self.predecessor
        if method == 'update_finger_table':
            print("update_finger_table called YES")
            exit(1)
        elif method == 'closest_preceding_finger':
            print('closest_preceding_finger')
            exit(1)
        # else:
        #     print(method)
        #     exit(202)
        elif hasattr(self, method):
            print (method, arg1, arg2)
            proc_method = getattr(self, method)

			# call the method according to how many arguments there are
            if arg1 and arg2:
                result = proc_method(arg1, arg2)
            elif arg1:
                result = proc_method(arg1)
            else:
                result = proc_method()
            return result
        else: 
            val = "invalid message >:/"
            print(val)
            return val

    
    def update_others(self):
        """ Update all other node that should have this node in their finger tables """
        for i in range(1, M):  # find last node p whose i-th finger might be this node
            # FIXME: bug in paper, have to add the 1 +
            p = self.find_predecessor((1 + self.node - 2**(i-1) + NODES) % NODES)
            print("RAWR", p)
            self.call_rpc(p, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """ if s is i-th finger of n, update this node's finger table with s """
        # FIXME: don't want e.g. [1, 1) which is the whole circle
        if (self.finger[i].start != self.finger[i].node
                 # FIXME: bug in paper, [.start
                 and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                     s, i, self.node, i, s, s, self.finger[i].start, self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # get first node preceding myself
            self.call_rpc(p, 'update_finger_table', s, i)
            return str(self)
        else:
            return 'did nothing {}'.format(self)

    def run(self):
        pass

def getNodeId(host, port): 
        """Sha-1 method for points"""
        """Not used for testing"""
        unencoded = host + "," + str(port)
        key = hashlib.sha1(str.encode(unencoded)).digest()
        key = int.from_bytes(key, 'little') % NODES 
        return key

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python chord_node.py n(Number between 0 and 15}")
        exit(1)

    
    if(len(sys.argv) == 2):
        print("Starting Network...\n")
        n = int(sys.argv[1])
        node = ChordNode(n) 
    if(len(sys.argv) == 3):
        print("Adding node ", sys.argv[1], "to network from node: ", sys.argv[2], "\n")
        n = int(sys.argv[1])
        node_in_network = int(sys.argv[2])
        node = ChordNode(n, node_in_network) 
    
   
    

    
    
    
   
    

        
       
    
    
