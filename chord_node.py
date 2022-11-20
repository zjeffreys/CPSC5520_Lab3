"""
@Author: Zachary Jeffreys


Hello grader (or anyone else reading), 
I was not able to get the nodes to properly change the 
finger table nodes before this assignments was due. However, this 
assignment you are able to type python3 chord_node.py 0 to start a new network
or python3 chord_node.py port, to start a new node. I used 
the Test_Base method for my port lookup, but I ended up using 
Base_Min = 50_000 for simplicity. 
"""
import sys
import hashlib
import csv
import threading
import pickle 
import socket
import time

M = 7  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M # Network must have at least 128 possible nodes. (M=7, NODES=2^^7=128)
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
    def __init__(self, n, remote = False):
        self.node = n
        if(remote == False):
            self.finger = [None] + [FingerEntry(n, k, self.node) for k in range(1, M+1)]  # indexing starts at 1
            self.predecessor = self.node
            print("# setting predecessor, successor, and finger table entries appropriately")
            self.print_info()
        else:
            self.finger = [None] + [FingerEntry(n, k) for k in range(1, M+1)]  # indexing starts at 1
            self.predecessor = None
            print("Adding Node: ", self.node)
        self.keys = {} 
        
        if(remote):
            print("adding remote node to network...")
            self.join(0)

        
        # Create a thread to handle listening for messages
        print("# starting dispatch loop...")
        # assume remote true means it is not the first
        threading.Thread(target=self.start_dispatch, args=(n,remote)).start()  

    def print_info(self, message=""):
        print()
        print(message)
        print("Successor:", self.successor)
        print("Predecessor:", self.predecessor)
        for index, each in enumerate(self.finger):
            if(index != 0):
                print(each)
        print()
    
    def join(self,node_in_network):
        self.initialize_finger_table(node_in_network) # init_finger_table(n')
        self.update_others()# update_others()
        self.print_info("After Joining Network")

    def initialize_finger_table(self, node_in_network):
        print("Initializing Finger Table:")

        self.finger[1].node = self.call_rpc(node_in_network, 'find_successor', self.finger[1].start) 
        print("Node", self.node, "finger[1].node =", self.finger[1].node)

        self.predecessor = self.call_rpc(self.successor, 'predecessor') 
        print("Node", self.node, "predecessor =", self.predecessor)

        self.call_rpc(self.successor, 'predecessor', self.node) # Set successors predecessor to current node
        print("Node", self.node, "")

        print("Initializing the rest of Node(", self.node, ") Fingers")
        for i in range(1,M): # Question) SHould this be M - 1?
            print(i, "start:", self.finger[i + 1].start, " ModRange(", self.node, ",",self.finger[1].node,")")
            if(self.finger[i + 1].start in ModRange(self.node, self.finger[i].node)):
                print("TRUE, it is in range")
                self.finger[i+1].node = self.finger[i].node
            else:
                print("False, is NOT in range")
                before = self.finger[i + 1].node
                self.finger[i + 1].node = self.call_rpc(node_in_network, 'find_successor', self.finger[i + 1].start) 
                print("Successor of before (", before, ") equals (", self.finger[i + 1].node, ")")
        self.print_info("After initialization:" )

    def call_rpc(self, id, procedure, arg1=None, arg2=None):
        # print("Calling RPC", id, procedure, arg1, arg2)
        if(self.node == id): # in case nodes successor to contact is itself
            # print("i am my own successor, e.g. first node in network")
            return self.node
        address = ('localhost', BASE_MIN+id)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(address)
                sock.sendall(pickle.dumps((procedure, arg1, arg2)))
                return pickle.loads(sock.recv(BUF_SZ))
            except Exception as e:
                return "SOCKET ERROR"


    def start_dispatch(self, n, remote):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if(remote == False):
            listener.bind((HOST, BASE_MIN))
        else:
            listener.bind((HOST, BASE_MIN + n))
        listener.listen(BACKLOG)
        print("Listening on PORT: ", listener.getsockname()[1])
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
        np = int(self.node)
        print("find_predecessor()")
        print("Node (", self.node, "), While(", id, ") not in ModRange(", np+1, ",", self.successor +1, "): ") 
        while id not in ModRange(np+1, self.call_rpc(np, 'successor')+1): # Question) is my error here? 
        # while id not in ModRange(np+1, self.successor):
        #     print("closest preceding finger called")
            np = self.call_rpc(np, 'closest_preceding_finger', id)
        print("returning ", np)
        return np

    
    def handle_rpc(self, client):
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        # print("n': ", method, arg1, arg2)
        client.sendall(pickle.dumps(result))

    def dispatch_rpc(self, method, arg1, arg2):
        print("Node(", self.node,") called (", method, ") with args (", arg1, ", ", arg2, ")")
        if method == "find_successor":
            succ = self.find_successor(arg1)
            print("Node(", self.node, ") returned the value (", succ, ")")
            return succ
        if method == "successor":
            succ = self.finger[1].node
            print("Node(", self.node, ") returned the value (", succ, ")")
            return succ
        if method == 'predecessor':
            if arg1:
                if(self.predecessor == self.successor): # BREAKING PROGRAM BUT WHY? only when adding the first node
                    self.finger[1].node = arg1
                self.predecessor = arg1
                print("updated node ", self.node, "to predecessor ", self.predecessor)

                self.print_info()
                return "OK"
            else:
                pred = self.predecessor
                print("Node(", self.node, ") returned the value (", pred, ")")
                return self.predecessor

        elif method == 'find_key': # If ou
            print("looking up key: ", arg1)
        
        elif method == 'populate_node':
            print("pop it")

        elif method == 'update_finger_table':
            print("update_finger_table called, why isn't this reaching here ahhh")
            exit(1)
        elif method == 'closest_preceding_finger': # Error Fix Me
            m = M 
            while m > 1:
                if(self.finger[m].node in ModRange(self.node, arg1)):
                    return self.finger[m].node
                m = m - 1
            return self.node
    
        elif hasattr(self, method):
            "WOW hasattr called"
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
        print("END")
        print(self.node, "=>", self.successor)

class NodeLocation(object):
    def __init__(self, host, port) -> None:
        self.endpoint = host 
        self.port = int(port)
        self.hashed = self.getHash(host+','+str(port))
        print("# Finding appropriate point int the network to add it based on SHA1 hash...")
        print("# hashing", self.endpoint, "+", self.port, "=> SHA1", self.hashed, "=", self.hashed % NODES)

    @staticmethod
    def getHash(str):
        sha1 = hashlib.sha1()
        sha1.update(str.encode('utf-8'))
        result = sha1.hexdigest()
        return int(result, 16)

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python chord_node.py 0_or_port_number")
        exit(1)

    if(sys.argv[1] == '0'):
        print("\n# Add first node to start a new network.")
        print("# Max Network Size:", NODES)
        node = ChordNode(0)
    else: 
        print("\nAdding additional node to the network, starting from active node.")
        # Find the appropriate point in the network to add it based on SHA1 hash of node name.
        point_in_network = NodeLocation(HOST, int(sys.argv[1]))
        node = ChordNode(point_in_network.hashed % NODES, True)
        
    

   
    

    
    
    
   
    

        
       
    
    
