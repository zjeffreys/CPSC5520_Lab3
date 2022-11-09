"""
@Author: Zachary Jeffreys
This module creates a new chord_node and adds it to the network
"""
import sys

class Chord_Node(object):
    def __init__(self, port):
        if(port == 0):
            self.start_new_network()
        else:
            self.join_network(port)

    def start_new_network(self):
        print('starting new network')

    def join_network(self, port):
        print("joining network")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python chord_node.py PORT_NUMBER_EXISTING_NODE_OR_0")
        exit(1)
    
    node = Chord_Node(sys.argv[1])