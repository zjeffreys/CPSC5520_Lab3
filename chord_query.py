"""
@Author: Zachary Jeffreys
This module creates a queries data from an existing node
"""
import sys

class Chord_Query(object):
    def __init__(self, port, key):
        print(port, key)

   

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_node.py PORT_NUMBER KEY")
        exit(1)
    
    query = Chord_Query(sys.argv[1], sys.argv[2])