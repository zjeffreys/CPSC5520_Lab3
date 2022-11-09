"""
@Author: Zachary Jeffreys
This module creates a populates an existing chord_node instance
"""
import sys

class Chord_Populate(object):
    def __init__(self, port, filename):
        print(port, filename)
       
   

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_node.py PORT_NUMBER FILENAME")
        exit(1)
    
    populate = Chord_Populate(sys.argv[1], sys.argv[2])