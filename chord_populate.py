"""
@Author: Zachary Jeffreys


Hello grader, 
I was not able to get my chord nodes to update their tables correctly so 
I was not able to connet this chord_populate.py to the chord_node.py. I 
figured I would still sumbit the parsing of the file to hopefully gain 
some points for this part. 

What this file does show, is that I parsed the Career_Stats_Passing.csv
file on row['play_Id'] and row['year']. I then used the SHA1
hash to create a dictionary of key value pairs ready to be 
added (adjusted) then added to the network. 

"""
import hashlib
import socket
import sys
import csv
import pickle

M = 7


class Chord_Populate(object):
    def __init__(self, port, filename):
        self.port = port
        self.dictionary = dict()
        

    def generate_hash(self, str):
        sha1 = hashlib.sha1()
        sha1.update(str.encode('utf-8'))
        result = sha1.hexdigest()
        return int(result, 16)
        
    def parse(self, filename): 
        with open(filename, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in reader:
                val = str(row[0]+ ","+ row[1])
                self.dictionary[self.generate_hash(val) % 2**7] = self.generate_hash(val)
                #self.dictionary.append({self.generate_hash(val) % 2**7: self.generate_hash(val)})
    
    
    def send_dict(self, dict):
        print('Sending dictionary...')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('localhost', self.port))
            sock.sendall(pickle.dumps('dictionary {}'.format(dict)))
            #print('Received response = {}'.format(pickle.loads(sock.recv(4096))))


    def insert_key_val(self, key, val):
        print('Inserting key = {k} and val = {v}'.format(key=key, v=val))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('localhost', self.port))
            sock.sendall(pickle.dumps('pop_it', key, val))# arg1 = key, arg2 = value
            print('Received response = {}'.format(pickle.loads(sock.recv(4096))))
    
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py port_number default")
        exit(1)
    
    
    file = "Career_Stats_Passing.csv"
    if(sys.argv[2] != "default"):
        print("two:, ", sys.argv[2])
        file = sys.argv[2]
    populator = Chord_Populate(sys.argv[1], sys.argv[2])
    populator.parse(file)
    for key in populator.dictionary:
        print(key, populator.dictionary[key])