from mpi4py import MPI
import numpy as np
import hashlib
import string
import random
import socket
import threading
import concurrent.futures
from queue import PriorityQueue
import time
import pickle
from merklelib import MerkleTree
import datetime
import json
import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkinter import messagebox

class Blockchain:
    def __init__(self,numofzeroes):
        self.chain = []
        self.numofzeroes = numofzeroes
        self.firsttime = True
        data = {'data':'0','previous_hash':'0','proof':'0','nodeID':'0'}
        self.create_block(data,0,(0,'0'),0,None)

    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys = True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

##Create block for block chain. The first block is a genesis block which has been initialized from constructor by default for every node. 
    def create_block(self,data,rank,result,size,comm):
        listdata = [data['data']]
        if len(self.chain) > 0:
            previous_hash = self.hash(self.chain[len(self.chain)-1])
        else:
            previous_hash = '000000000'
        root = MerkleTree(listdata)
        timestamp = str(datetime.datetime.now())
        block = {
                'header':
                    {
                        'block_version':len(self.chain) + 1,
                        'previous_hash':previous_hash,
                        'timestamp': timestamp,
                        'proof':data['proof'],
                        'merkle_tree':root.merkle_root
                    },
                'payload':
                    {
                        'nodeID':data['nodeID'],
                        'transaction':data['data']
                    }
                }
        current_hash = self.hash(block)
        block = {
                'header':
                    {
                        'block_version':len(self.chain) + 1,
                        'previous_hash':previous_hash,
                        'timestamp': timestamp,
                        'proof':data['proof'],
                        'merkle_tree':root.merkle_root
                    },
                'payload':
                    {
                        'currentHash':current_hash,
                        'nodeID':data['nodeID'],
                        'transaction':data['data']
                    }
                }
        #If genesis block just append to the chain else send the block to all the other nodes for verification and adding in their respective chain
        if self.firsttime == False:
            self.send_and_receive_block(rank,result,size,comm,block)
        else:
            self.chain.append(block)
            self.firsttime = False
    #Checking if the block is valid by verifying proof work with input given at the starting, The checking if hash the block is correct and block is not manipulated
    def is_valid_block(self,block):
        input_string = block['payload']['transaction']
        proof = block['header']['proof']
        temp_string = input_string + proof
        temp_static_hash = hashlib.md5(temp_string.encode()).hexdigest()
        temp_static_hash_binary = bin(int(temp_static_hash,16))
        if int(temp_static_hash_binary[-1*self.numofzeroes:]) != 0:
            return False

        temp_block = {
                'header':
                    {
                        'block_version':block['header']['block_version'],
                        'previous_hash':block['header']['previous_hash'],
                        'timestamp': block['header']['timestamp'],
                        'proof':block['header']['proof'],
                        'merkle_tree':block['header']['merkle_tree']
                    },
                'payload':
                    {
                        'nodeID':block['payload']['nodeID'],
                        'transaction':block['payload']['transaction']
                    }
                }

        if block['payload']['currentHash'] != self.hash(temp_block):
            return False
        return True
    #receiving block from the winner node for verification and adding to respective chain of nodes. 
    def send_and_receive_block(self,rank,result,size,comm,block):
        #Winner node sending block to everyone
        if rank == result[1]:
            for i in range(1,size):
                if i!=rank:
                    comm.send(block,dest=i,tag=14)
        temp_block = None
        sum_count = 0
        #If the received block is correctly verified by a node, then it sends count = 0 to the winner node else count =1 to winner node.
        if rank != result[1]:
            temp_block = comm.recv(temp_block,source=result[1],tag=14)
            if self.is_valid_block(temp_block):
                count = 0
                comm.send(count,dest=result[1],tag=14)
            else:
                count=1
                comm.send(count,dest=result[1],tag=14)
        #Winner node receiving count from every other node after their verification and summing up the count. So that if the sum count is greater than 1 then block is not verified by one or multiple nodes
        #if sum count = 0 that mean every node correctly verifies the block
        #After winner node then broad cast sum count to every other node. And if sum count = 0 then only all the nodes including winner node add the block to the chain
        if rank == result[1]:
            for i in range(1,size):
                if i != result[1]:
                    temp_count = None
                    temp_count = comm.recv(temp_count,source=i)
                    sum_count = sum_count+temp_count
        sum_count = comm.bcast(sum_count,root=result[1])
        if sum_count == 0:   
            if rank == result[1]:
                self.chain.append(block)
            else:
                self.chain.append(temp_block)

    #Chain received from other node to resolve conflict is getting verified by the node who has received the chain.
    def is_chain_valid(self, chain):
        previous_block = chain[0]
        block_index = 1
        while block_index < len(chain):
            block = chain[block_index]
            if block['header']['previous_hash'] != self.hash(previous_block):
                return False
            current_proof = block['header']['proof']
            transaction = block['payload']['transaction']
            temp_string = transaction + current_proof
            temp_static_hash = hashlib.md5(temp_string.encode()).hexdigest()
            temp_static_hash_binary = bin(int(temp_static_hash,16))
            if int(temp_static_hash_binary[-1*self.numofzeroes:]) != 0:
                return False
            previous_block = block
            block_index += 1
        return True
    #Sometimes node may be unable to add the node to its chain because of message failure or node failure. So resolve conflicts ensures that every node has same blockchain
    #before adding the block to its chain.
    def resolev_conflicts(self,rank,comm):
        chain_pq = PriorityQueue()
        chain_pq_copy = PriorityQueue()
        chain_length = len(self.chain)
        chain_pq.put((chain_length,rank))
        #Every node sends its chain length to everyone.
        for i in range(1,size):
            if i != rank:
                leng_tuple = (chain_length,rank)
                comm.send(leng_tuple,dest=i,tag=14)
        #Every node receives chain length from everyone and put it into the priority queue which add the (chain length, rank) tuple in the ncreasing order of chain length.
        for i in range(1,size):
            if i != rank:
                leng_tuple = None
                leng_tuple = comm.recv(leng_tuple,source=i,tag=14)
                chain_pq.put(leng_tuple)
        for i in chain_pq.queue:
            chain_pq_copy.put(i)
        while(chain_pq.qsize() > 1):
                temp = chain_pq.get()
        maxchain = chain_pq.get()
        #The node which has maximum length chain it sends the chain to everyone.
        if chain_length == maxchain[0] and rank == maxchain[1]:
            for i in chain_pq_copy.queue:
                if i[0] < chain_length:
                    comm.send(self.chain,dest=i[1],tag=14)
        #All the nodes which has chain length shorter than the maximum chain length, they replace their chains with the maximum length chain
        if chain_length < maxchain[0]:
            temp_chain = None
            temp_chain = comm.recv(temp_chain,source=maxchain[1],tag=14)
            if self.is_chain_valid(temp_chain):
                self.chain = temp_chain

    def return_lastblock(self):
        global total_reward
        lastblock = self.chain[len(self.chain)-1]
        data = {
                'numofblocks':len(self.chain),
                'timestamp':lastblock['header']['timestamp'],
                'merkle_root':lastblock['header']['merkle_tree'],
                'proof':lastblock['header']['proof'],
                'currentHash':lastblock['payload']['currentHash'],
                'reward':total_reward
                }
        return data

    def print_blockchain(self, rank):
        print('Rank ',rank)
        for i in self.chain:
            print(i)
#Every node always listens in thread if any node sending pow to it after sucessfully finding it.
def get_pow(port):
    global pq
    soc = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    soc.bind(("localhost", port))
    soc.listen(5)
    while True:
        connection, connection_info = soc.accept()
        message = connection.recv(1024)
        data = pickle.loads(message)
        q_data = (data['timestamp'],data['rank'],data['suffix'])
        pq.put(q_data)
        connection.close()
#Every node sends its pow along with timestamp to all the nodes.
def send_pow(current_timestamp, rank, suffix):
    global size, prefix_port
    for i in range(1,size):
        if i != rank:
            soc = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            soc.bind(('localhost',0))
            port = prefix_port+str(i)
            port = int(port)
            soc.connect(("localhost", port))
            data = {'timestamp':current_timestamp,'rank':rank,'suffix':suffix}
            data_byte = pickle.dumps(data)
            time.sleep(0.5)
            soc.sendall(data_byte)
            soc.close()
#All the other nodes except winner node verifies if pow recevied from winner node is a correct pow. 
def check_suffix(current_timestamp, rank, suffix, input_string):
    global size, numofzeroes, pq
    send_pow(current_timestamp,rank,suffix)
    while(pq.qsize() != size-1):
        pass
    result = pq.get()
    count = 0
    temp_suffix = result[2]
    temp_string = input_string + temp_suffix
    temp_static_hash = hashlib.md5(temp_string.encode()).hexdigest()
    temp_static_hash_binary = bin(int(temp_static_hash,16))
    if int(temp_static_hash_binary[-1*numofzeroes:]) != 0:
        count = 1
    sum_count = count
    #Every node if it successfully verifies the pow then send count = 0 to all the other nodes else sends count =1 to the other nodes.
    for i in range(1,size):
        if(i != rank):
            comm.send(count,dest=i,tag = 14)
    for i in range(1,size):
        if(i != rank):
            temp = None
            temp = comm.recv(temp,source=i,tag=14)
            sum_count = sum_count + temp
    #After collecting count from all the nodes, each nodes sums up the count. And if sum count = 0, then only accept the pow else rejects the pow. 
    if sum_count == 0:
        pq.put(result)
        return True
    else:
        while(pq.empty() == False):
            temp = pq.get()
        return False

def mine_block():
    global numofzeroes, rank, comm, pq, total_reward, reward
    if not pq.empty():
        while not pq.empty():
            temp = pq.get()
    input_string= None
    input_string = comm.recv(input_string,source=0)
    static_hash = hashlib.md5(input_string.encode()).hexdigest()
    static_hash_binary = bin(int(static_hash,16))
    suffix =random.randint(0,1000000)
    suffix_found = 'None'
    current_timestamp = None
    while(True):
        temp_suffix = str(suffix)
        temp_string = input_string + temp_suffix
        temp_static_hash = hashlib.md5(temp_string.encode()).hexdigest()
        temp_static_hash_binary = bin(int(temp_static_hash,16))
        #If the node finds out that any othe node finds out the pow and it has received it from the winner node, then it immediately checks the pow.
        if(pq.qsize() >= 1):
            current_timestamp = time.time()
            check = check_suffix(current_timestamp,rank,suffix_found,input_string)
            if check == True:
                break
            else:
                suffix =random.randint(0,1000000)
        if int(temp_static_hash_binary[-1*numofzeroes:]) == 0:
            suffix_found = temp_suffix
            current_timestamp = time.time()
            pq.put((current_timestamp,rank,suffix_found))
            check = check_suffix(current_timestamp,rank,suffix_found,input_string)
            if check == True:
                break
            else:
                suffix =random.randint(0,1000000)
        suffix = suffix+1

    result = pq.get()
    blockchain.resolev_conflicts(rank,comm) 
    data = {'data':input_string,'proof':result[2],'nodeID':rank}
    blockchain.create_block(data,rank,result,size,comm)
    if result[1] == rank:
        total_reward = total_reward + reward
    return result
    

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
num_of_proc = size
input_string = None
numofzeroes = 4
pq = PriorityQueue()
prefix_port = '142'
total_reward = 0
reward = 0

def extract(lab):
	return lab.get()

def extract_reward_and_input(ent_sr,ent_inp):
    global reward, comm, size
    reward = int(extract(ent_sr))  
    inp = extract(ent_inp)   
    input_string = inp
    choice = 'mine'
    for i in range(1,size):
        comm.send(choice, dest=i)
        comm.send(reward,dest=i)
    for i in range(1,size):
        comm.send(input_string, dest=i)
    final_msg = None
    for i in range(1,size):
        msg = None
        msg = comm.recv(msg,source=i)
        final_msg = msg
    messagebox.showinfo("showinfo", final_msg)

def openNewWindow(root1,minerId):
    global comm, reward
    choice = 'details'
    comm.send(choice,dest=int(minerId))
    comm.send(reward,dest=int(minerId))
    block = None
    block = comm.recv(block,source=int(minerId),tag=14)
    newWindow = Toplevel(root1)
    newWindow.title("Miner Details")
    label = tk.Label(newWindow, text="Miner Details", font=("Arial",30)).grid(row=0, columnspan=3)
    cols = ('ID', 'Total Blocks', 'Timestamp', 'Merkle Tree','Last PoW','Hash of Block','Reward')
    listBox = ttk.Treeview(newWindow, columns=cols, show='headings')
    for col in cols:
    		listBox.heading(col, text=col)    
    listBox.grid(row=1, column=0, columnspan=2)
    listBox.insert("", "end", values=(minerId, block['numofblocks'], block['timestamp'],block['merkle_root'],block['proof'],block['currentHash'],block['reward']))

def GUIMainLoop(root):
    root.mainloop()

if rank == 0:
	root1 = Tk()
	root1.title("Distributed Block Chain")
	#row 0
	sr_lab = Label(root1,text="set reward : ",anchor='w',width=22,font=("Arial",20)).grid(row=0,column=0)
	ent_sr = Entry(root1)
	ent_sr.grid(row=0,column=1)

	#row 1
	input_lab = Label(root1,text="Enter input : ",anchor='w',width=22,font=("Arial",20)).grid(row=1,column=0)
	ent_inp = Entry(root1)
	ent_inp.grid(row=1,column=1)

	#row 2
	min_but = Button(root1, text = 'Mine',font=("Arial",20),
		  command=(lambda : extract_reward_and_input(ent_sr,ent_inp)))
	min_but.grid(row=2,column=0,columnspan=2)

	# row 3
	id_lab = Label(root1,text="select id : ",anchor='w',width=22,font=("Arial",20)).grid(row=3,column=0)
	id_combobox = ttk.Combobox(root1)
	id_combobox['values'] = [str(i) for i in range(1,num_of_proc)]
	id_combobox.grid(row=3,column=1)

	#row-4
	show_btn = Button(root1, text = 'show details',font=("Arial",20), command=(lambda : openNewWindow(root1,id_combobox.get())))
	show_btn.grid(row=4,column=0,columnspan=2)
	root1.mainloop()
else:
    blockchain = Blockchain(numofzeroes)
    port = prefix_port+str(rank)
    port = int(port)
    t1 = threading.Thread(target=get_pow, args=(port,))
    t1.start()
    while True:
        choice = None
        temp_reward = None
        choice = comm.recv(choice, source=0)
        temp_reward = comm.recv(temp_reward,source=0)
        reward = temp_reward
        if choice == 'mine':
            result = None
            result = mine_block()
            message = "Minning Done. Winner Node "+str(result[1])
            comm.send(message,dest=0)
        elif choice == 'details':
            block = None
            block = blockchain.return_lastblock()
            comm.send(block,dest=0,tag=14)