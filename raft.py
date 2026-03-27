import socket
import struct
import json
from utils import receive_message, send_message, sender, listener, connect
from enum import Enum, auto
import multiprocessing
import time

ALL_NODES = [5000,5001]

class NodeRole(Enum):
    LEADER = auto()
    FOLLOWER = auto()
    CANDIDATE = auto()

class KVStore:
    def __init__(self):
        self.kv = {}
        self.log = []
        self.current_term = 0
        self.commit_index = -1
        self.last_applied = -1
        self.voted_for = None
        self.next_index = {node:0 for node in ALL_NODES}

    def append_log(self, entry):
        for i in range(len(entry)):
            self.log.append({
                "index": self.commit_index + 1 + i,
                "term" : self.kv.current_term,
                "entry" : entry[i]
            })

    def apply_loop(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.apply(self.log[self.last_applied]["command"])

    def apply(self, command):
        op = command["method"]
        key = command["params"]["key"]
        value = command["params"].get("value")

        if op == "SET":
            self.kv[key] = value
        elif op == "DELETE":
            self.kv.pop(key, None)

    def read(self, key):
        return self.kv.get(key)
    
    def increase_next_index(self, id):
        self.next_index[id] += 1
    
class Raft:
    def __init__(self, host, port, kv:KVStore, timeout):
        self.host = host
        self.port = port
        self.role = NodeRole.FOLLOWER
        self.timeout = timeout
        self.curr_time = time.time()
        self.commit_counter = 0
        self.kv = kv
        self.peers = [i for i in ALL_NODES if i != port]
        self.server = listener(host, port)
        self.conn = None
        self.vote_count = 0
        self.catalog = {
            "CLIENT" : self.client,
            "APPEND_ENTRIES": self.append_entries,
            "BECOME_FOLLOWER": self.become_follower,
            "HIGHER_TERM" : self.higher_term,
            "COMMIT": self.commit,
            "RESPOND_VOTE": self.respond_vote,
            "GRANT_VOTE" : self.grant_vote,
        }


    def receive_message(self):
        conn = connect(self.server)
        return receive_message(conn)
    
    def apply_message(self, msg):

        method = msg.get("method")
        params = msg.get("params")

        self.catalog[method](**params)

    def client(self, entry):
        self.reset_commit_counter()

        self.kv.append_log([entry])

        for i in self.peers:
            conn = sender(self.host, i)
            prev_index = self.kv.next_index[i] - 1
            prev_term = self.kv.log[prev_index]["term"]
            msg = {
                "method" :  "APPEND_ENTRIES",
                "params" : {
                    "node_id" : self.port,
                    "curr_term" : self.kv.current_term,
                    "prev_term" : prev_term,
                    "prev_index": prev_index,
                    "entry" : self.kv.log[self.kv.next_index[i]:]
                }
            }
            send_message(conn, msg)
    
    def append_entries(self, node_id, curr_term, prev_term, prev_index, entry):
        self.reset_time()

        if curr_term < self.kv.current_term:
            conn = sender(self.host, node_id)
            msg = {
                "method" :  "HIGHER_TERM",
                "params" : {
                    "term" : self.kv.current_term,
                }
            }
            send_message(conn, msg)
            return

        if curr_term > self.kv.current_term:
            self.kv.current_term = curr_term
            self.become_follower()

        #TODO: Handle log divergence
        if prev_index >= len(self.kv.log):
            return  
        
        if prev_index >= 0 and self.kv.log[prev_index]["term"] != prev_term:
            return
        

        insert_index = prev_index + 1

        i = 0
        while i < len(entry):
            if insert_index + i >= len(self.kv.log):
                break  

            if self.kv.log[insert_index + i]["term"] != entry[i]["term"]:
                self.kv.log = self.kv.log[:insert_index + i]
                break

            i += 1

        while i < len(entry):
            self.kv.log.append(entry[i])
            i += 1

        if entry:
            self.kv.commit_index = len(self.kv.log) - 1

        self.kv.apply_loop()

        conn = sender(self.host, node_id)
        msg = {
            "method" :  "COMMIT",
            "params" : {
                "node_id" : self.port
            }
        }
        send_message(conn, msg)
    
    def heartbeat(self):
        for i in self.peers:
            conn = sender(self.host, i)
            prev_index = self.kv.next_index[i] - 1
            prev_term = self.kv.log[prev_index]["term"]
            msg = {
                "method" :  "APPEND_ENTRIES",
                "params" : {
                    "node_id" : self.port,
                    "curr_term" : self.kv.current_term,
                    "prev_term" : prev_term,
                    "prev_index": prev_index,
                    "entry" : []
                }
            }
            send_message(conn, msg)
    
    def check_leader(self):
        if time.time() - self.curr_time > self.timeout:
            self.become_candidate()

    def request_vote(self):
        for i in self.peers:
            conn = sender(self.host, i)
            prev_index = self.kv.next_index[i] - 1
            prev_term = self.kv.log[prev_index]["term"]
            msg = {
                "method" :  "RESPOND_VOTE",
                "params" : {
                    "node_id" : self.port,
                    "curr_term" : self.kv.current_term,
                    "prev_term" : prev_term,
                    "prev_index": prev_index
                }
            }
            send_message(conn, msg)
    
    def respond_vote(self, node_id, curr_term, prev_term, prev_index):
        vote_granted = False

        # 1. Reject stale term
        if curr_term < self.kv.current_term:
            vote_granted = False

        else:
            # If term is newer → update
            if curr_term > self.kv.current_term:
                self.kv.current_term = curr_term
                self.kv.voted_for = None
                self.become_follower()

            # 2. Check if already voted
            if self.kv.voted_for in (None, node_id):

                # 3. Check log up-to-date
                my_last_index = len(self.kv.log) - 1
                my_last_term = self.kv.log[my_last_index]["term"] if my_last_index >= 0 else -1

                up_to_date = (
                    prev_term > my_last_term or
                    (prev_term == my_last_term and prev_index >= my_last_index)
                )

                if up_to_date:
                    vote_granted = True
                    self.kv.voted_for = node_id
                    self.reset_time()

        # Send response
        conn = sender(self.host, node_id)
        msg = {
            "method": "GRANT_VOTE",
            "params": {
                "term": self.kv.current_term,
                "vote_granted": vote_granted
            }
        }
        send_message(conn, msg)

    def grant_vote(self, term, vote_granted):
        if term > self.kv.current_term:
            self.kv.current_term = term
            self.become_follower()
            return

        if self.role != NodeRole.CANDIDATE:
            return

        if vote_granted:
            self.vote_count += 1

            if self.vote_count > len(self.peers) // 2:
                self.become_leader()
    
    def commit(self, node_id):
        self.kv.increase_next_index(node_id)
        self.commit_counter += 1
        self.final_commit()
    
    def final_commit(self):
        if self.commit_counter >= len(self.peers)//2:
            self.kv.commit_index += 1
            self.kv.apply_loop() 
            self.commit_counter -= len(self.peers)
    
    def higher_term(self, term):
        if self.kv.current_term < term:
            self.become_follower()

    def become_follower(self):
        self.role = NodeRole.FOLLOWER
    
    def become_candidate(self):
        self.kv.current_term += 1
        self.role = NodeRole.CANDIDATE

    def become_leader(self):
        self.role = NodeRole.LEADER
    
    def reset_time(self):
        self.curr_time = time.time()

    def reset_commit_counter(self):
        self.commit_counter = 0


def server(host, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)

    kv = KVStore()
    role = NodeRole.FOLLOWER

    follower = Follower(kv)
    leader = Leader(kv, host, port)

    while True:
        conn, _ = server.accept()
        msg = receive_message(conn)

        if not msg:
            conn.close()
            continue

        
        # -------- RAFT RPC --------
        if msg.get("type") == "AppendEntries":
            res = follower.handle_append_entries(msg)
            send_message(conn, res)

        elif msg.get("type") == "RequestVote":
            # minimal vote logic
            if msg["term"] >= kv.current_term:
                kv.current_term = msg["term"]
                send_message(conn, {"vote_granted": True})
            else:
                send_message(conn, {"vote_granted": False})

        # -------- CLIENT --------
        else:
            if role != "leader":
                send_message(conn, {"error": "not leader"})
            else:
                res = leader.handle_client(msg)
                send_message(conn, res)

        conn.close()

def main():
    host = "127.0.0.1"
    port = int(input("Port: "))

    server(host, port)

if __name__ == "__main__":
    main()