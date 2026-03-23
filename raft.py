import socket
import struct
import json
from utils import receive_message, send_message, sender, listener, connect
from enum import Enum, auto
import multiprocessing

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

    def append_log(self, entry):
        self.log.append(entry)

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


class Leader:
    def __init__(self, kv_store, host, port):
        self.host = host
        self.port = port
        self.peers = [p for p in ALL_NODES if p != port]
        self.kv_store = kv_store

    def handle_client(self, message):
        entry = {
            "term": self.kv_store.current_term,
            "command": message
        }
        self.kv_store.append_log(entry)

        success = self.replicate_log()

        if success:
            self.kv_store.commit_index = len(self.kv_store.log) - 1
            self.kv_store.apply_loop()
            return {"status": True}
        return {"status": False}

    def replicate_log(self):
        last_index = len(self.kv_store.log) - 1
        entry = self.kv_store.log[last_index]

        success_count = 1

        for p in self.peers:
            conn = sender(self.host, p)

            prev_index = last_index - 1
            prev_term = (
                self.kv_store.log[prev_index]["term"]
                if prev_index >= 0 else -1
            )

            msg = {
                "type": "AppendEntries",
                "term": self.kv_store.current_term,
                "leader_id": self.port,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": [entry],
                "leader_commit": self.kv_store.commit_index
            }

            send_message(conn, msg)
            res = receive_message(conn)

            if not res:
                continue

            if res.get("term", 0) > self.kv_store.current_term:
                self.kv_store.current_term = res["term"]
                return False

            if res.get("success"):
                success_count += 1

        return success_count > (len(self.peers) + 1) // 2

class Follower:
    def __init__(self, kv_store):
        self.kv_store = kv_store

    def handle_append_entries(self, msg):
        term = msg["term"]

        if term < self.kv_store.current_term:
            return {"success": False, "term": self.kv_store.current_term}

        self.kv_store.current_term = term

        prev_index = msg["prev_log_index"]
        prev_term = msg["prev_log_term"]

        if prev_index >= 0:
            if len(self.kv_store.log) <= prev_index:
                return {"success": False, "term": self.kv_store.current_term}

            if self.kv_store.log[prev_index]["term"] != prev_term:
                return {"success": False, "term": self.kv_store.current_term}

        for entry in msg["entries"]:
            self.kv_store.append_log(entry)

        if msg["leader_commit"] > self.kv_store.commit_index:
            self.kv_store.commit_index = min(
                msg["leader_commit"],
                len(self.kv_store.log) - 1
            )

        self.kv_store.apply_loop()

        return {"success": True, "term": self.kv_store.current_term}

class Candidate:
    def __init__(self, kv_store, node_id):
        self.kv_store = kv_store
        self.node_id = node_id
        self.peers = [p for p in ALL_NODES if p != node_id]
        self.votes = 0

    def run(self):
        self.kv_store.current_term += 1
        self.votes = 1

        last_index = len(self.kv_store.log) - 1
        last_term = (
            self.kv_store.log[last_index]["term"]
            if last_index >= 0 else -1
        )

        for p in self.peers:
            conn = sender("127.0.0.1", p)

            msg = {
                "type": "RequestVote",
                "term": self.kv_store.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_index,
                "last_log_term": last_term
            }

            send_message(conn, msg)
            res = receive_message(conn)

            if res and res.get("vote_granted"):
                self.votes += 1

        if self.votes > (len(self.peers) + 1) // 2:
            return "leader"

        return "follower"


def server(host, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)

    kv = KVStore()
    role = "follower"

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