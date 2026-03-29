import json
import struct
import socket
import time

def sender(host, port):
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(1)
        conn.connect((host, port))   # BLOCKING
        return conn
    except Exception:
        return None

def listener(host, port):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((host, port))
        server.listen(5)
        server.setblocking(False)
        return server
    except BlockingIOError:
        pass

def connect(server):
    conn, addr = server.accept()
    print("Address:", addr)
    return conn

def recv_exact(conn, n):
    data = b''
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data

def send_message(conn, data):
    if conn is None:
        return
    try:
        payload = json.dumps(data).encode()
        header = struct.pack('!I', len(payload))
        conn.sendall(header + payload)
    finally:
        conn.close()



def receive_message(conn:socket.socket):
    conn.settimeout(2)
    header = recv_exact(conn, 4)
    if header is None:
        return None

    if len(header) < 4:
        print("Invalid Header")
        return None

    length  = struct.unpack('!I', header)[0]
    data = recv_exact(conn, int(length))
    return json.loads(data.decode())

def receiver_loop(server, msg_queue):
    while True:
        try:
            conn, _ = server.accept()
        except BlockingIOError:
            time.sleep(0.01)
            continue

        try:
            msg = receive_message(conn)
            if msg:
                msg_queue.put(msg)
        finally:
            conn.close()