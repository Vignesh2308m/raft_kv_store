import json
import struct
import socket

def sender(host, port):
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((host, port))
    return conn

def listener(host, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    return server

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

def send_message(conn:socket.socket, data:dict):

    data_byte = json.dumps(data).encode()

    data_len = len(data_byte)

    header = struct.pack('!I', data_len)

    message = header + data_byte

    conn.sendall(message)



def receive_message(conn:socket.socket):

    header = recv_exact(conn, 4)

    if header is None:
        return None

    if len(header) < 4:
        print("Invalid Header")
        return None

    length  = struct.unpack('!I', header)[0]

    data = recv_exact(conn, int(length))

    return json.loads(data.decode())