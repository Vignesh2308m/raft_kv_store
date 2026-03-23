import socket
import json
import struct
from utils import send_message, receive_message
import threading
import random


def client(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
        conn.connect((host, port))
        try:
            method = input("Method:")
            key = input("Key:")
            value = input("Value:")

            # Send Request
            data = {}

            data["method"] = method
            data["params"] = {}
            data["params"]["key"] = key
            data["params"]["value"] = value

            send_message(conn, data)

            print("REQ SENT")

            #Receive Response

            message = receive_message(conn)
            if message is None:
                print("Connection closed by server")
            
            print("RES RECEIVED")

            print(f"Status: {message.get("status")}")
            print(f"Value: {message.get("value")}")
            print(f"Error: {message.get("error")}")

            
        except KeyboardInterrupt:
            print("Stopped...")

def main():
    host = "127.0.0.1"
    port = 5000

    client(host, port)

if __name__ == "__main__":
    main()