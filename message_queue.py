import queue
import threading

class MessageQueue:
    def __init__(self):
        self.inbound = queue.Queue()
        self.outbound = queue.Queue()

    # ---- INBOUND ----
    def push_inbound(self, msg):
        self.inbound.put(msg)

    def pop_inbound(self):
        try:
            return self.inbound.get_nowait()
        except queue.Empty:
            return None

    # ---- OUTBOUND ----
    def push_outbound(self, msg):
        self.outbound.put(msg)

    def pop_outbound(self):
        try:
            return self.outbound.get_nowait()
        except queue.Empty:
            return None