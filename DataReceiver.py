import zmq
import time
context = zmq.Context()
socket = context.socket(zmq.REP)
