#!/usr/bin/env python3

import redis
from redis.sentinel import Sentinel
import time
from datetime import datetime
import signal
import sys
import os

def signal_handler(signal, frame):
	sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

sentinel = Sentinel([('localhost', 26379), ('192.168.1.112', 26379)], socket_timeout=0.1)
master = sentinel.master_for('mypod', socket_timeout=0.1)
slave = sentinel.slave_for('mypod', socket_timeout=0.1)
#pid = os.getpid()
#master.set("foo", 0)
slave.set_response_callback('GET', int)

while True:
	master_tuple = sentinel.discover_master('mypod')
	master_str = "%s:%d" % (master_tuple[0], master_tuple[1])
	slave_list = sentinel.discover_slaves('mypod')
	slave_str = "%s:%d" % (slave_list[0][0], slave_list[0][1])
	write = master.incr("foo", 1)
	read = slave.get("foo")
	#print("%d:P " % pid, end="")
	print(datetime.now().strftime('%d %b %H:%M:%S.%f')[:-3], end="")
	print(" >>> wrote %d to master %s, read %d from slave %s" % (write, master_str, read, slave_str), flush=True)
	time.sleep(1)
