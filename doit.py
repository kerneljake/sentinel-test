#!/usr/bin/env python3

import redis
from redis.sentinel import Sentinel
import time
from datetime import datetime
import signal
import sys
import os
import getopt

MYPOD='mymaster'
sentinel_list = [("localhost", 26379)]

def signal_handler(signal, frame):
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

try:
	opts, args = getopt.getopt(sys.argv[1:], "p:s:")
except getopt.GetoptError:
	print("USAGE: " + sys.argv[0] + " [-p podname] [-s sentinels,comma:26379,separated]")
	sys.exit(2)
for opt, arg in opts:
	if opt in ("-p"):
		MYPOD = arg
	elif opt in ("-s"):
		sentinel_list = []
		for hostname_port in arg.split(","):
			try:
				sentinel, port = hostname_port.split(":")
			except ValueError:
				sentinel = hostname_port
				port = 26379
			sentinel_list.append((sentinel, int(port)))

print("sentinel list is " + str(sentinel_list))
sentinel = Sentinel(sentinel_list, socket_timeout=0.5)
master = sentinel.master_for(MYPOD, socket_timeout=0.1)
slave = sentinel.slave_for(MYPOD, socket_timeout=0.1)
slave.set_response_callback('GET', int)

while True:
	try:
		master_tuple = sentinel.discover_master(MYPOD)
		master_str = "%s:%d" % (master_tuple[0], master_tuple[1])
		slave_list = sentinel.discover_slaves(MYPOD)
		if len(slave_list) > 0:
			slave_str = "%s:%d" % (slave_list[0][0], slave_list[0][1])
		else:
			slave_str = master_str
		write = master.incr("foo", 1)
		read = slave.get("foo")
		print(datetime.now().strftime('%d %b %H:%M:%S.%f')[:-3], end="")
		print(" >>> wrote %d to master %s, read %d from slave %s" % (write, master_str, read, slave_str), flush=True)
	except SystemExit:
		sys.exit(0)
	except:
		e = sys.exc_info()[0]
		print(">>>>>>>>>>>>>>>>>> ERROR %s" % e, flush=True)

	time.sleep(1)
