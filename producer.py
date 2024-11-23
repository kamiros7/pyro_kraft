#!/usr/bin/env python3

# To run the producer, put in terminal:
# python3 ./producer.py N
# N is a number that is like id or number of the log

import Pyro5
from Pyro5.api import Daemon, Proxy
import Pyro5.server
import sys

def main():
     id = sys.argv[1]
     service_names = Pyro5.api.locate_ns()
     uri_leader = service_names.lookup("Leader_epoch1")

     leader_obj = Pyro5.api.Proxy(uri_leader)
     log = f"log_{id}"
     leader_obj.registerNewLog(log)
     
if __name__ == "__main__":
     main()