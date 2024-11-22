#!/usr/bin/env python3

# To run the leader, put in terminal:
# python3 ./broker_client.py N "type"
# N is the id of the broker client
# "type" must be a string, that represents the mode of the broker, that can be "obsever" or "voter"

# I will need to use more two threads. The main process will be blocked with requestLoop to receive the notification to update
# The first thread will be used to comunicate with leader (to request the data to update)
# The second thread will be used to send the heartbeat to server

import Pyro5
from Pyro5.api import Daemon, Proxy
import Pyro5.server
import sys
import threading

class BrokerClient(object):
    def __init__(self, broker_id, broker_state):
            self.broker_id = broker_id
            self.broker_state = broker_state
            self.log = (0,[])
            self.lock = threading.Lock()  # Lock to ensure thread-safe access
    
    #This function will be used just to receive the notification
    @Pyro5.api.expose
    @Pyro5.api.callback
    def updateLog(self):
        print(f"Hello from the client! called updateLog")
        thread = threading.Thread(target=self.fetch_data_from_leader)
        thread.start()
        return
    
    #Used to change the observer to voter
    def changeState(self, new_state):
        self.broker_state = new_state

    def fetch_data_from_leader(self):
        try:
            service_names = Pyro5.api.locate_ns()
            uri_leader = service_names.lookup("Leader_epoch1")
            leader = Pyro5.api.Proxy(uri_leader)

            start_index_to_request = len(self.log[1])
            response = leader.getLog(self.broker_id, start_index_to_request)
            print(f"Raw response: {response}")
            status, data = response
            
            if (status == "OK"):
                with self.lock:
                    print(f"Updated log: {self.log}")
                    log_entries = data
                    self.log[1].extend(log_entries)
                    self.log = (self.log[0] + 1, self.log[1])

                    leader.confirmLogStored(self.broker_id, start_index_to_request)
            elif status == "ERROR":
                # Update the client log to be the same as the leader's
                print(f"Error occurred to get the date. Start index: {broker_start_index}")
                broker_start_index = data
                with self.lock:
                    self.log = (broker_start_index, self.log[:broker_start_index])
            else:
                print(f"Unexpected status: {status}")
        except Exception as e:
            print(f"Error fetching data from leader: {e}")
    
def main():
    broker_client = BrokerClient(sys.argv[1], sys.argv[2])

    try:
        daemon = Daemon()
        uri_client = daemon.register(broker_client, f"Client_epoch{broker_client.broker_id}")
    except Exception as e:
        print(f"Error in register the broker client {uri_client} in leader: {e}")

    broker_client.broker_uri = uri_client

    service_names = Pyro5.api.locate_ns()
    uri_leader = service_names.lookup("Leader_epoch1")

    leader_obj = Pyro5.api.Proxy(uri_leader)
    leader_obj.registerBroker(broker_client.broker_id, broker_client.broker_uri, broker_client.broker_state)

    ## Aqui criar uma thread que de tempos em tempos, chama uma função que manda heartbeat para lider (manda apenas id)
    daemon.requestLoop()

if __name__ == "__main__":
     main()