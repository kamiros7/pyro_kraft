#!/usr/bin/env python3

# The order to run the scripts
# 1 - Service names
# 2 - Leader
# 3 - Broker clients
# 4 - Producer
# 5 - Consumer

# Before run the leader, open other terminal to run the service name, with:
# python3 -m Pyro5.nameserver

# To run the leader, put in terminal:
# python3 ./leader.py

## Precisamos verificar quais funções serão oneway, quais precisaram de timeout, para não travar tudo

import Pyro5
from Pyro5.api import Daemon, Proxy
import Pyro5.server

class Leader(object):
    def __init__(self):
            self.id = 1
            self.log = []
            self.broker_clients = {}
    
    @Pyro5.server.expose
    def getLog(self, broker_client_id, startIndex, desireIndex):
        print(f"Hello from the server! Broker client with id {broker_client_id} called getLog sent: {startIndex} {desireIndex}")

        if broker_client_id not in self.broker_clients:
            print(f"broker client {broker_client_id} isn't registered in leader")
            return ("ERROR", -1, -1)
        
        broker_id, (callback_uri, broker_state, broker_start_index) = self.broker_clients[broker_client_id]

        if (desireIndex > len(self.log)):
            print("desireIndex is outside than leader log")
            return ("ERROR", broker_start_index, len(self.log))

        if (broker_start_index != startIndex):
            print(f"broker client {broker_client_id} is requesting data from a wrong start index")
            return ("ERROR", broker_start_index, desireIndex)

        self.broker_clients[broker_client_id] = ((callback_uri, broker_state, startIndex+desireIndex))
        return ("OK", self.log[startIndex:desireIndex])
    
    @Pyro5.server.expose
    def registerNewLog(self, newData):
        #Receives the new data from the producer
        self.log.append(newData)
        self.notify_all_brokers()
        return f"Hello from the server! You called setNewLog and sent: {newData}"
    
    @Pyro5.server.expose
    def registerBroker(self, broker_id, broker_uri, broker_state):
        self.broker_clients[broker_id] = (broker_uri, broker_state, 0) #The last number is the current epoch of the broker client to get the logs. Always starts with 0
        print(f"Registered broker: {broker_id} -> URI: {broker_uri}, Description: {broker_state}")

    def notify_all_brokers(self):
        for broker_id, (callback_uri, broker_state, _) in self.broker_clients.items():
            # We going to notify just the broker clients that are voters (observer doesnt receive the data or request data)
            if broker_state == 'observer':
                continue

            try:
                callback = Pyro5.api.Proxy(callback_uri)
                callback.updateLog()
            except Exception as e:
                print(f"Failed to notify broker {broker_id}: {e}")
        return

def main():     
    # Server-side code
    daemon = Daemon()
    uri = daemon.register(Leader(), 'Leader_epoch1')
    print("Server started at:", uri)

    name_server = Pyro5.api.locate_ns()
    name_server.register('Leader_epoch1', uri)

    daemon.requestLoop()

if __name__ == "__main__":
     main()