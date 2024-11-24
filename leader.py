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
import datetime
import threading
import time

LIMIT_LIFE_TIME = 60  # Lifetime in seconds (e.g., 60 seconds)

class Leader(object):
    def __init__(self):
            self.id = 1
            self.log = [] #Each element is a pair, the first is the log, the second is a list that contains the broker clients that commited the respective log
            self.commited_log = []
            self.broker_clients = {}
            self.broker_clients_heartbeat = {}
            self.voters_number = 1
            self.lock = threading.Lock()  # Lock to ensure thread-safe access
    
    @Pyro5.server.expose
    def getLog(self, broker_client_id, startIndex):
        desireIndex = startIndex + 1

        print(f"Hello from the server! Broker client with id {broker_client_id} called getLog sent: {startIndex} {desireIndex}")
        if broker_client_id not in self.broker_clients:
            print(f"broker client {broker_client_id} isn't registered in leader")
            return ("ERROR", -1)
        
        callback_uri, broker_state, broker_start_index = self.broker_clients[broker_client_id]

        if (desireIndex > len(self.log)):
            print("desireIndex is outside than leader log")
            return ("ERROR", broker_start_index)

        if (broker_start_index != startIndex):
            print(f"broker client {broker_client_id} is requesting data from a wrong start index")
            return ("ERROR", broker_start_index)

        self.broker_clients[broker_client_id] = ((callback_uri, broker_state, desireIndex))
        log_list = [pair[0] for pair in self.log[startIndex:desireIndex]] #To just return the logs
        return ("OK", log_list)
    
    @Pyro5.server.expose
    @Pyro5.server.oneway
    def confirmLogStored(self, broker_id, startIndex):
        commited_brokers = self.log[startIndex][1]
        if broker_id not in commited_brokers:
            commited_brokers.append(broker_id)
        
        if len(commited_brokers) >= self.voters_number and self.log[startIndex][0] not in self.commited_log:
            self.commited_log.append(self.log[startIndex][0])
        
        #Here receive that the specific broker client stored the log
        #When all the voters stored the log, the log will be with status commited
        #The way to get just the filtered list is:
        #filtered_list = [pair[0] for pair in original_list if len(pair[1]) > 3]

    @Pyro5.server.expose
    def getCommitedLog(self, startIndex):
        if startIndex > len(self.commited_log):
            return ("ERROR", len(self.commited_log) - 1)

        if (len(self.commited_log[startIndex:]) == 0):
            return("WARN", "there aren't new logs")
        
        return ("OK", self.commited_log[startIndex:])

    @Pyro5.server.expose
    def registerNewLog(self, newData):
        #Receives the new data from the producer
        self.log.append((newData, []))
        self.notify_all_brokers()
        print(f"Hello from the server! You called setNewLog and sent: {newData}")
        return
    
    @Pyro5.server.expose
    def registerBroker(self, broker_id, broker_uri, broker_state):
        self.broker_clients[broker_id] = (broker_uri, broker_state, 0) #The last number is the current epoch of the broker client to get the logs. Always starts with 0
        print(f"Registered broker: {broker_id} -> URI: {broker_uri}, Description: {broker_state}")

    @Pyro5.server.expose
    @Pyro5.server.oneway
    def update_broker_timestamp(self, broker_client_id):
        current_timestamp = datetime.datetime.now()
        self.broker_clients_heartbeat[broker_client_id] = current_timestamp
        print(f"Updated broker {broker_client_id} with timestamp {current_timestamp}")

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
    
    def collect_expired_brokers(self):
        expired_brokers = []
        current_timestamp = datetime.datetime.now()
        with self.lock:  # Ensure thread-safe access
            for broker_client_id, timestamp in list(self.broker_clients_heartbeat.items()):
                diff = (current_timestamp - timestamp).total_seconds()
                if diff > LIMIT_LIFE_TIME:
                    expired_brokers.append(broker_client_id)
        return expired_brokers
    
    def monitor_broker_lifetimes(self):
        while True:
            expired_brokers = self.collect_expired_brokers()
            if expired_brokers:
                print(f"Expired brokers: {expired_brokers}")

                ##Here for each expired broker, call the updateLog to the correct broker
                
            else:
                print("No expired brokers.")
            time.sleep(5)

def main():     
    # Server-side code
    daemon = Daemon()
    uri = daemon.register(Leader(), 'Leader_epoch1')
    print("Server started at:", uri)

    name_server = Pyro5.api.locate_ns()
    name_server.register('Leader_epoch1', uri)

    leader = Leader()
    monitor_thread = threading.Thread(target=leader.monitor_broker_lifetimes)
    monitor_thread.daemon = True  # Ensure the thread stops when the main program exits
    monitor_thread.start()

    daemon.requestLoop()

if __name__ == "__main__":
     main()