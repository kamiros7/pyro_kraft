#!/usr/bin/env python3

# To run the consuner, put in terminal:
# python3 ./consumer.py

import time
import threading
import Pyro5.api

def fetch_logs_periodically(shared_state):
    while True:
        try:
            start_index = shared_state["start_index"]
            logs = shared_state["logs"]

            service_names = Pyro5.api.locate_ns()
            uri_leader = service_names.lookup("Leader_epoch1")

            leader_obj = Pyro5.api.Proxy(uri_leader)
            response = leader_obj.getCommitedLog(start_index)
            status, data = response

            if status == "OK":
                print(f"New logs arrived: {data}")
                logs.extend(data)
                shared_state["start_index"] = len(logs)
            elif status == "ERROR":
                shared_state["start_index"] = data  # Adjust startIndex from server response
                print("Error to obtain the new log")
            else:
                print("There isn't new log")

            shared_state["logs"] = logs  # Persist logs
        except Exception as e:
            print(f"An error occurred: {e}")

        # Wait for 5 seconds before the next iteration
        time.sleep(5)

if __name__ == "__main__":
    # Initialize shared state
    shared_state = {
        "start_index": 0,
        "logs": []
    }

    log_thread = threading.Thread(target=fetch_logs_periodically, args=(shared_state,))
    log_thread.daemon = True  # Optional: Ensures the thread stops when the main program exits
    log_thread.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("Exiting program...")
        print(f"Final logs: {shared_state['logs']}")
