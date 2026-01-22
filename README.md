# Distributed Diagnostics & Telemetry System (Embedded SW Design Challenge)

This repository contains a simulated distributed diagnostics and anomaly detection framework designed for systems deployed in remote environments.  
The architecture models a **sensor node** communicating with a **host controller** over a reliable, ACK-based protocol.

The system is implemented in Python 

---

## System Overview

The system consists of two primary components:

### Sensor Node
- Periodically samples local sensors (temperature, humidity, vibration)
- Performs local anomaly checks based on configurable limits
- Write-ahead logs telemetry to simulated non-volatile memory (NVM)
- Reliably replays unacknowledged telemetry after outages
- Applies configuration updates pushed by the host
- Executes on-demand diagnostics (e.g., power rail checks)

### Host Controller
- Accepts TCP connections from nodes
- Receives and logs telemetry data
- Sends cumulative ACKs so nodes can clear buffered data
- Periodically issues configuration updates
- Periodically requests diagnostics and retries them across disconnects
- Simulates host restarts to test fault tolerance




>[!NOTE]
>I used ip "127.0.0.1" and port 9000 as the ip is the loopback on my machine and the port doesn't require privledges. If you need to change these for your machine they are constants in common/constants.py. I did this on my windows computer so if using linux I beleive it should all be the same except the actual python comand to execute the script and the ip and port. 


## Code Instructions 
1. **Clone this repository via the following terminal command and then change in to the directory**
    ```
    git clone https://github.com/Caleb4884/SWFW_DesignChallenge.git
    cd SWFW_DesignChallenge
    ```
2. **Run the Host first in one terminal using the following command**

    `python host.py`

>[!NOTE]
>The host should print a message to the terminal and create a log file with the time and date of execution

3. **Open a Second Terminal in the same directory**

4. **In the second Terminal run**

    `python node.py`

>[!NOTE]
>The node will create its own files as well. One holds the most recent acknowled message by the host and the other is used as the buffer. The buffer doesn't clear because it is a json file but the logic to hold the most recent ack is there so its possible in the non simulated system to clear memory on the node.

5. **To Terminate the test in the different terminals pres Ctrl+C and they should shut down**
>[!NOTE]
>Shutdown the host then the node otherwise it doesn't react quite right and I am not sure why exactly

6. **The data is now stored in a json file in the same directory. There is diagnostics messages in ther as well which in the future would probobly be better in a specific diagnostic file but this gives you one place to look** 


## Log Screenshots 

