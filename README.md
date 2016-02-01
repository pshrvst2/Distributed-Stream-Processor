CS425 – MP4 Report
Group 31 members:
1. Xiaoming Chen (xchen135) 2. Piyush Shrivastava (pshrvst2)

Design
In this MP we have implemented a simple distributed real-time computation system called Crane which uses Gossip protocol for failure detection. Similar to the Apache Storm, Crane uses tuples, spouts, bolts, sink, and topologies.
For the simplicity, we combine the Nimbus, Zookeeper and spout into the master node which is in charge of reading and inputting streams, assigning role of each bolts, and carrying the major part of fault tolerance. Based on this design, we need to make an assumption that the master node would never fail. Among of all the other worker nodes, one will be selected by master node randomly as a sink which handle the aggregation at the end of the topology. Shuffle grouping is in used on our Crane system to distribute the tuples across the bolts.

Design of assigning task:
1. By design, the master node would automatically be selected as the Spout. The other worker node would start a TCP connection for listening the role assignment from the master node.
2. When user fires an “assign” command, master node will randomly pick one work node as a sink, and other worker node will become a regular blots. The master node send out the role messages through the TCP connection to indicate the sink node id, each node will compare its id with the message, marks itself as sink if it matches, marks itself as regular bolt otherwise.

Design of Crane topologies:
1. Overall, the topologies are trees in this system. The streams are read and inputted by the Spout, and the regular bolt will execute some simply logic like filter, and when the task is done, pass the result to sink to do aggregation or join. Final result could be a file in the sink node or return back to the master node.
2. Spout, when user fires “start” command, master node will read up the streams from the dataset and multiple new threads will distribute the streams line by line to all the regular bolts in round-robin style.
3. Bolts, when streams received from the spout, run the task and pass the result to sink node. The sink node will do aggregation or join, when all tasks done, result could present as a file in the sink node, or return as message on the master node’s console.

Design of fault tolerance:
1. The failure detection extends from MP2, when the master node detects failures, it
send out a “forceDropAll” message to all the worker nodes to ask them drop all the current
tasks, clean up their role and wait for the new role assignment.
2. The master node randomly selects a new work node as a sink and pass the id through
TCP connection to all the alive worker nodes. Work nodes update their role according to the
message and wait for the streams.
3. Master start to read up and pass down the streams to the regular bolts, and bolts will
pass the result to the sink node till to all the tasks are accomplished.
4. By following this design, Master node doesn’t need to keep tracking of stream data
and the tasks, when encounters a failure, just ask the worker nodes to drop all the works,
reassign the roles and do the job all over again. The system will run very fast when no failure
happen, but in trade-off, process time will be double or even triple when encounter more than
one failures.

