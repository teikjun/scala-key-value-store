# Key-Value Store

A simple distributed fault-tolerant replicated key-value store

Original challenge from [Programming Reactive Systems course](https://www.edx.org/course/scala-akka-reactive)

## Current Design

### Reading and Writing to the Key-Value Store

The client interface has 3 operations: Insert, Remove and Get. Modification operations (Insert and Remove) are handled using a **single-master** protocol, while read operations are handled using a **distributed protocol**. 

The primary replica contains the master copies for all objects. For update operations, the master copy is updated first, then the updates are propagated to other copies. For read operations, we can read from any one replica in the cluster.

The figure below summarizes the high-level interactions between the client, replicas, and Replicator.

![image](https://user-images.githubusercontent.com/46853051/149536034-015b6db3-b7c6-4ecb-bfb1-0e5f90682472.png)

*Fig 1: High-level interactions within the key-value store* 

### Replication Protocol

Internally, the consistency of the key-value store is maintained using the replication protocol. Each secondary replica has an associated Replicator. The Replicator acts as a middleman that accepts update events and propagates them to the corresponding secondary replica. 

There may be multiple conflicting updates on the same key at a replica. When this happens, the replica reconciles the conflicts using the **last-writer-wins heuristic**, by applying updates in order that they arrive.

### Order of Updates

In order to enforce a total ordering between updates, we keep track of the sequence number that is expected at any point. In the secondary replica, updates are processed in ascending order of sequence number, using a **stop-and-wait protocol**. The replica only processes updates with the current expected sequence number. The next expected sequence number incremented by one based on the previous expected sequence number.

The replica ignores any operation with sequence number greater than the expected sequence number. For sequence numbers smaller than the expected sequence number, the replica also does not perform any operation, but it sends an acknowledgement message in case the previous acknowledgement message was missed due to lossy communication.

### Eager Protocol

Our replication protocol is an eager protocol. No batching is done at the Replicator. The Replicator simply forwards the operation and sends a Snapshot message to the appropriate secondary replica.

### Timeouts

In order to ensure the liveness of our cluster, we impose time limits on the operations. Each operation is given a time limit of 1 second. A cancellable background task is scheduled to send OperationFailure to the client. Also, the replica stores the task’s cancellator in a buffer. If the success of the operation is not confirmed within 1 second, the scheduled task will proceed and send OperationFailure to the client. Otherwise, if an acknowledgement message is received for the operation within the 1 second time limit, the replica cancels the scheduled task and send OperationAck instead.

### Consistency Guarantees

The key-value store has multiple levels of consistency guarantees depending on how the client reads from the cluster. 

If the client always reads from the primary replica, the level of consistency is **Strong Consistency**, because the primary replica always contains the latest copy of the values. If the client always reads from the same replica, the level of consistency is **Monotonic Reads**, where each read in the client’s session should have a timestamp greater or equal to previous read. 

If the client reads from any replica (including the secondary replicas), the level of consistency is **Eventual Consistency**. For the above consistency guarantees, we assumed that no write operations failed. If any write operation fails, the cluster may be in an inconsistent state. In this scenario, there is no guarantee of consistency for the client.

### Joining and Leaving the Cluster

When a new replica joins the cluster, it sends a Join message to the Mediator. The Mediator assigns the replica with either a Primary or Secondary role. A new replica can join the cluster at any time. When a new secondary replica joins the cluster, the primary replica adds the replica to the list of secondary replicas and assigns a Replicator to it. This allows any new changes to be replicated to the secondary replica.

![image](https://user-images.githubusercontent.com/46853051/149536581-fa0f9c30-8dfc-4dbb-ac22-c46ebdb35ae2.png)

*Fig 2: A new replica joins the cluster*

Conversely, a replica can leave the cluster at any time. When a replica leaves the cluster, any OperationAck that is expected from the replica will be waived. This means that a replica can indirectly trigger an OperationAck by leaving the cluster. Hence, when a replica is removed from the cluster, the primary replica checks all relevant ids and sends OperationAck if appropriate.

### Persistence

A Persist message is sent by replica to the Persistence module to request the given state to be persisted. From the other side, a Persisted message is sent by Persistence to the replica to acknowledge that state has been persisted.

The Persistence module is flaky and has two points of failure. Firstly, the Persistence module could terminate, which results in unacknowledged messages. To deal with this, our replica supervises the Persistence module and **restarts** the Persistence module whenever it terminates. Secondly, the communication medium between the Persistence module and Replica can be lossy. To deal with this problem, we periodically resend the Persist messages for any unacknowledged operations.

![image](https://user-images.githubusercontent.com/46853051/149537310-fef34cf9-b1c1-4b67-b6aa-302c9c91d006.png)

*Fig 3: Replica sends a Persist message to the Persistence module, expecting an acknowledgement*

### Handling Lossy Communication

Lossy communication is a possible point of failure for reading and writing operations. Any message could be lost during the transfer from sender to receiver. To resolve this issue, messages are sent periodically until an acknowledgement is received. In order to send messages periodically, we **schedule a background tas** that sends a message at a set time interval. When a message is acknowledged, the **scheduled task is cancelled** so that the same message will not continue to be sent. 

There are multiple tricky issues that can arise due to lossy communication. All kinds of messages including acknowledgement messages can be lost. Our solution to this problem is to resend all unacknowledged messages at a set interval. However, this could lead to the opposite problem, where the receiver receives multiple messages that are intended to be one message. This occurs when the delay in message sending is greater than the set interval. To handle this case, the unique id given by the client is used to uniquely identify the messages.

### Existing Limitations 

The current implementation of the key-value store has multiple limitations. To wrap up this discussion, let us explore some ideas for lifting the key limitations of the current key-value store.

Firstly, the current implementation only accepts updates via the primary replica. So, the primary replica is a bottleneck for the system’s throughput. Instead of having a single master replica which holds all the primary copies, we can distribute the primary copies to other replicas via **sharding**. We can designate a key set to each replica via **hash partition** or **range partition**. Each replica will be responsible for updating their designated primary copies. Alternatively, we could have a fully distributed protocol, where updates are accepted at any replica, and inconsistent updates are eventually reconciled based on timestamp using the last-writer-wins heuristic. The choice between these two implementations should be based on the desired level of consistency.

Secondly, the current implementation assumes that the primary replica does not fail. If the primary replica fails, the entire cluster will not be able to accept update operations and will also lose track of current replicas and other bookkeeping information. To resolve this limitation, we can use an **election** process similar to PAXOS or RAFT. Essentially, we want to have a protocol to elect another replica as the leader when the current leader fails. This allows our cluster to be more fault tolerant.

Thirdly, the current implementation assumes that the update rate is low. If the update rate is high, having a single replica handle all the updates would result in very slow updates. To work around this problem, we can allow writes to occur at any replica. To maintain strong consistency, one method is to write to all replicas and read from any one of them to get the latest value. However, this is equally inefficient. As suggested in the instruction document, we can use the idea of **quorums** to increase the efficiency of the cluster. We start by assigning a write quorum W and read quorum R, such that R + W > N, where N is the total number of nodes. Each time we write to the key-value store, we need to write to W replicas. Each time we read from the key-value store, we must read from R replicas and take the value which has the highest timestamp. The inequality ensures that the read and write quorums overlap. By reading the value with the latest timestamp, we are guaranteed to obtain the latest value. By using quorums, our cluster becomes more efficient and more available, as one replica failure will not prevent our cluster from functioning.

Finally, the current implementation ignores inconsistencies after OperationFailure. As a result, clients can read inconsistent results from this key-value store. As mentioned in the instruction document, lifting this restriction depends on the level of consistency required by the client. It is possible to allow the client to choose the level of consistency required. Each level of consistency has a minimum acceptable timestamp for read operations. For example, the minimum acceptable timestamp is 0 for Eventual Consistency, max(all timestamps) for Strong Consistency, (currentTimestamp – t) for Bounded Staleness Consistency with a maximum staleness of t. If we keep track of the timestamp after each update, we will be able to determine an appropriate result to return for a client that is reading at a particular level of consistency. A good reference for this is the Pileus system , a system designed to allow clients to choose their desired level of consistency. 



