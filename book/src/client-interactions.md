# Client Interactions

In Raft, any command is sent to the leader and later the command is processed by the state machine. That is a textbook explanation of Raft protocol.

However, this needs to send the request to the leader (lol implements forwarding so user request sent to any of the servers are forwarded to the leader), the command is replicated to the majority of servers, wait for application to state machine and finally acks. This takes much time.

Some command may need all these processings before ack but other may not. For example, some command may be allowed to ack before entry application as it is guaranteed to be applied in some time later.

lol allows clients to interact with the cluster in a variety of ways. Here is the list:

- **RequestApply**: The command is sent to the leader, appended to log, replicated to majority, applied and then ack.
- **RequestCommit**: The command is sent to the leader, appended to log, replicated to majority and then ack.