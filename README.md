# raft-kv-store
Complete implementation of a Raft-based Fault Tolerant Key Value store, implemented as part of my assignments in CS-582 Distributed Systems.

The <b>Raft</b> implmentation can be found in the /raft/raft.go directory. In order to test, Raft implementation, /raft/test_test.go can be run which tests the implementation through network partitions and failures of leaders.

The <b>KV-Store</b> implementation can be found in the /kvraft directory. The file /server.go contains the Raft server interface while the file /client.go contains the Client interface. To test the implmentation of the kv-store, run the file /kvraft/test_test.go which tests concurrent reads and writes as well as unreliable servers by creating network partitions.

Test Command: <b>go test</b> in the relevant directory.
