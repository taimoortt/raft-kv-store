package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID     int64 // client ID
	SeqNum int   // Sequence number for the request
	Key    string
	Value  string
	Cmd    string // Put, Append or Get
}

// func (o *Op) info() (int64, int, string, string, string) {
// 	return o.ID, o.SeqNum, o.Key, o.Value, o.Cmd
// }

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keystore map[string]string // key value store using map
	clients  map[int64]Client  // map containing info about each client
	appLock  sync.Mutex        // Lock for use by kv server.
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{args.ID, args.SeqNum, args.Key, "", "Get"}
	_, _, isLeader := kv.rf.Start(op)

	if isLeader {
		// Accessing shared variable, so acquire lock
		kv.appLock.Lock()
		kv.checkClientID(args.ID)
		tempCh := kv.clients[args.ID].Ch
		kv.appLock.Unlock()

		var value string
		wait := true
		// If leadership lost before request is committed and processed, inform client
		for wait {
			select {
			case value = <-tempCh:
				wait = false
			case <-time.After(20 * time.Millisecond):
				_, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.WrongLeader = true
					reply.Err = ErrNoKey
					return
				}
			}
		}

		if value == "stale" { // Got an previous request again. Send the last Get response
			kv.appLock.Lock()
			value = kv.clients[args.ID].LastGet
			kv.appLock.Unlock()
		}

		reply.WrongLeader = false
		reply.Err = OK
		reply.Value = value
	} else {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		reply.Value = ""
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{args.ID, args.SeqNum, args.Key, args.Value, args.Op}
	_, _, isLeader := kv.rf.Start(op)

	if isLeader {
		// Accessing shared variables
		kv.appLock.Lock()
		kv.checkClientID(args.ID)
		tempCh := kv.clients[args.ID].Ch
		kv.appLock.Unlock()

		var value string
		wait := true
		// If leadership lost before request is committed and processed, inform client
		for wait {
			select {
			case value = <-tempCh:
				wait = false
			case <-time.After(20 * time.Millisecond):
				_, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.WrongLeader = true
					reply.Err = ErrNoKey
					return
				}
			}
		}

		if value == "stale" { // Got an previous request again. Do nothing
		}
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 10000) // buffer to ensure that our underlying Raft does not block
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.keystore = make(map[string]string)
	kv.clients = make(map[int64]Client)

	go kv.Main()

	return kv
}

// Main is the function which runs continuously and checks for committed operation and processes them accordingly.
func (kv *RaftKV) Main() {

	for {
		applyMsg := <-kv.applyCh
		_, leader := kv.rf.GetState()
		op := applyMsg.Command.(Op)

		// Accessing shared variables, so acquire lock
		kv.appLock.Lock()
		kv.checkClientID(op.ID)
		client := kv.clients[op.ID]
		kv.appLock.Unlock()

		if client.LastSeqNum+1 != op.SeqNum && leader { // If did not get consecutive requests, do nothing
			client.Ch <- "stale"
		} else {
			client.LastSeqNum = op.SeqNum

			if op.Cmd == "Get" { // GET Request
				value, ok := kv.keystore[op.Key]
				if !ok { // if value does not exists, take empty string as default value
					value = ""
				}
				if leader { // send on channel if leader
					client.Ch <- value
				}
				client.LastGet = value
			} else if op.Cmd == "Append" { // APPEND Request
				value, ok := kv.keystore[op.Key]
				if ok { // If value exists, append
					kv.keystore[op.Key] = value + op.Value
				} else { // If value does not exist, act as PUT
					kv.keystore[op.Key] = op.Value
				}
				if leader { // send on channel if leader
					client.Ch <- ""
				}
			} else if op.Cmd == "Put" { // PUT Request
				kv.keystore[op.Key] = op.Value
				if leader { // send on channel if leader
					client.Ch <- ""
				}
			}
			// Accessing shared variable, so acquire lock
			kv.appLock.Lock()
			kv.clients[op.ID] = client // update client info
			kv.appLock.Unlock()
		}
	}
}

func (kv *RaftKV) checkClientID(id int64) {
	_, ok := kv.clients[id]
	if !ok {
		kv.clients[id] = Client{id, -1, make(chan string), ""}
	}
}
