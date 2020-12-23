package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64 // unique random id for each Clerk
	seqNum int   // sequence number is used to ensure only once semantics
	leader int   // maintain index of leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand() // Assign random ID to each client
	ck.seqNum = 0
	ck.leader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{key, ck.id, ck.seqNum}
	var reply GetReply
	for {
		reply = GetReply{}
		// Call Get RPC on kv server
		ok := ck.servers[ck.leader].Call("RaftKV.Get", &args, &reply)
		if ok && reply.WrongLeader == false { // if RPC successful and kv server was indeed leader
			break
		} else { // otherwise, try with different kv server
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
	ck.seqNum++ // increment seq number for next request
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.id, ck.seqNum}
	for {
		reply := PutAppendReply{}
		// Call RPC PutAppend on kv server
		ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false { // if RPC successful and kv server was indeed leader
			break
		} else { // otherwise, try with different kv server
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
	}
	ck.seqNum++ // increment seq number for next request
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
