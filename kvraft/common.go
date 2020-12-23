package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID     int64 // Client ID
	SeqNum int   // Sequence number of request
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key    string
	ID     int64 // Client ID
	SeqNum int   // Sequence number of request
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type Client struct {
	ID         int64       // Client ID
	LastSeqNum int         // Sequence number of last request
	Ch         chan string // channel for each client
	LastGet    string      // store last Get response
}
