package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// ApplyMsg is used for ....
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

// Raft is an object
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm       int // in persistent storage
	VotedFor          int // in persistent storage
	lastHeartBeatTime time.Time
	state             int // -1 -> dead, 0 -> follower, 1 -> candidate, 2-> leader
	//New for Assignment 3
	commitIndex  int // Last value committed at majority
	lastApplied  int // Last value passed to State Machine
	lastLogIndex int
	lastLogTerm  int
	followers    []follower //information for followers if I am leader
	Log          []LogEntry // in persistent storage

	applyCh  chan ApplyMsg //pass committed values on this channel
	commitCh chan ApplyMsg //pass values to be committed on this channel
	majority int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.Log)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&rf.CurrentTerm)
	decoder.Decode(&rf.VotedFor)
	decoder.Decode(&rf.Log)
	rf.lastLogIndex = len(rf.Log) - 1             // update this according to log
	rf.lastLogTerm = rf.Log[rf.lastLogIndex].Term // update this according to log
}

//
// RequestVoteArgs is RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply is RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// critical section. Accessing shared variables
	rf.mu.Lock()
	// new term detected. Convert to follower, update term and vote for candidate requesting vote if his log is up-to-date
	if args.Term > rf.CurrentTerm {
		rf.state = 0
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		if (args.LastLogTerm > rf.lastLogTerm) || (args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex) {
			// Grant vote if candidate's log is up-to-date
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true

		} else { // reject vote if candidate's log is not up-to-date
			rf.VotedFor = -1
			reply.VoteGranted = false

		}
		// same term and candidate's log is up-to-date
	} else if args.Term == rf.CurrentTerm && ((args.LastLogTerm > rf.lastLogTerm) || (args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex)) {
		if rf.VotedFor == -1 { // if not voted, vote for candidate requesting vote
			reply.Term = rf.CurrentTerm
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true
		} else { // aleady voted so decline vote request
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
		}
	} else { // got a stale vote request. Ignore
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
	rf.persist() // persist before responding to RPC
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start is used to pass a command to the Raft server/instance
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// accessing shared variables
	rf.mu.Lock()
	if rf.state == 2 {
		// If leader, append command to the log and return its index
		index = rf.lastLogIndex + 1
		term = rf.CurrentTerm
		isLeader = true

		rf.Log = append(rf.Log, LogEntry{rf.CurrentTerm, command})
		rf.lastLogTerm = rf.CurrentTerm
		rf.lastLogIndex++
		rf.persist() // log is updated. So, persist
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// Kill to return long running go routines
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// change state to dead -> -1
	rf.mu.Lock()
	rf.state = -1
	rf.mu.Unlock()
}

// Make initializes a Raft server
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = 0
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.followers = make([]follower, 0)
	// In the beginning, nextIndex of each follower will be 1 and matchIndex will be 0
	for i := 0; i < len(rf.peers); i++ {
		rf.followers = append(rf.followers, follower{1, 0})
	}

	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{-1, -1}) // log should be index from 1. So, filling index 0 with dummy value
	rf.lastLogIndex = 0
	rf.lastLogTerm = -1
	rf.applyCh = applyCh
	rf.commitCh = make(chan ApplyMsg, 100)
	rf.majority = len(rf.peers)/2 + 1 // size of majority to win election

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.commitEntries()
	go rf.main()
	return rf
}

func (rf *Raft) main() {

	rf.mu.Lock()
	rf.lastHeartBeatTime = time.Now()
	rf.mu.Unlock()

	var state int

	for {
		rand.Seed(time.Now().UnixNano())
		// critical section
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()

		// electionTimeout := rand.Intn(301) + 300 // [300,600] interval for selecting timeout

		// follower
		if state == 0 { // follower
			rf.followerState()
		} else if state == 1 { // candidate
			rf.candidateState()
		} else if state == 2 { // leader
			rf.leaderState()
		} else if state == -1 { // Raft server is killed
			return
		}
	}
}

func (rf *Raft) followerState() {
	electionTimeout := rand.Intn(301) + 300 // [300,600] interval for selecting timeout
	for {
		rf.mu.Lock()
		// timeout detected. Change state to candidate, increment term, vote for self and reset timer
		if int(time.Since(rf.lastHeartBeatTime).Milliseconds()) > electionTimeout {
			rf.state = 1
			rf.CurrentTerm++
			rf.VotedFor = rf.me
			rf.lastHeartBeatTime = time.Now()
			rf.mu.Unlock()
			break
		} else { // timeout not detected. Continuously recheck for timeout
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) candidateState() {
	electionTimeout := rand.Intn(301) + 300 // [300,600] interval for selecting timeout

	rf.mu.Lock()
	if rf.state == 1 { // recheck if still a candidate
		term := rf.CurrentTerm
		lastLogIndex := rf.lastLogIndex
		lastLogTerm := rf.lastLogTerm
		rf.mu.Unlock()

		// conduct election in a go routine so that it is not blocking
		ch := make(chan bool)
		go rf.conductElection(ch, term, rf.majority, lastLogIndex, lastLogTerm)

		for {
			select {
			case won := <-ch: // result of election
				if won {
					rf.mu.Lock()
					if rf.CurrentTerm == term && rf.state == 1 { // check that election win is not stale
						rf.state = 2
					}
					rf.mu.Unlock()
					// Return. State is changed. Leader now
					return
				}
				rf.mu.Lock()
				if rf.state == 0 { // check if follower, then return
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			default:
				time.Sleep(10 * time.Millisecond)
				rf.mu.Lock()
				// election time out. Icrement term, become candidate , vote for self and reset timer
				if int(time.Since(rf.lastHeartBeatTime).Milliseconds()) > electionTimeout {
					rf.state = 1
					rf.CurrentTerm++
					rf.VotedFor = rf.me
					rf.lastHeartBeatTime = time.Now()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}
	}
	// no longer a candidate. So, return
	rf.mu.Unlock()
}

func (rf *Raft) leaderState() {
	rf.mu.Lock()
	if rf.state == 2 {
		rf.lastLogIndex = len(rf.Log) - 1
		rf.lastLogTerm = rf.Log[rf.lastLogIndex].Term

		for i := range rf.followers {
			if i != rf.me {
				rf.followers[i].matchIndex = 0
				rf.followers[i].nextIndex = rf.lastLogIndex + 1
				go rf.followerHandler(i)
			}

		}
		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			if rf.state == 2 {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			} else {
				rf.mu.Unlock()
				return
			}
		}
	}
	rf.mu.Unlock()
}

// Deals with each Follower, replicates leader's log and send him periodic heartbeats
func (rf *Raft) followerHandler(server int) {

	for {
		rf.mu.Lock()
		if rf.state != 2 { // if no loner leader, return
			rf.mu.Unlock()
			return
		}

		// Follower log is behind. Bring it up to date
		if rf.followers[server].matchIndex < rf.lastLogIndex {
			prevLogIndex := rf.followers[server].nextIndex - 1
			prevLogTerm := rf.Log[prevLogIndex].Term

			var entries []LogEntry
			//Have no new log entry to send. However, don't know about matchIndex of follower
			if rf.followers[server].nextIndex > rf.lastLogIndex {
				entries = nil
			} else { // Send all missing entries
				entries = rf.Log[rf.followers[server].nextIndex : rf.lastLogIndex+1]
			}
			go rf.sendHeartBeat(server, rf.CurrentTerm, rf.me, entries, prevLogIndex, prevLogTerm, rf.commitIndex)
			rf.mu.Unlock()
		} else { // FOllower log is up-to-date. Just send empty, periodic heartbeat to follower
			prevLogIndex := rf.followers[server].nextIndex - 1
			prevLogTerm := rf.Log[prevLogIndex].Term
			go rf.sendHeartBeat(server, rf.CurrentTerm, rf.me, nil, prevLogIndex, prevLogTerm, rf.commitIndex)
			rf.mu.Unlock()
		}
		// Wait before sending heartbeat again
		time.Sleep(20 * time.Millisecond)
	}
}

// Sends a single heartbeat to a follower
func (rf *Raft) sendHeartBeat(server int, term int, leaderID int, entries []LogEntry, prevLogIndex int, prevLogTerm int, leaderCommit int) {
	args := AppendEntriesArgs{term, leaderID, entries, prevLogIndex, prevLogTerm, leaderCommit}
	reply := AppendEntriesReply{}

	if rf.sendAppendEntries(server, args, &reply) {
		rf.mu.Lock()
		// If a higher term detected, update term, change to follower, vote for no one and reset timer.
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.state = 0
			rf.VotedFor = -1
		} else if rf.CurrentTerm == term && !reply.Success { // Term is same but details of prev entry does not match with follower
			// decrement follower's nextIndex but it cannot be lower than 1
			if rf.followers[server].nextIndex > 1 {
				rf.followers[server].nextIndex--
			}
		} else if rf.CurrentTerm == term && reply.Success { // Term is same and prev entry matches with follower
			if entries == nil { // empty hearbeat
				rf.followers[server].matchIndex = prevLogIndex
				rf.followers[server].nextIndex = prevLogIndex + 1
			} else { // new entry
				rf.followers[server].matchIndex = prevLogIndex + len(entries)
				rf.followers[server].nextIndex = prevLogIndex + len(entries) + 1
			}
			rf.commitReplicatedEntries() //matchIndex updated. Might be able to commit some previously uncommited log entries.
		}
		rf.mu.Unlock()
	}
}

// Must be called while holding the lock. Checks if any uncommitted log entry is replicated on
// majority of servers and commits it
func (rf *Raft) commitReplicatedEntries() {
	// Only commit entries if leader
	if rf.state == 2 {
		newCommitIndex := 0
		// Iterate over all uncommitted entries in the log from highest index to lowest index to find highest N such that
		// N > commitIndex, log[N] is replicated on majority of servers, and log[N].term == currentTerm
		// This N will be our newCommitIndex
		for i := rf.lastLogIndex; i > rf.commitIndex; i-- {
			if rf.Log[i].Term == rf.CurrentTerm && rf.isReplicated(i) {
				newCommitIndex = i
				break
			}
		}
		// Update commitIndex according to new commitIndex and commit values
		for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
			rf.commitIndex = i
			rf.lastApplied = i
			// sent to other go routine to avoid blocking
			rf.commitCh <- ApplyMsg{i, rf.Log[i].Cmd, false, make([]byte, 0)}
		}
	}
}

// Must be called while holding the lock.
// Checks if the log entry at the provided index is replicated on majority of servers or not.
func (rf *Raft) isReplicated(index int) bool {
	count := 1 // entry is present in leader's log
	// Iterate over all servers
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			if rf.followers[server].matchIndex >= index { // Follower's log contains entry
				count++
				if count == rf.majority { // Majority of servers contain the entry
					return true
				}
			}
		}
	}
	// If majority not gained, return false by default
	return false
}

// Conduct Election
func (rf *Raft) conductElection(ch chan bool, term int, me int, lastLogIndex int, lastLogTerm int) {
	repliesReceived := make(chan RequestVoteReply, len(rf.peers))
	for i := range rf.peers { // request votes
		if i == rf.me { // do not request vote from self
			continue
		}

		go func(server int, ch chan RequestVoteReply) { // sendRequestVote and transmit back reply using channel
			args := RequestVoteArgs{term, me, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, args, &reply) {
				ch <- reply
			}
		}(i, repliesReceived)
	}

	votes := 1 // initial vote 1 as it votes for itself

	for i := 0; i < len(rf.peers); i++ {
		reply := <-repliesReceived
		rf.mu.Lock()
		// If a higher term detected in replies, update term, change to follower, vote for no one and reset timer. Transmit failure of election
		if rf.CurrentTerm < reply.Term {
			rf.CurrentTerm = reply.Term
			rf.state = 0
			rf.VotedFor = -1
			rf.lastHeartBeatTime = time.Now()
			rf.mu.Unlock()
			ch <- false
			return
		}
		rf.mu.Unlock()

		if reply.VoteGranted == true {
			votes++
			if votes >= rf.majority { // majority vote obtained. Transmit success of election
				ch <- true
				return
			}
		}

	}
	// lost if majority not obtained
	ch <- false
}

// AppendEntriesArgs is struct to pass to AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	Entries      []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

// AppendEntriesReply is struct to return reply from AppendEntries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries used for heartbeats and appending new log entries
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// critical section
	rf.mu.Lock()
	if rf.CurrentTerm > args.Term { // stale message, so ignore
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else if rf.CurrentTerm == args.Term { // same term. Reset timer
		reply.Term = rf.CurrentTerm
		rf.lastHeartBeatTime = time.Now()
		rf.processHeartbeatEntries(args, reply)
	} else { // Higher term detected. Update term, change to follower, vote for no one and reset timer.
		rf.state = 0
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		reply.Term = rf.CurrentTerm
		rf.lastHeartBeatTime = time.Now()
		rf.processHeartbeatEntries(args, reply)
	}
	rf.persist() // persist before responding to RPC
	rf.mu.Unlock()

}

// Must be called while holding the lock. Processes heartbeat and updates attributes accordingly. Sets reply.Success too
func (rf *Raft) processHeartbeatEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	// Prev Entry of Leader is the same my last entry.
	if args.PrevLogIndex == rf.lastLogIndex && args.PrevLogTerm == rf.lastLogTerm {
		reply.Success = true
		if args.Entries != nil { // Append entries in AppendEntries struct to log
			rf.Log = append(rf.Log, args.Entries...)
			rf.lastLogIndex = len(rf.Log) - 1
			rf.lastLogTerm = rf.Log[rf.lastLogIndex].Term
		}
		rf.followerCommitEntries(args)
	} else { // Prev of Leader not aligned with my last entry
		reply.Success = false
		if args.PrevLogIndex == 0 { //log restored to original state
			rf.Log = rf.Log[:1]
		} else {
			if args.PrevLogIndex < len(rf.Log) { //truncate all the excessive wrong entries
				rf.Log = rf.Log[:args.PrevLogIndex]
			}
		}
		rf.lastLogIndex = len(rf.Log) - 1
		rf.lastLogTerm = rf.Log[rf.lastLogIndex].Term
	}
}

// Must be called while holding lock. Commits entries using leaderCommit field of AppendEntries struct
func (rf *Raft) followerCommitEntries(args AppendEntriesArgs) {

	// Leader's commitIndex is greater than mine
	if args.LeaderCommit > rf.commitIndex {
		// Commit all entries in the log
		if args.LeaderCommit >= rf.lastLogIndex {
			for i := rf.commitIndex + 1; i <= rf.lastLogIndex; i++ {
				rf.commitCh <- ApplyMsg{i, rf.Log[i].Cmd, false, make([]byte, 0)}
			}
			rf.commitIndex = rf.lastLogIndex
			rf.lastApplied = rf.lastLogIndex
		} else { // Commit all entries upto leader's commitIndex
			for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
				rf.commitCh <- ApplyMsg{i, rf.Log[i].Cmd, false, make([]byte, 0)}
			}
			rf.commitIndex = args.LeaderCommit
			rf.lastApplied = args.LeaderCommit
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Commits entries sent on rf.commitCh
func (rf *Raft) commitEntries() {
	for {
		select {
		case msg := <-rf.commitCh:
			rf.applyCh <- msg
		}
	}
}

// LogEntry is a struct describing a single entry in a Raft server's log
type LogEntry struct {
	Term int
	Cmd  interface{}
}

type follower struct {
	nextIndex  int
	matchIndex int
}
