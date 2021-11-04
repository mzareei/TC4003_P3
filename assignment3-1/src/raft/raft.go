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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type state int

const (
	follower state = iota
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex
	peers             []*labrpc.ClientEnd
	persister         *Persister
	me                int // index into peers[]
	currentTerm       int
	transitionToState chan state
	currentState      state
	votedFor          int
	timeoutTimer      *timeoutTimer
	heartbeatTicker   *heartbeatTicker
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type heartbeatTicker struct {
	ticker   *time.Ticker
	duration time.Duration
}

func (ht *heartbeatTicker) reset (){
	ht.ticker.Stop()
	ht.ticker.Reset(ht.duration)
}

func (ht *heartbeatTicker) stop (){
	ht.ticker.Stop()
}

type timeoutTimer struct {
	timer    *time.Timer
	duration time.Duration
}

func (tt *timeoutTimer) reset (){
	tt.timer.Stop()
	tt.timer.Reset(tt.duration)
}

func (tt *timeoutTimer) stop (){
	tt.timer.Stop()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.currentState == leader
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
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Server %d received request vote of term %d from %d.\n", rf.me, args.Term, args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
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

type AppendEntriesArgs struct {
	Term    int
	Entries map[int]int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Entries == nil {
		rf.heartbeatReceived(args.Term)
	}
}

func (rf *Raft) heartbeatReceived(heartbeatTerm int) {
	DPrintf("Server %d received heartbeat of term %d.\n", rf.me, heartbeatTerm)
	if heartbeatTerm > rf.currentTerm {
		rf.currentTerm = heartbeatTerm
		rf.transitionToState <- follower
	} else {
		rf.timeoutTimer.reset()
	}

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.heartbeatTicker.stop()
	rf.timeoutTimer.stop()
	close(rf.transitionToState)
}

//
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
	rf.transitionToState = make(chan state)

	d := time.Duration(rand.Intn(150)+150) * time.Millisecond
	timer := time.AfterFunc(d, func() {
		DPrintf("Server %d timed out! Becoming candidate.\n", rf.me)
		rf.transitionToState <- candidate
	})
	timer.Stop()
	rf.timeoutTimer = &timeoutTimer{
		timer: timer,
		duration: d,
	}

	d = time.Duration(rand.Intn(10)+1) * time.Millisecond
	ticker := time.NewTicker(d)
	ticker.Stop()
	rf.heartbeatTicker = &heartbeatTicker{
		ticker: ticker,
		duration: d,
	}

	go rf.handleStateTransition()
	rf.transitionToState <- follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) handleStateTransition() {
	for state := range rf.transitionToState {
		switch state {
		case follower:
			if rf.currentState == leader {
				rf.heartbeatTicker.stop()
			}
			rf.currentState = follower
			rf.votedFor = -1
			rf.timeoutTimer.reset()
			DPrintf("Server %d is follower.\n", rf.me)
		case candidate:
			rf.currentState = candidate
			rf.currentTerm++
			rf.timeoutTimer.reset()
			go rf.sendRequestVoteToPeers()
			DPrintf("Server %d is candidate.\n", rf.me)
		case leader:
			rf.currentState = leader
			rf.timeoutTimer.stop()
			go rf.sendHeartbeats()
			DPrintf("Server %d is leader for term %d.\n", rf.me, rf.currentTerm)
		}
	}
}

func (rf *Raft) sendRequestVoteToPeers() {
	votes := 1 // vote for itself
	var majority int = len(rf.peers)/2 + 1
	for peer := range rf.peers {
		if peer != rf.me {
			DPrintf("Server %d sending RequestVote to %d\n", rf.me, peer)
			reply := RequestVoteReply{}
			rf.sendRequestVote(
				peer,
				RequestVoteArgs{
					rf.currentTerm,
					rf.me,
				},
				&reply,
			)
			if reply.VoteGranted {
				votes++
				DPrintf("Server %d has %d votes (needs >= %d).\n", rf.me, votes, majority)
				if votes >= majority {
					rf.transitionToState <- leader // win the election
					break
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.transitionToState <- follower
					break
				}
			}
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.heartbeatTicker.reset()
	for range rf.heartbeatTicker.ticker.C {
		for p := range rf.peers {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(
				p,
				AppendEntriesArgs{
					rf.currentTerm,
					nil,
				},
				&reply,
			)
			if reply.Term > rf.currentTerm {
				rf.transitionToState <- follower
				break
			}
		}
	}
}
