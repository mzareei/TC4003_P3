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
	"fmt"
	"main/labrpc"
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

type Raft struct {
	sync.Mutex
	peers             []*labrpc.ClientEnd
	persister         *Persister
	me                int // index into peers[]
	currentTerm       int
	transitionToState chan state
	currentState      state
	votedFor          int
	timeoutTimer      *timeoutTimer
	heartbeatTicker   *heartbeatTicker
}

type heartbeatTicker struct {
	ticker *time.Ticker
}

func newHeartbeatTicker() *heartbeatTicker {
	d := time.Duration(rand.Intn(10)+1) * time.Millisecond
	ticker := time.NewTicker(d)
	ticker.Stop()
	return &heartbeatTicker{
		ticker: ticker,
	}
}

func (ht *heartbeatTicker) reset() {
	d := time.Duration(rand.Intn(10)+1) * time.Millisecond
	ht.ticker.Reset(d)
}

func (ht *heartbeatTicker) stop() {
	ht.ticker.Stop()
}

type timeoutTimer struct {
	timer   *time.Timer
	stopped chan struct{}
}

func newTimeoutTimer() *timeoutTimer {
	d := time.Duration(rand.Intn(150)+150) * time.Millisecond
	timer := time.NewTimer(d)
	timer.Stop()
	return &timeoutTimer{
		timer:   timer,
		stopped: make(chan struct{}),
	}
}

func (tt *timeoutTimer) reset() {
	d := time.Duration(rand.Intn(150)+150) * time.Millisecond
	tt.timer.Reset(d)
}

func (tt *timeoutTimer) stop() {
	tt.stopped <- struct{}{}
}

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

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Server %d received request vote of term %d from %d.\n", rf.me, args.Term, args.CandidateId)
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.transitionToFollower(args.CandidateId)
		reply.VoteGranted = true
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
}

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
	DPrintf("Server %d received heartbeat of term %d. Current term %d.\n", rf.me, heartbeatTerm, rf.currentTerm)
	if heartbeatTerm > rf.currentTerm {
		rf.currentTerm = heartbeatTerm
		rf.transitionToFollower(-1)
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

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.transitionToState = make(chan state)

	rf.timeoutTimer = newTimeoutTimer()
	go rf.timeoutTimerExpired()

	rf.heartbeatTicker = newHeartbeatTicker()
	go rf.sendHeartbeats()

	go rf.handleStateTransition()
	rf.transitionToFollower(-1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) transitionToFollower(votedFor int) {
	rf.votedFor = votedFor
	rf.transitionToState <- follower
}

func (rf *Raft) handleStateTransition() {
	for state := range rf.transitionToState {
		switch state {
		case follower:
			if rf.currentState == leader {
				rf.heartbeatTicker.stop()
			}
			rf.currentState = follower
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
			rf.heartbeatTicker.reset()
			DPrintf("Server %d is leader for term %d.\n", rf.me, rf.currentTerm)
		}
	}
}

func (rf *Raft) timeoutTimerExpired() {
	for {
		select {
		case <-rf.timeoutTimer.stopped:
			DPrintf("Server %d timeout timer stopped.\n", rf.me)
			rf.timeoutTimer.timer.Stop()
			<-rf.timeoutTimer.timer.C
		case <-rf.timeoutTimer.timer.C:
			DPrintf("Server %d timed out! Becoming candidate.\n", rf.me)
			rf.transitionToState <- candidate
		}
	}
}

func (rf *Raft) sendRequestVoteToPeers() {
	votes := 1 // vote for itself
	var majority int = len(rf.peers)/2 + 1
	for peer := range rf.peers {
		if peer != rf.me {
			DPrintf("Server %d sending RequestVote to %d\n", rf.me, peer)
			if reply, e := rf.sendRequestVoteWithTimeout(peer); e == nil {
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
						rf.transitionToFollower(-1)
						break
					}
				}
			} else {
				DPrintf("Server %d error sending RequestVote to %d: %s.\n", rf.me, peer, e.Error())
			}
		}
	}
}

func (rf *Raft) sendRequestVoteWithTimeout(peer int) (*RequestVoteReply, error) {
	replyReceived := make(chan RequestVoteReply)

	go func() {
		reply := RequestVoteReply{}
		rf.sendRequestVote(
			peer,
			RequestVoteArgs{
				rf.currentTerm,
				rf.me,
			},
			&reply,
		)
		replyReceived <- reply
	}()

	select {
	case reply := <-replyReceived:
		return &reply, nil
	case <-time.After(5 * time.Millisecond):
		return &RequestVoteReply{}, fmt.Errorf("request timed out")
	}
}

func (rf *Raft) sendHeartbeats() {
	for range rf.heartbeatTicker.ticker.C {
		for peer := range rf.peers {
			if peer != rf.me {
				DPrintf("Server %d sending heartbeat to %d.\n", rf.me, peer)
				if reply, e := rf.sendHeartbeatWithTimeout(peer); e == nil {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.transitionToFollower(-1)
						break
					}
				} else {
					DPrintf("Server %d error sending heartbeat to %d: %s.\n", rf.me, peer, e.Error())
				}

			}
		}
	}
}

func (rf *Raft) sendHeartbeatWithTimeout(peer int) (*AppendEntriesReply, error) {
	replyReceived := make(chan AppendEntriesReply)

	go func() {
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(
			peer,
			AppendEntriesArgs{
				rf.currentTerm,
				nil,
			},
			&reply,
		)
		replyReceived <- reply
	}()

	select {
	case reply := <-replyReceived:
		return &reply, nil
	case <-time.After(10 * time.Millisecond):
		return &AppendEntriesReply{}, fmt.Errorf("request timed out")
	}
}
