package raft

import "fmt"

// Arguments of RequestVote RPC
type RequestVoteRequest struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

func (request RequestVoteRequest) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}",
		request.Term, request.CandidateId, request.LastLogIndex, request.LastLogTerm)
}

// Results of RequestVote RPC
type RequestVoteResponse struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (response RequestVoteResponse) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}",
		response.Term, response.VoteGranted)
}

// Arguments of AppendEntries RPC
type AppendEntriesRequest struct {
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	LeaderCommit int     // leader’s commitIndex
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

func (request AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,"+
		"PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}",
		request.Term, request.LeaderId, request.PrevLogIndex,
		request.PrevLogTerm, request.LeaderCommit, request.Entries)
}

// Results of AppendEntries RPC
type AppendEntriesResponse struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

func (response AppendEntriesResponse) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,ConflictIndex:%v,ConflictTerm:%v}",
		response.Term, response.Success, response.ConflictIndex, response.ConflictTerm)
}
