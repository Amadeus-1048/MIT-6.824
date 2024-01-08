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
	Term         int     // leader’s term  当前任期
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones	前一个日志的 index
	PrevLogTerm  int     // term of prevLogIndex entry	前一个日志的 term
	LeaderCommit int     // leader’s commitIndex	已提交的日志 index
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency) 将要追加的日志列表（空则成为心跳包）
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
	ConflictIndex int  // 领导者应从该索引开始发送日志条目以解决冲突
	ConflictTerm  int  // PreLog位置发生冲突时，追随者在该位置的log term
}

func (response AppendEntriesResponse) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,ConflictIndex:%v,ConflictTerm:%v}",
		response.Term, response.Success, response.ConflictIndex, response.ConflictTerm)
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedTerm  int
	LastIncludedIndex int
	Data              []byte
}

func (request InstallSnapshotRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v,DataSize:%v}",
		request.Term, request.LeaderId, request.LastIncludedIndex, request.LastIncludedTerm, len(request.Data))
}

type InstallSnapshotResponse struct {
	Term int
}

func (response InstallSnapshotResponse) String() string {
	return fmt.Sprintf("{Term:%v}", response.Term)
}
