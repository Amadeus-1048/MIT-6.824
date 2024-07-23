package raft

import "fmt"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // Raft 日志条目的索引值。每次在 Raft 中成功提交一个日志条目，都会为该条目分配一个唯一的 commandIndex。
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (msg ApplyMsg) String() string {
	if msg.CommandValid {
		return fmt.Sprintf("{Command:%v,CommandTerm:%v,CommandIndex:%v}", msg.Command, msg.CommandTerm, msg.CommandIndex)
	} else if msg.SnapshotValid {
		return fmt.Sprintf("{Snapshot:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.Snapshot, msg.SnapshotTerm, msg.SnapshotIndex)
	} else {
		panic(fmt.Sprintf("unexpected ApplyMsg{CommandValid:%v,CommandTerm:%v,CommandIndex:%v,SnapshotValid:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.CommandValid, msg.CommandTerm, msg.CommandIndex, msg.SnapshotValid, msg.SnapshotTerm, msg.SnapshotIndex))
	}
}

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
	Term         int     // leader’s term  发送时 Leader 的任期号
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones	预期要插入的日志条目位置紧接着前面的一个日志的下标，插入操作前要校对日志下标和任期来确保之前的日志全部同步
	PrevLogTerm  int     // term of prevLogIndex entry	预期要插入的日志条目位置紧接着前面的一个日志的任期，插入操作前要校对日志下标和任期来确保之前的日志全部同步
	LeaderCommit int     // leader’s commitIndex	Leader 节点当前的 commitIndex
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
	LastIncludedTerm  int // 最后被快照的日志条目的任期
	LastIncludedIndex int // 最后被快照的日志条目的下标索引
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

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, response)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, response)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", request, response)
}
