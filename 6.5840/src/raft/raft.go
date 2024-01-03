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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// 保护对 Raft 结构体中共享数据的访问，确保在多个协程操作这些数据时的线程安全。
	mu sync.Mutex // Lock to protect shared access to this peer's state
	// 存储所有节点（peers）的 RPC 端点，使当前节点能够与集群中的其他节点通信。
	peers []*labrpc.ClientEnd // RPC end points of all peers
	// 用于持久化存储节点的状态，如当前任期号、投票信息和日志条目。这确保了即使节点崩溃重启，也能恢复其状态
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]   存储当前节点在 peers 数组中的索引，即当前节点的唯一标识
	dead      int32      // 标记节点是否已经被关闭， set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh        chan ApplyMsg // 向应用层提交已提交的日志条目
	applyCond      *sync.Cond    // 当有新的日志条目被提交时，唤醒应用日志条目的协程
	replicatorCond []*sync.Cond  // 每个follower有一个与之对应的条件变量，用于触发日志复制的协程
	state          NodeState     // 节点当前的状态: Follower、Candidate、Leader

	// Persistent state on all servers
	currentTerm int     // 当前的任期号，用于Leader选举和日志复制
	votedFor    int     // 当前任期内投票给的Candidate的索引。如果没有投票，则为 -1。
	logs        []Entry // 节点的日志条目数组，日志条目包含命令以及它们被提交时的任期号

	// Volatile state on all servers
	commitIndex int // 已知被提交的最新的日志条目的索引
	lastApplied int // 已经被应用到状态机的最新的日志条目的索引

	// Volatile state on leaders
	nextIndex  []int // 对于每个节点，需要发送给它的下一个日志条目的索引
	matchIndex []int // 对于每个节点，已经复制到该节点的最新日志条目的索引

	// Timer
	electionTimer  *time.Timer // 用于触发选举的计时器。如果在超时时间内没有收到leader的心跳，则启动新的选举
	heartbeatTimer *time.Timer // leader用于发送心跳的计时器，以防止follower超时并启动新的选举
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
}

// example RequestVote RPC handler.
// 在选举过程中请求投票时调用
func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {
	// Your code here (2A, 2B).

	// 加锁和持久化
	rf.mu.Lock()         // 锁定 Raft 节点的互斥锁。这确保了在处理请求期间 Raft 状态的一致性。
	defer rf.mu.Unlock() // 在方法返回之前释放锁
	defer rf.persist()   // 确保 Raft 节点的当前状态在处理完请求后被持久化。

	// 日志打印（调试用）   打印节点在处理投票请求之前和之后的状态
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,"+
		"firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied,
		rf.getFirstLog(), rf.getLastLog(), request, response)

	// 投票决策
	if request.Term < rf.currentTerm || // 如果请求中的任期号小于当前节点的任期号,
		// 或者当前节点在当前任期已经投票给了其他候选人，则拒绝投票
		(request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		response.Term = rf.currentTerm
		response.VoteGranted = false
		return
	}

	// 检查任期号
	if request.Term > rf.currentTerm {
		// 如果请求中的任期号大于当前节点的任期号，当前节点需要更新自己的任期号，并变回follower状态，重置已投票状态
		rf.ChangeState(StateFollower)
		rf.currentTerm = request.Term
		rf.votedFor = -1
	}

	// 检查日志是否最新
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		// 检查候选人的日志是否至少和自己的一样新。如果不是，则拒绝投票
		response.Term = rf.currentTerm
		response.VoteGranted = false
		return
	}

	// 投票给候选人
	rf.votedFor = request.CandidateId                   // 已经通过了所有检查，投票给发起请求的候选人
	rf.electionTimer.Reset(RandomizedElectionTimeout()) // 重置选举计时器（避免在已经投票的情况下启动新的选举）
	response.Term = rf.currentTerm
	response.VoteGranted = true // 在响应中表明已授予投票

	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

// used by RequestVote to judge which log is newer
// 在处理投票请求时判断candidate的日志是否足够新
func (rf *Raft) isLogUpToDate(term, index int) bool { // term, index: 候选人的最后日志条目的任期号和索引
	lastLog := rf.getLastLog() // 获取当前节点的最后一个日志条目
	// 判断候选人的日志是否至少和当前节点的日志一样新
	// 如果候选人的最后日志条目的任期号大于当前节点的当前任期号，则认为候选人的日志是更新的
	// 如果候选人的最后日志条目的任期号与当前节点相同，但日志条目的索引大于等于当前节点的最后日志条目的索引，
	// 则也认为候选人的日志是至少和当前节点一样新的。
	if term > rf.currentTerm || (term == lastLog.Index && index >= lastLog.Index) {
		return true
	}
	return false
}

func (rf *Raft) ChangeState(state NodeState) {
	// 如果节点已经处于请求的状态，则无需进行任何操作
	if state == rf.state {
		return
	}
	// 打印状态变化（调试用）
	DPrintf("{Node %d} changes state from %d to %d in term %d",
		rf.me, rf.state, state, rf.currentTerm)
	// 将节点的状态更新为新的状态
	rf.state = state
	// 根据新状态执行操作
	switch state {
	case StateFollower:
		rf.heartbeatTimer.Stop()                            // 停止心跳定时器
		rf.electionTimer.Reset(RandomizedElectionTimeout()) // 重置选举定时器（设置随机超时，以防止选举冲突）
	case StateCandidate:
	// No specific action for candidate
	// 候选者的行为通常在其他地方实现，如启动选举
	case StateLeader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			// 初始化 matchIndex 和 nextIndex, 用于跟踪日志复制进度
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()                           // 停止选举定时器
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout()) // 重置心跳定时器, Leader通过定期向Followers发送心跳信息维持其统治
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 创建并初始化一个 Raft 实例
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me, // this peer's index into peers[]
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// 从持久化存储中读取并恢复 Raft 状态。这对于在崩溃后重启节点很重要，可以从最后保存的状态继续运行。
	rf.readPersist(persister.ReadRaftState())

	// 创建条件变量 applyCond，与 rf 的互斥锁 mu 相关联。用于控制日志的应用。
	rf.applyCond = sync.NewCond(&rf.mu)

	// todo 初始化日志复制相关字段

	// start ticker goroutine to start elections
	// 启动定时器
	go rf.ticker() // 用来触发 heartbeat timeout 和 election timeout

	// todo start applier goroutine
	// 启动应用goroutine
	// 用来往 applyCh 中 push 提交的日志并保证 exactly once
	return rf
}
