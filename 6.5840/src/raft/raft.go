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
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// 保护对 Raft 结构体中共享数据的访问，确保在多个协程操作这些数据时的线程安全。
	mu sync.RWMutex // Lock to protect shared access to this peer's state
	// 存储所有节点（peers）的 RPC 端点，使当前节点能够与集群中的其他节点通信。
	peers []*labrpc.ClientEnd // RPC end points of all peers
	// 用于持久化存储节点的状态，如当前任期号、投票信息和日志条目。这确保了即使节点崩溃重启，也能恢复其状态
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]   存储当前节点在 peers 数组中的索引，即当前节点的唯一标识
	dead      int32      // 标记节点是否已经被关闭， set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh        chan ApplyMsg // 一个通道，向应用层提交已提交的日志条目
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

// Snapshot 在服务端触发日志压缩。index 表示快照覆盖到的日志索引，snapshot 是一个字节数组，包含了要保存的快照数据
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 加锁与检查索引
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex { // 检查传入的 index 是否 <= 当前快照的索引（snapshotIndex）
		// 如果是，即传入的快照已经是旧的或与当前状态重叠，则返回，不进行日志压缩
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v "+
			"as current snapshotIndex %v is larger in term %v",
			rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	// 压缩日志
	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:]) // 移除索引 <= index 的日志条目，保留之后的日志条目
	rf.logs[0].Command = nil                                    // 将压缩后的第一个日志条目的命令设置为 nil，
	// 这表示这个条目是压缩后的第一个条目，它的索引对应快照中的最后一个状态，nil 标记了一个新的快照起点

	// 保存状态和快照
	// todo 将当前的 Raft 状态和传入的快照数据一起保存到持久化存储中
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,"+
		"lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v "+
		"as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex,
		rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)

}

// InstallSnapshot 处理领导者节点发送来的安装快照请求, 在追随者节点上更新状态以匹配领导者的快照
func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	// 加锁和设置响应的任期号
	rf.mu.Lock()
	defer rf.mu.Unlock()
	response.Term = rf.currentTerm // 设置响应的任期号为当前节点的任期号
	// 检查请求的任期号
	if request.Term < rf.currentTerm { // 请求中的任期号小于当前节点的任期号
		return // 直接返回。因为这意味着请求来自一个过时的领导者。
	}
	if request.Term > rf.currentTerm { // 请求中的任期号大于当前节点的任期号
		rf.currentTerm = request.Term // 更新当前节点的任期号
		rf.votedFor = -1              // 重置投票信息
		rf.persist()
	}
	// 变更节点状态并重置选举计时器
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout()) // 重置选举计时器，以防止在处理快照期间发生不必要的选举
	// 检查快照是否过时
	if request.LastIncludedIndex <= rf.commitIndex {
		return // 快照是过时的，不进行任何操作
	}
	// 异步发送快照到应用通道
	go func() { // 启动一个新的协程来将快照信息发送到应用通道（applyCh）, 允许追随者异步地处理快照，不会阻塞当前的执行流程
		rf.applyCh <- ApplyMsg{ // 发送的 ApplyMsg 包含快照数据以及快照的任期和索引信息
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
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
	// 只有在 grant 投票时才重置选举超时时间，这样有助于网络不稳定条件下选主的 liveliness 问题
	rf.votedFor = request.CandidateId                   // 已经通过了所有检查，投票给发起请求的候选人
	rf.electionTimer.Reset(RandomizedElectionTimeout()) // 重置选举计时器（避免在已经投票的情况下启动新的选举）
	response.Term = rf.currentTerm
	response.VoteGranted = true // 在响应中表明已授予投票

	return
}

// AppendEntries 附加日志条目（Append Entries）RPC。
// 领导者（Leader）使用此 RPC 来复制日志条目到其他节点（Follower）
func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	// 加锁和持久化
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// 检查任期号
	if request.Term < rf.currentTerm { // 如果请求中的任期号小于当前节点的任期号，说明已经过期，拒绝请求
		response.Term = rf.currentTerm
		response.Success = false
		return
	}
	if request.Term > rf.currentTerm { // 如果请求中的任期号大于当前节点的任期号，则更新节点的任期号并重置投票信息
		// 在 Raft 算法中，如果一个节点（无论是领导者、候选人还是追随者）收到的 RPC 请求中包含的任期号大于其自身的当前任期号，
		// 这意味着存在一个更新的任期，节点之前的信息可能已经过时。在这种情况下，更新节点的任期号并重置投票信息是非常重要的
		rf.currentTerm = request.Term
		rf.votedFor = -1
	}
	// 变更状态和重置选举计时器
	rf.ChangeState(StateFollower)                       // 无论任期号如何，都将当前节点状态更改为Follower
	rf.electionTimer.Reset(RandomizedElectionTimeout()) // 并重置选举计时器

	// 检查日志一致性
	// 确保在执行追加日志条目之前，追随者的日志与领导者的日志在 PrevLogIndex 处是匹配的
	if request.PrevLogIndex < rf.getFirstLog().Index { // 如果 PrevLogIndex 比追随者的日志中的第一个条目的索引还小
		// 表明追随者缺少领导者假定其应该拥有的日志条目，或者追随者的日志已经被压缩
		// 在这种情况下，追随者不能正确地追加新的日志条目，因为它在日志中没有足够的历史信息来确保与领导者的日志一致
		response.Term = 0
		response.Success = false // 表示追随者无法追加日志条目
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} "+
			"because prevLogIndex %v < firstLogIndex %v",
			rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}
	// 检查领导者发送的 PrevLogTerm 和 PrevLogIndex 是否与当前日志匹配。如果不匹配，返回 false 并设置响应中的冲突信息
	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term = rf.currentTerm
		response.Success = false
		// 确定冲突条目的索引和任期号。 用于帮助领导者快速定位到日志不一致的位置，从而高效地修复日志不一致的问题
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex { // 如果追随者的日志比领导者请求的 PrevLogIndex 短.
			// 说明其缺少领导者期望的日志条目
			response.ConflictTerm = -1             // 将 ConflictTerm 设置为 -1（表示不存在的任期号）
			response.ConflictIndex = lastIndex + 1 // 设置 ConflictIndex 为追随者日志的下一个索引位置
		} else { // 如果追随者的日志包含 PrevLogIndex，则找出在该位置及之前发生冲突的最早任期号和索引
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index-- // 发生冲突的最早索引 通过查找第一个任期号与 PrevLogIndex 处任期号不同的条目来实现
			}
			response.ConflictIndex = index
		}
	}
	// 追加日志条目
	// 在追随者的日志中追加或替换来自领导者的日志条目，以确保日志的一致性
	firstLogIndex := rf.getFirstLog().Index     // 获取第一个日志条目的索引，因为日志数组可能不是从索引 0 开始的
	for index, entry := range request.Entries { // 遍历请求中的日志条目
		if entry.Index-firstLogIndex >= len(rf.logs) || // 检查要追加的日志条目的索引是否在追随者当前日志的范围外
			rf.logs[entry.Index-firstLogIndex].Term != entry.Term { // 或指定索引处的日志条目的任期号与领导者的不一致
			// 需要在该位置追加或替换日志条目
			// rf.logs[:entry.Index-firstIndex] ：先保留直到新条目开始索引之前的所有日志条目
			// request.Entries[index:]... ：然后追加从当前条目开始直到请求中的最后一个条目
			// 使用 shrinkEntriesArray 函数来减少内存占用，特别是在删除大量旧日志条目的时候
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstLogIndex], request.Entries[index:]...))
			break
		}
	}
	// 更新当前节点的 commitIndex（已提交日志的最高索引），这是基于领导者的 LeaderCommit
	rf.updateCommitIndexForFollower(request.LeaderCommit)

	// 设置成功响应
	response.Term = request.Term
	response.Success = true
}

// replicateOneRound 领导者节点向指定的追随者节点（peer）发送日志复制或快照安装的请求
func (rf *Raft) replicateOneRound(peer int) {
	// 检查节点状态
	rf.mu.RLock()                // 使用读锁定来保护对状态的访问
	if rf.state != StateLeader { // 检查当前节点是否是领导者
		rf.mu.RUnlock() // 如果不是领导者，则解锁并直接返回，因为只有领导者才能发送日志复制或快照安装的请求
		return
	}
	// 确定发送日志复制还是快照安装
	preLogIndex := rf.nextIndex[peer] - 1     // 领导者认为追随者需要的下一个日志条目的前一个索引
	if preLogIndex < rf.getFirstLog().Index { // prevLogIndex 小于领导者日志中的第一个条目的索引，意味着追随者落后太多
		// 无法通过普通的日志复制来更新，只能使用快照
		// todo 领导者将发送一个快照安装请求

	} else { // 普通的日志复制就可以
		// 领导者发送一个日志复制请求
		request := rf.genAppendEntriesRequest(preLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) { // 发送请求
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response) // 处理响应
			rf.mu.Unlock()
		}
	}
}

// updateCommitIndexForLeader 在领导者节点上计算并更新 commitIndex
func (rf *Raft) updateCommitIndexForLeader() {
	n := len(rf.matchIndex) // matchIndex 数组记录了每个追随者最后一个与领导者匹配的日志条目的索引
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	insertionSort(srt)
	newCommitIndex := srt[n-(n/2+1)] // 数组的中位数索引代表了所有节点中大多数已复制日志条目的最小索引。
	if newCommitIndex > rf.commitIndex {
		// only update commitIndex for current term's log
		if rf.matchLog(rf.currentTerm, newCommitIndex) { // 检查新的 commitIndex 是否对应当前任期的日志条目
			// 因为 Raft 只允许在当前任期内提交日志条目
			DPrintf("{Node %d} update commitIndex from %d to %d with matchIndex %v in term %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex // 更新 commitIndex
			rf.applyCond.Signal()           // 并通过条件变量 applyCond 通知可能在等待应用日志的协程
		} else {
			DPrintf("{Node %d} can not update commitIndex from %d "+
				"because the term of newCommitIndex %d is not equal to currentTerm %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}
}

// updateCommitIndexForFollower 在追随者节点上根据领导者的 leaderCommit 来更新 commitIndex。
func (rf *Raft) updateCommitIndexForFollower(leaderCommit int) {
	// 如果 leaderCommit < rf.getLastLog().Index，意味着追随者的日志落后于领导者
	// 在后续的 AppendEntries 请求中，领导者将发送缺失的日志条目，以便追随者可以追上领导者的日志状态
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index) // 为了防止追随者提交尚未复制的日志条目
	if newCommitIndex > rf.commitIndex {
		// 如果 newCommitIndex < rf.commitIndex，意味着追随者已经接收并应用了领导者发送的所有日志条目，
		// 或者领导者发送的日志还没有追上追随者已有的日志
		DPrintf("{Node %d} update commitIndex from %d to %d with leaderCommit %d in term %d",
			rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal() // 通过条件变量 applyCond 通知可能在等待应用日志的协程
	}
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

// 发起新的领导者选举
func (rf *Raft) StartElection() {
	// 生成投票请求
	request := rf.genRequestVoteRequest()
	// 初始化变量和更新状态
	grantVotes := 1     // 节点首先给自己投票
	rf.votedFor = rf.me // 表示当前节点给自己投票
	rf.persist()        // 保存当前节点的最新状态
	// 向其他节点发送投票请求
	for peer := range rf.peers { // 遍历所有节点
		if peer == rf.me { // 跳过当前节点，因为它已经给自己投票
			continue
		}
		go func(peer int) { // 为每个节点启动一个协程来发送投票请求
			// 发送投票请求并处理响应
			response := &RequestVoteResponse{}
			if rf.sendRequestVote(peer, request, response) { // 通过 RPC 向其他节点发送请求并接收响应
				// 锁定并处理投票结果
				rf.mu.Lock() // 在处理投票响应时，锁定 Raft 节点以确保对状态的修改是线程安全的
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v "+
					"from {Node %v} after sending RequestVoteRequest %v in term %v",
					rf.me, response, peer, request, rf.currentTerm)
				// 确保当前节点任期没有变化并且仍然处于候选者状态时才处理投票结果
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {
						grantVotes += 1                   // 获得投票，则增加 grantedVotes 计数
						if grantVotes > len(rf.peers)/2 { // 获得超过半数节点的投票，则当前节点成为新的领导者
							DPrintf("{Node %v} receives majority votes in term %v",
								rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader) // 赢得选举后，切换到领导者状态
							// todo 广播
							// 通过 BroadcastHeartbeat 发送心跳信息
						}
					} else if response.Term > rf.currentTerm { // 收到的响应中任期号比当前节点更高，说明存在更新的领导者
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v "+
							"and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)  // 当前节点应切换回追随者状态
						rf.currentTerm = response.Term // 并更新自己的任期号
						rf.votedFor = -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

// 生成投票请求
func (rf *Raft) genRequestVoteRequest() *RequestVoteRequest {
	// 生成一个新的投票请求。这个请求包含当前节点的状态，例如任期号和日志信息，用于请求其他节点的投票。
	lastLog := rf.getLastLog()
	request := &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
	}
	return request
}

// genAppendEntriesRequest 用于Leader发送一个日志复制请求，参数表示将要发送的日志条目中的前一个日志条目的索引
func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	// 获取追随者日志中第一个条目的索引
	firstIndex := rf.getFirstLog().Index                               // 日志数组可能不是从索引 0 开始的（特别是在实现日志压缩时）
	entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:])) // 即将要发送的日志条目（前一条日志索引+1到最后）
	copy(entries, rf.logs[prevLogIndex+1-firstIndex:])
	request := &AppendEntriesRequest{
		Term:         rf.currentTerm,                        // 设置请求的 Term 为领导者的当前任期
		LeaderId:     rf.me,                                 // 领导者自身的节点 ID
		PrevLogIndex: prevLogIndex,                          // 这些日志条目前一个条目的索引
		PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term, // prevLogIndex 所在条目的任期号
		Entries:      entries,                               // 要复制的日志条目
		LeaderCommit: rf.commitIndex,                        // 领导者的 commitIndex，告诉追随者领导者已提交的日志条目的最高索引
	}
	return request
}

// handleAppendEntriesResponse 领导者处理日志复制响应
func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	// 检查领导者状态和任期匹配
	if rf.state == StateLeader && rf.currentTerm == request.Term { // 检查当前节点是否仍是领导者，并且处理的响应是针对当前任期内发出的请求
		if response.Success { // 如果响应成功，说明追随者成功复制了日志条目
			// 更新对应追随者的 matchIndex 和 nextIndex
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries) // 更新最后一个复制的日志条目的索引
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1                      // 更新下一个要发送的日志条目的索引
			rf.updateCommitIndexForLeader()                                   // 尝试更新 commitIndex
		} else { // 处理失败的响应
			if response.Term > rf.currentTerm { // 响应包含的任期号大于当前任期号，这表明存在一个更新的领导者
				rf.ChangeState(StateFollower)  // 当前节点应变成追随者
				rf.currentTerm = response.Term // 并更新任期号
				rf.votedFor = -1
				rf.persist()
			} else if response.Term == rf.currentTerm { // 任期号与当前任期号相同,意味着需要解决日志不一致的问题
				// 领导者需要调整下次向该追随者发送日志条目的起始位置
				// 设置追随者的下一个日志索引
				rf.nextIndex[peer] = response.ConflictIndex // ConflictIndex 由追随者提供，表示它在自己的日志中发现不匹配的第一个日志条目的索引
				// 解决日志不一致
				if response.ConflictTerm != -1 { //  追随者在自己的日志中找到了一个与领导者日志不匹配的特定任期号
					firstIndex := rf.getFirstLog().Index
					// 领导者遍历自己的日志，从 PrevLogIndex 开始向前查找，直到它达到日志数组的第一个元素。
					// 目的是在领导者日志中找到与 ConflictTerm 相同的任期号的最后一个日志条目
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						// 一旦找到这样的条目，将 nextIndex 更新为该条目的下一个索引（即 i + 1），
						// 这意味着下一次日志复制将从这个新的索引开始。
						// 这样做是为了在下一次尝试时跳过所有已知的不匹配的任期号的日志条目
						// 这段代码的目的是快速地定位到日志不一致的原点，从而减少领导者和追随者之间解决日志不一致所需的通信往返次数。
						// 通过这种方式，领导者可以更高效地与追随者同步日志，即使在面临日志不一致的情况下也能迅速恢复一致性。
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is "+
		"{state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} "+
		"after handling AppendEntriesResponse %v for AppendEntriesRequest %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied,
		rf.getFirstLog(), rf.getLastLog(), response, request)
}

// BroadcastHeartbeat 领导者节点向所有追随者广播心跳信号或触发日志复制。
// 接受一个布尔值 isHeartBeat，用于决定是发送心跳还是触发日志复制
func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers { // 遍历集群中的所有节点
		if peer == rf.me {
			continue // 节点不需要给自己发送心跳或复制日志
		}
		if isHeartbeat { // 当前操作是为了发送心跳信号
			// 心跳是空的日志条目，用来维持领导者的权威和防止追随者发起不必要的选举
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else { // 当前操作是为了触发日志复制
			// just signal replicator goroutine to send entries in batch
			// 给每个追随者节点发送信号给与该节点相关的 replicatorCond 条件变量
			// 这种机制用于日志条目的批量发送，允许合并多个日志条目以提高效率
			rf.replicatorCond[peer].Signal()
		}
	}
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
// 用于处理来自服务（如键值存储服务器）的命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 如果当前服务器不是领导者，它将返回 false，表示无法开始对命令的一致性达成过程。
	// 如果是领导者，方法将立即开始一致性达成过程，但不保证该命令最终会被提交到 Raft 日志中，因为领导者可能会失败或失去选举。
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock() // 锁定状态，以防止在处理命令的同时状态被其他协程更改
	defer rf.mu.Unlock()
	if rf.state != StateLeader { // 检查当前服务器是否为领导者
		isLeader = false
		return index, term, isLeader
	}
	newLog := rf.appendNewEntry(command) // 如果当前服务器是领导者，它会将新命令作为新日志条目追加到自己的日志中。
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v",
		rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false) // 触发日志复制过程
	index = newLog.Index         // 新追加日志条目的索引
	term = newLog.Term           // 新追加日志条目的任期号
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

func (rf *Raft) Me() int {
	return rf.me
}

// ticker 协程会定期收到两个 timer 的到期事件。
// 如果是 election timer 到期，则发起一轮选举；
// 如果是 heartbeat timer 到期且节点是 leader，则发起一轮心跳。
func (rf *Raft) ticker() {
	for rf.killed() == false { // 循环直到节点被停止
		// Your code here (2A)
		// Check if a leader election should be started.
		select { // 使用 select 语句来等待两个计时器中的任何一个到期
		case <-rf.electionTimer.C: // 选举计时器到期
			rf.mu.Lock()                                        // 锁定自身状态
			rf.ChangeState(StateCandidate)                      // 切换到候选者状态
			rf.currentTerm += 1                                 // 增加当前任期号
			rf.StartElection()                                  // 发起新一轮选举
			rf.electionTimer.Reset(RandomizedElectionTimeout()) // 重置选举计时器为一个随机超时时长（防止选举冲突）
			rf.mu.Unlock()                                      // 解锁状态
		case <-rf.heartbeatTimer.C: // 心跳计时器到期
			rf.mu.Lock()
			if rf.state == StateLeader { // 如果当前状态是领导者，则广播一轮心跳
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout()) // 重置心跳计时器为一个稳定的超时时长
			}
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350 milliseconds.
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

// used by AppendEntries to judge whether log is matched
// 判断接收到的 AppendEntries 请求中的索引位置的日志条目的任期号是否与提供的 term 相等
func (rf *Raft) matchLog(term, index int) bool {
	// 检查提供的索引 index 是否在当前节点日志数组的有效范围内
	// 如果索引大于当前节点日志的最后一个条目的索引，那么匹配失败
	// 如果索引有效，接下来检查索引位置的日志条目的任期号是否与提供的 term 相等。
	// 为了得到正确的日志条目，需要从索引 index 中减去第一个日志条目的索引，
	// 因为日志数组可能不是从索引 0 开始的（特别是在实现日志压缩时）
	// 如果这两个条件都满足（即索引在有效范围内，并且任期号匹配），则函数返回 true 表示日志匹配；否则返回 false
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term
}

// used by Start function to append a new Entry to logs
func (rf *Raft) appendNewEntry(command any) Entry {
	lastLog := rf.getLastLog()
	newLog := Entry{
		Index:   lastLog.Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me] = newLog.Index
	rf.nextIndex[rf.me] = newLog.Index + 1
	rf.persist()
	return newLog
}

// used by replicator goroutine to judge whether a peer needs replicating
// 判断给定的追随者（peer）是否需要进行日志复制
func (rf *Raft) needReplicate(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// 首先判断当前节点是否为领导者。在 Raft 中，只有领导者节点负责向追随者发送日志复制请求。
	// 然后追随者的最新日志索引必须小于领导者日志的最后一条条目的索引
	// 因为这样说明追随者的日志落后于领导者，需要进行日志复制
	if rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index {
		return true
	}
	return false
}

// HasLogInCurrentTerm is used by upper layer to detect whether there are any logs in current term
func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastLog().Term == rf.currentTerm
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

// Replicator 用于管理特定追随者（peer）的日志复制过程。
// 负责判断何时需要向追随者发送日志条目，并触发相应的复制操作
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()         // 锁定与特定追随者相关联的条件变量的锁
	defer rf.replicatorCond[peer].L.Unlock() // 在方法结束时释放锁
	// 循环检查是否需要复制
	for rf.killed() == false { // 循环会一直运行，直到该 Raft 实例被终止
		// 等待复制信号
		for !rf.needReplicate(peer) { // 如果当前没有需要复制到这个追随者的日志条目, 该协程会等待，直到收到复制的信号
			rf.replicatorCond[peer].Wait() // wait方法会阻塞协程直到其他协程在相同的条件变量上调用 Signal() 或 Broadcast()
		}
		// 触发日志复制
		rf.replicateOneRound(peer) // 执行一轮日志复制
	}
}

// applier 负责将已提交的日志条目应用到上层服务，并且确保每个日志条目都被准确且恰好一次地推送到应用通道（applyCh）
func (rf *Raft) applier() {
	// 循环检查是否有新的日志需要应用
	for !rf.killed() {
		rf.mu.Lock()
		// 检查并等待可应用的日志条目
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex { // 如果最后应用到状态机的日志索引 大于或等于 已提交的最高日志索引
			rf.applyCond.Wait() // 则没有新的日志条目需要应用，因此在条件变量上等待，直到其他协程提交了新的日志条目
		}
		// 复制需要应用的日志条目
		firstIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied) // 计算需要应用的日志条目的范围，并创建一个切片存储这些日志条目
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		// 应用日志条目到上层服务
		for _, entry := range entries { // 遍历 entries 切片，将每个日志条目作为 ApplyMsg 发送到 applyCh
			rf.applyCh <- ApplyMsg{ // 触发上层服务（如键值存储服务）应用这些日志条目到其状态机
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		// 更新 lastApplied
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v",
			rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		// 更新 lastApplied 为最大的 commitIndex 值，确保不会重复应用相同的日志条目。
		// 这里使用 Max 函数是为了防止在应用日志期间接收到快照导致 lastApplied 回滚的情况
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
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
// 创建并初始化一个新的 Raft 服务器实例。
// 设置了 Raft 服务器的初始状态，并启动了一些长时间运行的协程（goroutines）来处理选举、日志复制和日志应用等任务
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 创建并初始化一个 Raft 实例
	rf := &Raft{
		peers:          peers,
		persister:      persister, // 用于持久化 Raft 状态的对象
		me:             me,        // this peer's index into peers[]
		dead:           0,
		applyCh:        applyCh, // 一个通道，用于发送应用到状态机的日志条目
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
	rf.readPersist(persister.ReadRaftState()) // 如果之前有保存的状态（例如在服务器重启后），则从持久化存储中恢复这些状态

	// 创建条件变量 applyCond，与 rf 的互斥锁 mu 相关联。用于控制日志的应用。
	rf.applyCond = sync.NewCond(&rf.mu)

	// 初始化日志复制相关字段
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{}) // 为每个追随者创建一个条件变量
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i) // 并启动一个 replicator 协程，用于管理该追随者的日志复制
		}
	}

	// start ticker goroutine to start elections
	// 启动 ticker 协程，用于管理选举和发送心跳
	go rf.ticker() // 用来触发 heartbeat timeout 和 election timeout

	// start applier goroutine
	// 启动 applier 协程，用于将已提交的日志条目发送到 applyCh并保证 exactly once，从而应用到上层服务的状态机中
	go rf.applier()
	return rf
}
