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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	currentTerm int     // 当前的最新任期号，用于Leader选举和日志复制。初始化为0
	votedFor    int     // 当前任期内投票给Candidate的ID。如果没有投票，则为 -1。
	logs        []Entry // 节点的日志条目数组，日志条目包含命令以及它们被提交时的任期号。认定第一个日志条目的下标是 0

	// Volatile state on all servers
	commitIndex int // 已知被提交的最新的日志条目的索引。初始化为 0
	lastApplied int // 已经被应用到状态机的最新的日志条目的索引。初始化为 0

	// Volatile state on leaders
	nextIndex  []int // 对于每个节点，需要发送给它的下一个日志条目的索引。初始化为 Leader 节点的最后一个日志条目下标 +1
	matchIndex []int // 对于每个节点，已经复制到该节点的最新日志条目的索引。初始化为 0

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
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
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
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
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
// 作为 Leader 节点时接收新的请求 LogEntry，并新增相关的日志条目。
// 不是 Leader 节点时，返回相关的信息，例如当前节点认为的 Leader 节点是谁，使得请求可以重定向到 Leader 节点
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 如果当前服务器不是领导者，它将返回 false，表示无法开始对命令的一致性达成过程。
	// 如果是领导者，方法将立即开始一致性达成过程，但不保证该命令最终会被提交到 Raft 日志中，因为领导者可能会失败或失去选举。
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock() // 锁定状态，以防止在处理命令的同时状态被其他协程更改
	defer rf.mu.Unlock()
	if rf.state != StateLeader { // 检查当前服务器是否为领导者
		return index, term, isLeader
	}
	newLog := rf.appendNewEntry(command) // 如果当前服务器是领导者，它会将新命令作为新日志条目追加到自己的日志中。
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v",
		rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false) // 触发日志复制过程
	index = newLog.Index         // 新追加日志条目的索引
	term = newLog.Term           // 新追加日志条目的任期号
	isLeader = true
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
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
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
	for rf.killed() == false {
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
