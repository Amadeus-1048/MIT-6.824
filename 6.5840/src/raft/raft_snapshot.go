package raft

// Snapshot 在服务端触发日志压缩。index 表示快照覆盖到的日志索引，snapshot 是一个字节数组，包含了要保存的快照数据
// 上层服务调用Snapshot()将其状态的快照传递给Raft。上层一旦调用下层的该方法，就会让下层修剪日志。
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
	rf.persister.Save(rf.encodeState(), snapshot) // 将当前的 Raft 状态和传入的快照数据一起保存到持久化存储中
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,"+
		"lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v "+
		"as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex,
		rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)

}

// InstallSnapshot 处理Leader发送的安装快照请求, 在一个非常落后的Follower上更新状态以匹配Leader的快照。
// InstallSnapshot RPC 和 AppendEntries RPC 在某种程度上是共通的，他们都是由 Leader 节点发起用于更新 Follower 节点信息的
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
	if request.LastIncludedIndex <= rf.commitIndex { // 说明本地已经包含了该 snapshot 所有的数据信息
		// 尽管可能状态机还没有这个 snapshot 新，即 lastApplied 还没更新到 commitIndex
		// 但是 applier 协程也一定尝试在 apply 了，此时便没必要再去用 snapshot 更换状态机了。
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

// CondInstallSnapshot 处理由上层服务触发的快照安装请求，与直接从领导者接收快照安装请求（InstallSnapshot）不同。
// 接收的参数为快照的任期号、索引以及快照数据。返回值表示是否接受了快照。
// 该方法是为了保证每次snapshot安装的状态都为最新apply的状态， 在config.go中的appliersnap()方法安装快照之后，
// 会更新该方法内的lastApplied（不是raft自身保存的lastApplied）为快照的最后的index（相当于回溯了）。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// 加锁并检查快照是否过时
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex { // 快照是过时的，直接返回 false
		return false
	}
	// 更新日志和状态
	if lastIncludedIndex > rf.getLastLog().Index { // 快照索引超过了当前日志的最后索引
		rf.logs = make([]Entry, 1) // 创建一个只包含哑元条目的新日志数组
	} else { // 压缩现有日志以匹配快照
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}
	// 更新哑元条目的任期和索引为快照的任期和索引
	rf.logs[0].Term = lastIncludedTerm
	rf.logs[0].Index = lastIncludedIndex
	// 将 lastApplied 和 commitIndex 更新为快照索引
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	// 保存状态和快照
	rf.persister.Save(rf.encodeState(), snapshot) // 将当前的 Raft 状态和新的快照数据保存到持久化存储中

	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,"+
		"lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot "+
		"which lastIncludedTerm is %v, lastIncludedIndex is %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex,
		rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true // 表示快照已被接受并处理
}

// Leader向Follower发送快照安装请求。通常在Follower的日志严重落后或缺失时使用，以帮助其快速同步到Leader的当前状态
func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	// 获取 Raft 日志的第一个条目（firstLog），它通常是一个哑元条目，其索引和任期标记着快照的起点
	firstLog := rf.getFirstLog()
	request := &InstallSnapshotRequest{
		Term:              rf.currentTerm,              // 当前领导者的任期号
		LeaderId:          rf.me,                       // 领导者的节点 ID
		LastIncludedIndex: firstLog.Index,              // 快照覆盖的最后一个日志条目的索引
		LastIncludedTerm:  firstLog.Term,               // 快照覆盖的最后一个日志条目的任期号
		Data:              rf.persister.ReadSnapshot(), // 快照数据本身，从持久化存储中读取
	}
	return request
}

// Leader处理快照安装的响应，更新内部状态，包括日志匹配索引。
// 成功的响应会导致更新这些索引，从而确保领导者认为追随者的状态与自己同步。
func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	// 检查状态和任期
	if rf.state == StateLeader && rf.currentTerm == request.Term { // 检查当前节点是否仍是领导者，并且响应是针对当前任期内发出的请求
		// 处理不同的响应情况
		if response.Term > rf.currentTerm { // 表明存在一个更新的领导者
			rf.ChangeState(StateFollower)
			rf.currentTerm = response.Term
			rf.votedFor = -1
			rf.persist()
		} else {
			// 更新与该追随者相关的 matchIndex 和 nextIndex
			// 这表示领导者认为追随者已经接收并应用了快照中包含的所有日志条目，
			// 因此更新 matchIndex 为快照的最后包含索引，并将 nextIndex 设置为此后的下一个索引
			rf.matchIndex[peer] = request.LastIncludedIndex
			rf.nextIndex[peer] = request.LastIncludedIndex + 1
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,"+
		"lastApplied %v,firstLog %v,lastLog %v} after handling InstallSnapshotResponse %v "+
		"for InstallSnapshotRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex,
		rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
}
