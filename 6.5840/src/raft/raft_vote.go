package raft

// RequestVote RPC handler.
// Follower 在选举过程中处理投票请求时调用
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

	// 投票决策（拒绝投票）
	if request.Term < rf.currentTerm || // 如果请求中的任期号小于当前节点的任期号,
		// 或者当前节点在当前任期已经投票给了其他候选人，则拒绝投票
		(request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		response.Term = rf.currentTerm // 当前节点的任期更大
		response.VoteGranted = false
		return
	}

	// 检查任期号
	if request.Term > rf.currentTerm { // 请求中的任期号大于当前节点的任期号
		rf.ChangeState(StateFollower) // 当前节点变回follower状态
		rf.currentTerm = request.Term // 更新自己的任期号
		rf.votedFor = -1              // 重置已投票状态
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
			response := new(RequestVoteResponse)
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
							rf.BroadcastHeartbeat(true) // 通过 BroadcastHeartbeat 发送心跳信息
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

// used by RequestVote to judge which log is newer
// 在处理投票请求时判断candidate的日志是否足够新
func (rf *Raft) isLogUpToDate(term, index int) bool { // term, index: 候选人的最后日志条目的任期号和索引
	lastLog := rf.getLastLog() // 获取当前节点的最后一个日志条目
	// 判断候选人的日志是否至少和当前节点的日志一样新
	// 如果候选人的最后日志条目的任期号大于当前节点最新日志的任期号，则认为候选人的日志是更新的
	// 如果候选人的最后日志条目的任期号与当前节点最新日志的相同，但日志条目的索引大于等于当前节点的最后日志条目的索引，
	// 则也认为候选人的日志是至少和当前节点一样新的。

	// 之前写成了term > rf.currentTerm，导致test fail
	if term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index) {
		return true
	}
	return false
}
