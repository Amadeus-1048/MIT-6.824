package raft

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
		return // 漏了会导致 Test (2B): agreement after follower reconnects 失败
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
	response.Term = rf.currentTerm
	response.Success = true
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

// used by Start function to append a new Entry to logs
func (rf *Raft) appendNewEntry(command interface{}) Entry {
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
