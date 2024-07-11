package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)



type KVServer struct {
	mu      sync.RWMutex	// 确保多个 goroutine 同时访问 KVServer 时不会出现数据竞争
	me      int	
	rf      *raft.Raft	// Raft 协议的实例
	applyCh chan raft.ApplyMsg	// Raft 实例在日志条目被提交时会通过applyCh发送消息，KVServer 会从applyCh中读取消息并应用到状态机中
	dead    int32 // set by Kill()	标识服务器是否已停止运行

	maxraftstate int // 指定 Raft 日志的最大大小。日志增长超过这个大小时会触发快照以压缩日志
	lastApplied int // 记录最后一次应用到状态机的日志索引，以防止状态机回滚

	stateMachine kVStateMachine	// 键值状态机， 提供键值存储的具体实现
	lastOperations map[int64]OperationContext	// 记录每个客户端的最后一个命令 ID 和响应，防止重复执行相同的命令。客户端 ID 作为键，OperationContext 作为值，存储每个客户端的操作上下文
	notifyChans map[int]chan *CommandResponse 	// 用于通知客户端请求的通道。服务器在应用日志后会通过相应的通道通知等待响应的客户端 goroutine，以便它们可以返回结果
}

// 通过Raft协议处理客户端请求
func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), request, response)
	// 加读锁检查请求是否重复, 如果请求不是OpGet且是重复请求，直接返回上一次的响应结果
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicateRequest(request.clientID, request.commandID) {	
		lastResponse := kv.lastOperations[request.ClientID].LastResponse
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	// 释放锁以提高吞吐量，确保Raft层可以继续提交日志。
	// 调用kv.rf.Start提交请求到Raft日志，如果当前节点不是领导者，返回错误
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(Command{request})	// 将请求封装为Command提交到Raft日志中
	if !isLeader {
		response.Err = ErrWrongLeader
		return 
	}
	// 加锁获取通知通道
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	// 等待对应日志索引的通知通道返回结果或超时
	select {	
	case result := <- ch:
		response.Value, response.Err = result.Value, result.Err
	case <- time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	// 异步清理通知通道，释放内存，避免阻塞客户端请求
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan()
		kv.mu.Unlock()
	}()
}

// 判断一个客户端请求是否是重复请求。
// 假设每个新的RPC请求都意味着客户端已经收到并处理了之前的所有请求的回复。因此，如果当前请求的ID小于等于记录的最大命令ID，则认为是重复请求。
func (kv *KVServer) isDuplicateRequest(clientID int64, requestID int64) bool {
	operationContext, ok := kv.lastOperations[clientID]	// 获取clientID对应客户端的最新操作上下文
	// 如果查找到操作上下文，并且请求ID小于等于该客户端的最大已处理命令ID，则表示该请求是重复请求
	return ok && requestID <= operationContext.MaxAppliedCommandID
}

// 获取用于通知客户端的通道chan *CommandResponse。通道在Raft日志条目被应用到状态机之后，通知等待结果的客户端
// index：Raft日志条目的索引，用于标识该日志条目
// chan *CommandResponse：通道用于通知特定索引的日志条目应用结果
func (kv *KVServer) getNotifyChan(index int) chan *CommandResponse {	// 确保每个Raft日志条目都有一个对应的通道	
	if _, ok := kv.notifyChans[index]; !ok {	// 检查通道是否存在
		kv.notifyChans[index] = make(chan *CommandResponse, 1)	// 创建通道
	}
	return kv.notifyChans[index]	// 返回通道
}

// 当Raft日志条目被应用并且客户端已经被通知后，相关的通知通道就不再需要了。为了避免内存泄漏和无用的资源占用，需要删除这些过时的通道
func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
