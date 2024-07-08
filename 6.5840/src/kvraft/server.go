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
	mu      sync.Mutex	// 确保多个 goroutine 同时访问 KVServer 时不会出现数据竞争
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


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
