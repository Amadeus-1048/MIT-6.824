package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


// 客户端，负责与分布式键值存储系统进行交互
type Clerk struct {
	servers []*labrpc.ClientEnd
	leaderID int64	// Clerk需要跟踪当前的领导者节点，因为领导者是唯一可以处理客户端写请求的节点（Put和Append）
	clientID int64
	commandID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:servers,
		leaderID: 0,
		clientID: nrand(),
		commandID: 0,
	}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Command(
		&CommandRequest{
			Key: key,
			Op: OpGet,
		}
	)
}

func (ck *Clerk) Put(key string, value string) {
	return ck.Command(
		&CommandRequest{
			Key: key,
			Value: value,
			Op: OpPut,
		}
	)
}
func (ck *Clerk) Append(key string, value string) {
	return ck.Command(
		&CommandRequest{
			Key: key,
			Value: value,
			Op: OpAppend,
		}
	)
}


// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// 发送一个CommandRequest请求到集群中的服务器，并处理返回的CommandResponse响应
func (ck *Clerk) Command(request *CommandRequest) string {
	// 将请求的ClientID和CommandID设置为Clerk的当前clientID和commandID
	request.ClientID, request.CommandID = ck.clientID, ck.commandID
	for {	// 使用无限循环来处理请求发送和响应处理,确保请求最终能够成功
		response := &CommandResponse{}	// 存储服务器的响应
		// 用RPC发送请求到当前的领导者服务器
		if !ck.servers[ck.leaderID].Call("KVServer.Command", request, response) || // 如果调用失败
		response.Err == ErrWrongLeader  || 	// 或当前服务器不是领导者
		response.Err == ErrTimeout {	// 或请求超时
			ck.leaderID = (ck.leaderID + 1) % int64(len(ck.servers))	// 尝试下一个服务器
			continue
		}
		// 请求成功后命令ID递增，返回响应中的值
		ck.commandID++
		return response.Value
	}
}