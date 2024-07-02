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
func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientID, request.CommandID = ck.clientID, ck.commandID
	for {
		response := &CommandResponse{}
		if !ck.servers[ck.leaderID].Call("KVServer.Command", request, response)  {
			ck.leaderID = (ck.leaderID + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandID++
		return response.Value
	}
}