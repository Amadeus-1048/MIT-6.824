package kvraft


const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	*CommandRequest
}

type CommandRequest struct {
	Key string
	Value string
	Op op
	ClientID int64
	CommandID int64
}

func (request CommandRequest) String() string {
	return fmt.Sprintf("{Key:%v, Value:%v, Op:%v, ClientId:%v, CommandId:%v}", request.Key, request.Value, request.Op, request.ClientId, request.CommandId)
}

type CommandResponse struct {
	Err   Err
	Value string
}

func (response CommandResponse) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", response.Err, response.Value)
}

type Operation uint8

func (op Operation) String() string {
	switch op {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	}
	panic(fmt.Sprintf("unexpected Operation %d", op))
}

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
