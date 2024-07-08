package kvraft


const Debug = false
const ExecuteTimeout = 500 * time.Millisecond


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

const (
	OpPut Operation = iota
	OpAppend
	OpGet
)

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

// 跟踪客户端请求的状态和响应（防止重复处理、提高效率、保证幂等性、状态跟踪）
type OperationContext struct {
	MaxAppliedCommandID int64	// 记录已成功应用的最大命令ID
	LastResponse        *CommandResponse	// 存储上一次处理请求的响应
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
