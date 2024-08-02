package kvraft

// 键值状态机接口
type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

// 基于内存的键值存储实现
type MemoryKV struct {
	KV map[string]string // 存储键值对
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		KV: make(map[string]string),
	}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}
