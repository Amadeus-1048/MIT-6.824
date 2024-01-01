package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type NodeState uint8

// Entry :每个日志条目包含有序编号，它被创建时的任期号，和用于状态机执行的命令
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}
