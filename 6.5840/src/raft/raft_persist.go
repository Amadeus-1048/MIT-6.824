package raft

import (
	"6.5840/labgob"
	"bytes"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
	//rf.persister.Save(rf.encodeState(), nil)

}

// 需要持久化的状态：currentTerm, votedFor, log[]
// 持久化的时机是需要持久化的状态发生改变的时候
// log[]是唯一能用来重建应用程序状态的信息，所以Log必须要被持久化存储。
// votedFor是为了避免在一个term内，节点对其它两个节点投票，造成脑裂的情况。
// currentTerm被持久化，是为了防止网络分区的情况下（Leader写入了某个term的日志，但是该日志还没有追加给其它节点），一个分区内的节点宕机再恢复，如果只看日志决定term，会导致选择的term过旧，一个term内写的日志在两个分区内不一致。
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// restore previously persisted state. 从持久化存储中恢复之前保存的 Raft 状态。
// 接收一个字节切片 data，这个字节切片包含了之前持久化的 Raft 状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return // 接收一个字节切片 data，这个字节切片包含了之前持久化的 Raft 状态
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)    // 创建一个新的缓冲区, 并将 data 作为输入
	d := labgob.NewDecoder(r)     // 使用该缓冲区创建一个新的解码器 d
	var currentTerm, votedFor int // 存储解码后的状态
	var logs []Entry
	if d.Decode(&currentTerm) != nil || // 使用解码器 d 从缓冲区中解码 currentTerm、votedFor 和 logs
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs // 使用解码得到的数据更新 Raft 实例的状态
	// there will always be at least one entry in rf.logs
	rf.lastApplied, rf.commitIndex = rf.logs[0].Index, rf.logs[0].Index
}
