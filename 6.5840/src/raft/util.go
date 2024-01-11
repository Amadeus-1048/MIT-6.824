package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// NodeState ：Raft将系统中的角色分为领导者（Leader）、跟从者（Follower）和候选人（Candidate）
type NodeState uint8

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

// Entry :每个日志条目包含有序编号，它被创建时的任期号，和用于状态机执行的命令
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// Timer
type lockedRand struct {
	mu   sync.Mutex // 互斥锁
	rand *rand.Rand // 随机数生成器
}

func (r *lockedRand) Intn(n int) int {
	// 确保即使在多协程的环境中，随机数的生成也是线程安全的
	r.mu.Lock()           // 锁定互斥锁
	defer r.mu.Unlock()   // 解锁
	return r.rand.Intn(n) // 执行随机数生成
}

// 全局变量，用当前时间初始化其随机数生成器。可以在程序的任何地方安全地生成随机数。
var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// 心跳超时和选举超时的基础时长
const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

// StableHeartbeatTimeout 返回一个固定的心跳超时时长（毫秒）。用于leader定期向follower发送心跳信号，维持其领导地位并防止follower启动新的选举
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

// RandomizedElectionTimeout 返回一个随机化的选举超时时长。
// 将基础选举超时时长（1000 毫秒）加上一个 0 到 1000 毫秒的随机数，这样总的超时时长会在 1000 到 2000 毫秒之间
// 在 Raft 算法中，随机化选举超时时间是避免选举冲突和split-brain情况的重要机制。
// 每个追随者都有一个随机的超时时长，在这段时间内如果没有收到领导者的心跳，它会启动新的领导者选举。
func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

// shrinkEntriesArray 是对 Raft 日志条目切片的内存优化措施。
// 当检测到切片只使用了一小部分分配的内存时，通过创建一个更小的切片并复制现有元素，可以减少内存的使用
func shrinkEntriesArray(entries []Entry) []Entry {
	const lenMultiple = 2                        // 用于判断切片的当前长度是否小于其容量的一定比例（即容量的一半）
	if len(entries)*lenMultiple < cap(entries) { // 切片的长度小于其容量的一半，意味着切片正在浪费一半以上的分配空间
		newEntries := make([]Entry, len(entries)) // 创建一个新的切片 newEntries，其长度与 entries 当前长度相同
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

// 插入排序 递增
func insertionSort(s []int) {
	// 将第一待排序序列第一个元素看做一个有序序列，把第二个元素到最后一个元素当成是未排序序列。
	// 从头到尾依次扫描未排序序列，将扫描到的每个元素插入有序序列的适当位置。
	for i := 1; i < len(s); i++ { // 未排序序列
		for j := i; j > 0 && s[j] < s[j-1]; j-- { // 在已排序序列中从后向前扫描，找到相应位置并插入
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
