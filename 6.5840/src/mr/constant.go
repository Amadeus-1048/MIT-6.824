package mr

type JobType uint8

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

type TaskStatus uint8

const (
	Idle TaskStatus = iota // 闲置的
	Working
	Finished
)
