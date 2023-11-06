package mr

import (
	"fmt"
	"time"
)

const MaxTaskRunInterval = 10 * time.Second

type JobType uint8

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

func (job JobType) String() string {
	switch job {
	case MapJob:
		return "MapJob"
	case ReduceJob:
		return "ReduceJob"
	case WaitJob:
		return "WaitJob"
	case CompleteJob:
		return "CompleteJob"
	}
	panic(fmt.Sprintf("unexpected jobType %d", job))
}

type TaskStatus uint8

const (
	Idle TaskStatus = iota // 闲置的
	Working
	Finished
)

type SchedulePhase uint8

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

func (phase SchedulePhase) String() string {
	switch phase {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case CompletePhase:
		return "CompletePhase"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}
