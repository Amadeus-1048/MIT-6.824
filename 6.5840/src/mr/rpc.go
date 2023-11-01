package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 用于心跳信号的请求。通常，心跳请求不需要额外信息
type HeartbeatRequest struct{}

// 用于心跳响应
type HeartbeatResponse struct {
	FilePath string
	JobType  JobType
	NReduce  int // Reduce 任务的数量
	NMap     int // Map 任务的数量
	Id       int
}

func (response HeartbeatResponse) String() string {
	switch response.JobType {
	case MapJob:
		return fmt.Sprintf("{JobType:%v,FilePath:%v,Id:%v,NReduce:%v}", response.JobType, response.FilePath, response.Id, response.NReduce)
	case ReduceJob:
		return fmt.Sprintf("{JobType:%v,Id:%v,NMap:%v,NReduce:%v}", response.JobType, response.Id, response.NMap, response.NReduce)
	case WaitJob, CompleteJob:
		return fmt.Sprintf("{JobType:%v}", response.JobType)
	}
	panic(fmt.Sprintf("unexpected JobType %d", response.JobType))
}

// 用于报告任务进度的请求
type ReportRequest struct {
	Id    int           // 任务 ID
	Phase SchedulePhase // 当前的调度阶段
}

func (request ReportRequest) String() string {
	return fmt.Sprintf("{Id:%v,SchedulePhase:%v}", request.Id, request.Phase)
}

// 用于任务报告的响应。表示调度器已收到并处理了报告
type ReportResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// 用于生成一个唯一的 UNIX-domain socket 名称。它以固定的前缀开始，然后加上当前用户的 UID
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
