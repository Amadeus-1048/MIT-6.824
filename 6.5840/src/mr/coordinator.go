package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	response *ReportRequest
	ok       chan struct{}
}

// 无状态、基于channel
type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// 处理工作节点的心跳信号
// 心跳信号是工作节点定期发送给协调器的，以证明它们仍然活着，并可能请求新的工作
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{
		response: response,
		ok:       make(chan struct{}),
	}
	// 将 msg 推送到处理管道中，从而让负责心跳处理的逻辑能够接手并开始处理心跳
	c.heartbeatCh <- msg // 如果没有其他协程准备从这个通道接收数据，这个发送操作将会阻塞，直到另一个协程准备好从该通道接收数据
	<-msg.ok
	// msg.ok 是一个无缓冲的通道，这意味着发送操作和接收操作必须同时发生，否则会阻塞等待对方。
	// 当处理心跳的协程完成了对心跳消息的处理（可能是更新内部状态、调度任务等），它会向 msg.ok 通道发送一个信号（空结构体）。
	// 这个信号不携带任何数据（因为它是一个 struct{}），它的目的仅仅是通知 Heartbeat 方法可以继续执行

	// <-msg.ok 这行代码的作用是同步：它确保了 Heartbeat 方法在返回前等待心跳消息被实际处理。
	// 只有当处理协程显式地向 msg.ok 通道发送一个信号时，这个等待才会解除，
	// Heartbeat 方法才会继续到达 return nil 这行代码，然后返回，这表明心跳处理流程已经完成，RPC 调用可以安全地返回了。
	return nil
}

// 处理 worker 发送的任务完成报告
func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{
		response: request,
		ok:       make(chan struct{}), // 这种类型的通道通常用于信号传递，而不是数据传递。它的目的是同步，确保报告被处理后再继续。
	}
	c.reportCh <- msg
	<-msg.ok // 等待接收 msg.ok 通道上的信号, 是一个典型的同步等待模式，确保任务报告被正确处理之前，方法不会返回。
	return nil
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase

}

func (c *Coordinator) initReducePhase() {

}

func (c *Coordinator) initCompletePhase() {

}

func (c *Coordinator) schedule() {

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
