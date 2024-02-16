package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	fileName  string // 文件名
	id        int
	startTime time.Time
	status    TaskStatus
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// 无状态、基于channel
type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int           // Reduce任务数量
	nMap    int           // Map任务数量
	phase   SchedulePhase // 当前阶段
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// 处理 worker 发送的心跳。Worker定期发送心跳信号给Coordinator，以证明它们仍然活着，并可能请求新的工作
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{
		response: response,
		ok:       make(chan struct{}), // 是一个无缓冲的通道，即发送操作和接收操作必须同时发生，否则会阻塞等待对方
	}
	// 将 msg 推送到heartbeatCh管道中，从而让负责处理心跳的函数能接手并开始处理心跳
	// 如果没有其他协程从这个通道接收数据，这个发送操作将会阻塞，直到一个协程从该通道接收数据
	c.heartbeatCh <- msg

	// 当处理心跳的协程完成了对心跳消息的处理，它会向 msg.ok 通道发送一个信号（空结构体）
	// 这个信号不携带任何数据（因为它是一个 struct{}），它的目的仅仅是通知 Heartbeat 方法可以继续执行
	<-msg.ok

	// <-msg.ok 这行代码的作用是同步：它确保了 Heartbeat 方法在返回前等待心跳消息被实际处理。
	// 只有当处理协程显式地向 msg.ok 通道发送一个信号时，这个等待才会解除，
	// Heartbeat 方法才会继续到达 return nil 这行代码，然后返回，这表明心跳处理流程已经完成，RPC 调用可以安全地返回了。
	return nil
}

// 处理 worker 发送的任务完成报告
func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{
		request: request,
		ok:      make(chan struct{}), // 这种类型的通道通常用于信号传递，而不是数据传递。它的目的是同步，确保报告被处理后再继续。
	}
	c.reportCh <- msg
	<-msg.ok // 等待接收 msg.ok 通道上的信号, 是一个典型的同步等待模式，确保任务报告被正确处理之前，方法不会返回。
	return nil
}

// 任务调度器初始化Map阶段
func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files)) // 存储所有的任务, 每个输入文件将对应一个Map任务
	for index, file := range c.files {
		c.tasks[index] = Task{
			fileName: file,
			id:       index,
			status:   Idle, // 这个任务目前还没有被分配出去, 状态设置为待处理
		}
	}
}

// 在Map任务全部完成之后，为接下来的Reduce阶段做准备
func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce) // 会有c.nReduce个Reduce任务需要被执行
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle, // 这个任务目前还没有被分配出去, 状态设置为待处理
		}
	}
}

// 设置协调器的状态到完成状态，并且发出一个信号表示所有MapReduce任务已经执行完毕
func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	// 用来通知其他正在等待任务完成的Goroutine，MapReduce任务已经全部结束了。
	// 在doneCh上等待（<-c.doneCh）的Goroutine将接收到这个信号并继续执行。
	c.doneCh <- struct{}{}
}

// 用于初始化和管理不同阶段的任务分配和监控，是MapReduce框架中Coordinator的核心功能
func (c *Coordinator) schedule() {
	// 初始化Map任务
	c.initMapPhase() // 设置当前的任务阶段为MapPhase，并初始化一个任务列表（tasks），每个文件对应一个Map任务

	// 无限循环
	for {
		// select语句等待多个channel的输入
		select { // select语句让Coordinator能响应不同类型的事件：心跳（heartbeatCh）和任务完成报告（reportCh）
		// 处理心跳信号
		case msg := <-c.heartbeatCh: // Coordinator收到来自Worker的心跳时，它会检查当前的阶段并分配任务，或告诉Worker任务已经完成
			// 检查状态和任务分配
			if c.phase == CompletePhase { // 表示所有任务都已完成
				msg.response.JobType = CompleteJob
			} else if c.selectTask(msg.response) { // 分配新的任务，返回值用来检验所有任务是否完成
				//  阶段转换
				switch c.phase { // 根据当前的阶段来决定接下来的行动
				case MapPhase: // Map任务已完成，将初始化Reduce阶段
					log.Printf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(msg.response)
				case ReducePhase: // Reduce任务已完成，将进入完成阶段
					log.Printf("Coordinator: %v finished, Congratulations \n", ReducePhase)
					c.initCompletePhase()
					msg.response.JobType = CompleteJob
				case CompletePhase:
					panic(fmt.Sprintf("Coordinator: enter unexpected branch"))
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			// 发送确认给工作节点
			msg.ok <- struct{}{} // 确认心跳已处理，任务分配或状态更新完成
		// 处理任务完成报告
		case msg := <-c.reportCh: // 从Worker接收到任务完成的报告
			if msg.request.Phase == c.phase { // 报告msg的阶段与当前阶段相同
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				c.tasks[msg.request.ID].status = Finished // 更新对应任务的状态
			}
			msg.ok <- struct{}{} // 确认报告已经被处理
		}
	}
}

// 返回allFinished：检查任务队列，根据其状态和执行情况动态分配任务给请求者，并更新其响应信息
func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewJob := true, false // 是否所有任务都完成   是否有新任务可以分配
	for id, task := range c.tasks {       // 遍历Coordinator中所有的任务
		switch task.status { // 根据任务的状态来执行不同的操作
		case Idle: // 意味着有任务还未被执行
			allFinished, hasNewJob = false, true // 所有任务未全部完成，且存在新的可分配任务
			c.tasks[id].status, c.tasks[id].startTime = Working, time.Now()
			// c.nReduce：存储了Reduce任务的总数量，在Coordinator初始化时就已设定好且在整个MapReduce任务过程中是不变的
			response.NReduce, response.ID = c.nReduce, id // 在response中设置NReduce（Reduce任务的总数）和任务的ID
			if c.phase == MapPhase {                      // 根据当前的phase设置任务类型（MapJob或ReduceJob）以及相应的文件路径或nMap（Map任务的数量）
				response.JobType, response.FilePath = MapJob, c.files[id]
			} else {
				// 将Map阶段完成的任务总数赋值给NMap字段，它将告诉Worker在Reduce任务中需要处理的中间文件数量
				response.JobType, response.NMap = ReduceJob, c.nMap // nMap是map任务的数量
			}
		case Working: // 有任务正在进行中
			allFinished = false                                      // 所有任务未全部完成
			if time.Now().Sub(task.startTime) > MaxTaskRunInterval { // 如果一个正在工作的任务超时了
				hasNewJob = true                   // 则重新分配这个任务
				c.tasks[id].startTime = time.Now() // 并更新开始时间
				response.NReduce, response.ID = c.nReduce, id
				if c.phase == MapPhase {
					response.JobType, response.FilePath = MapJob, c.files[id]
				} else {
					response.JobType, response.NMap = ReduceJob, c.nMap // nMap是map任务的数量
				}
			}
		case Finished:
			// 如果任务已经完成，那么什么也不做
		}
		if hasNewJob { // 如果已经找到一个新任务，就跳出循环
			break
		}
	}
	if !hasNewJob { // 如果没有新任务可以分配，则设置响应类型为WaitJob，让工作节点等待
		response.JobType = WaitJob
	}
	return allFinished
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
// 确定整个MapReduce作业是否完成
func (c *Coordinator) Done() bool {
	// Your code here.
	// doneCh是一个带有一个缓冲的通道，带有缓冲的通道是非阻塞的，直到其缓冲区填满。
	// 缓冲区大小为1，意味着Coordinator可以发送一个完成信号，而不用担心阻塞，如果已经有一个信号在通道中了，发送者将会阻塞
	<-c.doneCh
	// 一旦c.doneCh通道接收到信号，这个方法就会返回true，表示整个MapReduce作业已经完成
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),              // 每个文件对应一个map任务
		heartbeatCh: make(chan heartbeatMsg), // 接收来自Worker的心跳消息
		reportCh:    make(chan reportMsg),    // 接收Worker完成任务后的报告消息
		doneCh:      make(chan struct{}, 1),  // 在整个作业完成时发送信号，缓冲大小为1，允许发送一个信号而不阻塞
	}
	// 初始化服务器
	c.server()
	// 开始调度
	go c.schedule()
	return &c
}
