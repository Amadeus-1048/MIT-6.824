package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 轮训做任务
	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask()
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	// 读取输入文件
	fileName := response.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	// 处理读取的文件内容
	kva := mapF(fileName, string(content))
	// 分配键值对到中间存储
	intermediates := make([][]KeyValue, response.NReduce) // 根据 Reduce 任务的数量（response.NReduce），创建一个二维切片
	for _, kv := range kva {                              // 切片用来存储每个 Reduce 任务的中间键值对。
		index := ihash(kv.Key) % response.NReduce // 对每个键进行哈希和取模后确定这个键值对应该分配给哪一个 Reduce 任务
		intermediates[index] = append(intermediates[index], kv)
	}
	// 并发写入中间结果
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) { // 对每个 Reduce 任务的中间结果，启动一个并发的 Go 协程进行处理
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(response.ID, index) // response.ID 即 mapNumber， index 即 reduceNumber
			var buf bytes.Buffer                                                  // 暂存编码后的数据
			encode := json.NewEncoder(&buf)                                       // 建一个 JSON 编码器
			for _, kv := range intermediate {
				err = encode.Encode(&kv) // 遍历 intermediate 中的每个键值对，并使用 JSON 编码器将它们编码
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			err = atomicWriteFile(intermediateFilePath, &buf) // 将缓冲区中的数据写入到中间文件中
			if err != nil {
				log.Fatalf("cannot write file %v", intermediateFilePath)
			}
		}(index, intermediate)
	}
}

func doReduceTask() {

}

func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}
