package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const interval = 20

var nReduce int
var nMapper int

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	getReduceCount()
	for {
		task := requestTask()
		exit := false
		switch task.TaskType {
		case 0:
			//log.Printf("Started one mapper")
			runMap(task.TaskFile, mapf, task.TaskId)
			exit = reportTaskDone(task.TaskId, task.TaskType)
		case 1:
			//log.Printf("Started one reducer")
			runReduce(reducef, task.TaskId)
			exit = reportTaskDone(task.TaskId, task.TaskType)
		default:
			time.Sleep(time.Millisecond * interval)
			exit = reportTaskDone(task.TaskId, task.TaskType)
		}
		if exit {
			return
		}
	}
}

func runReduce(reducef func(string, []string) string, taskId int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", tempDir, "*", taskId))
	if err != nil {
		log.Fatalf("Cannot list reduce files")
	}

	kvMap := make(map[string][]string)
	var kv KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open file %v\n", filename)
		}
		decoder := json.NewDecoder(file)
		for decoder.More() {
			err = decoder.Decode(&kv)
			if err != nil {
				log.Fatalf("Decode error")
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	writeReducerResult(reducef, taskId, kvMap)
}

func writeReducerResult(reducef func(string, []string) string, taskId int, kvMap map[string][]string) {
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	filename := fmt.Sprintf("%v/mr-out-%v-%v", tempDir, taskId, os.Getpid())
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("Failed to create file %v\n", filename)
	}
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, v)
		if err != nil {
			log.Fatalf("Cannot write result %v %v to file %v", k, v, filename)
		}
	}
	newPath := fmt.Sprintf("mr-out-%v", taskId)
	err = os.Rename(filename, newPath)
	if err != nil {
		log.Fatalf("Cannot rename the file from %v to %v", filename, newPath)
	}
}

func runMap(filename string, mapf func(string, string) []KeyValue, taskId int) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("Cannot open file %v\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read file %v\n", filename)
	}
	intermediate := mapf(filename, string(content))
	writeMapperResult(taskId, intermediate)
}

func writeMapperResult(taskId int, content []KeyValue) {
	prefix := fmt.Sprintf("%v/mr-%v", tempDir, taskId)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Cannot create file %v\n", filename)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	for _, kv := range content {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode %v to file", kv)
		}
	}
	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			log.Fatalf("Cannot flush buffer for file %v", files[i].Name())
		}
	}

	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("Failed to rename file from %v to %v", file.Name(), newPath)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func requestTask() *TaskRequestReply {
	args := TaskRequestArgs{}
	reply := TaskRequestReply{}
	ok := call("Coordinator.RequestForTask", &args, &reply)
	if !ok {
		log.Fatal("Cannot get the task from the coordinator")
	}
	return &reply
}

func reportTaskDone(taskId int, taskType TaskType) bool {
	args := TaskFinishReportArgs{
		TaskId:   taskId,
		TaskType: taskType,
	}
	reply := TaskFinishReportReply{}
	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	if !ok {
		log.Fatal("Failed to report the task finish")
	}
	return reply.Finish
}

func getReduceCount() {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	ok := call("Coordinator.GetReduceCount", &args, &reply)
	if !ok {
		log.Fatal("Cannot get the reducer count")
	}
	nReduce = reply.Count
}
