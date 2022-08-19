package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const tempDir = "tmp"
const timeout = 10

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	IDLE
)

type TaskStatus int

const (
	AVAILABLE TaskStatus = iota
	PENDING
	FINISH
)

type Task struct {
	File   string     // input filename
	Type   TaskType   // 0 for mapping 1 for reduce 2 for finish
	Status TaskStatus // 0 haven't started 1 pending 2 finished
	Index  int
}

type Coordinator struct {
	// Your definitions here.
	lock       sync.Mutex
	mapTask    []Task
	reduceTask []Task
	nMap       int
	nReduce    int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nMap == 0 && c.nReduce == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		mapTask:    make([]Task, 0, len(files)),
		reduceTask: make([]Task, 0, nReduce),
	}
	for i, file := range files {
		newTask := Task{file, MAP, AVAILABLE, i}
		c.mapTask = append(c.mapTask, newTask)
	}
	for i := 0; i < nReduce; i++ {
		newTask := Task{"", REDUCE, AVAILABLE, i}
		c.reduceTask = append(c.reduceTask, newTask)
	}
	c.server()

	if err := os.RemoveAll(tempDir); err != nil {
		log.Fatalf("Cannot remove the temporary folder %v\n", tempDir)
	}
	if err := os.Mkdir(tempDir, 0755); err != nil {
		log.Fatalf("Cannot create the temporary folder %v\n", tempDir)
	}
	return &c
}

func (c *Coordinator) RequestForTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.lock.Lock()
	var task *Task
	if c.nMap > 0 {
		task = selectTask(c.mapTask)
	} else if c.nReduce > 0 {
		//if task == nil && c.nReduce > 0 {
		task = selectTask(c.reduceTask)
	}
	if task == nil {
		task = &Task{"", IDLE, FINISH, -1}
	}
	reply.TaskId = task.Index
	reply.TaskType = task.Type
	reply.TaskFile = task.File
	c.lock.Unlock()
	go c.waitTaskFinish(task)
	return nil
}

func selectTask(tasks []Task) *Task {
	var task *Task
	for i := 0; i < len(tasks); i++ {
		if tasks[i].Status == 0 {
			task = &tasks[i]
			task.Status = 1
			return task
		}
	}
	return nil
}

func (c *Coordinator) waitTaskFinish(task *Task) {
	if task == nil || task.Type > 1 {
		return
	}
	<-time.After(time.Second * timeout)
	c.lock.Lock()
	if task.Status == 1 {
		task.Status = 0
	}
	c.lock.Unlock()
}

func (c *Coordinator) ReportTaskDone(args *TaskFinishReportArgs, reply *TaskFinishReportReply) error {
	c.lock.Lock()
	var task *Task
	switch args.TaskType {
	case MAP:
		task = &c.mapTask[args.TaskId]
	case REDUCE:
		task = &c.reduceTask[args.TaskId]
	default:
		task = &Task{"", IDLE, FINISH, -1}
	}
	if task.Status == PENDING {
		task.Status = FINISH
		if args.TaskType == MAP && c.nMap > 0 {
			c.nMap--
		} else if args.TaskType == REDUCE && c.nReduce > 0 {
			c.nReduce--
		}
	}
	reply.Finish = c.nMap == 0 && c.nReduce == 0
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.Count = len(c.reduceTask)
	return nil
}
