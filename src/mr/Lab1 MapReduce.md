# Lab 1 MapReduce

The official website of this lab: [6.824 Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

First of all, we can get the source code of this project using:

```bash
git clone git://g.csail.mit.edu/6.824-golabs-2022 6.824
```

Inside this repository, there's already an implementation of the sequential version of MapReduce. We can compile it using:

```bash
cd 6.824/src/main/
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrsequential.go wc.so pg*.txt
more mr-out-0
```

The working path of the project is under the directory `src/main`, we can compile `wc.go` to the dynamic linked plugin first, and load it when running it. The `-race` parameter is used to detect the race conditions. When testing on the local environment we can add this parameter, but from the requirement it says, the actual test will run without the `-race` parameter and check it there's any race conditions.

When the above command finished running, we can see that every line of the output file is representing the frequency the word shown in the document.

## Task

1. Design an distributed version of MapReduce

    - It will contain two different programs an Coordinator and a Worker

2. Worker process keep requesting tasks from the Coordinator process using RPC, when the task finished, it reports to Coordinator about the task finished

3. Each Mapper takes an input file and split it into multiple files, which is considered as one task

4. The test script will require to run Map and Reduce tasks in parallel, and require to deal with recovering from worker crash

    - Means we need to monitor the status of the worker and reassign task if the task does not finish in time (10 seconds)

5. When the Coordinator finished all of the task, the Coordinator exit if there's any worker keeping request from the Coordinator, the workers will fail the RPC call and exit directly

Although the task here is to design an distributed version of the MapReduce, but to simplify the task, all the programs are running on the same instance. If we want to running the instances on different instances, we just have to change the RPC design and change the file system to a shared or distributed file system.

We can only change the files under the directory of `mr`, in order to manually test, we can run the command below:

```bash
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
```

Inside a new terminal, we can start a worker.

```bash
go run -race mrworker.go wc.so
```

Finally, we can check the result using:

```bash
cat mr-out-* | sort | more
```

We can also use the test script provided to run automatic check.

```bash
bash test-mr.sh
```

## Design

In this design, two different task lists are used to track the map tasks and the reduce tasks. The map task is used to assign each input file to the mapper and the reduce task is used to gather the output files of the mapper and combine the result into a single file. Actually, we can use queue instead of list, whenever a task is assigned, the task can be put at the end of the list or another list to get tracked latter.

Whenever the worker request for new task, the coordinator will loop through the list to check if there's any tasks available for mapper first. If yes, then assign the task to the mapper and change the task status to pending. If all the map task have finished, then we can start assigning reduce tasks. If no task can be assigned, but the MapReduce have not finish, then we can assign an idle task to keep the workers waiting. Then we can use a go thread to track the status of the task. If the task timed out, then we assign the task to the new worker requesting for new tasks.

Two different variables `nMap` and `nReduce` are used to track if the task have finished. When ever the corresponding map task finished `nMap` will deduct by 1, same for `nReduce`. Whenever both `nMap` and `nReduce` become 0, this means the whole MapReduce task has finished, and both coordinator and worker can exit.

## Implementation

### Coordinator

First we define there are 3 different status of tasks and there are 3 different kinds of tasks.

```go
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
```

The Task is designed as below, the File property is mainly used for the mapper to get the corresponding input file. While the index is the task id for each worker and mapper, this index will be used to address the intermediate file.

```go
type Task struct {
    File     string // input filename
    Type     TaskType    // 0 for mapping 1 for reduce 2 for finish
    Status   TaskStatus    // 0 haven't started 1 pending 2 finished
    Index    int
}
```

The coordinator will contain two different task list, two index tracker and a lock to prevent race conditions.

```go
type Coordinator struct {
    // Your definitions here.
    lock       sync.Mutex
    mapTask    []Task
    reduceTask []Task
    nMap       int
    nReduce    int
}
```

When initialize the Coordinator, all the tasks can be created since we know how many input files (how many map task) are there and how many output files (how many reduce task) will be. Also we can also create the intermediate directory `tmp` before the whole task starts.

```go
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
```

### Worker

For the worker, we need a while loop to keep checking if there's any new tasks available. After getting the task, it finish the task first and report the task done. Then we can run a query to check if the MapReduce task has finished. But, we can also put this query into the report task done. If all task finish, then the worker will exit.

Since the worker needs the number of reducers in order for the mapper to produce the corresponding number of files, we can get this information right after the worker start running.

```go
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
```

### Get reducer count

So we can implement the functions required above in the worker. The first one is to get the number of reducers.

```go
type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
    Count int
}

func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
    c.lock.Lock()
    defer c.lock.Unlock()
    reply.Count = len(c.reduceTask)
    return nil
}
```

Then in the worker side, we need a function to call this RPC. First we need to define a global variable to store the number of reducers.

```bash
var nReduce int
func getReduceCount() {
    args := GetReduceCountArgs{}
    reply := GetReduceCountReply{}
    ok := call("Coordinator.GetReduceCount", &args, &reply)
    if !ok {
        log.Fatal("Cannot get the reducer count")
    }
    nReduce = reply.Count
}
```

### Get task

In order for the worker to get the tasks, we need to create an RPC call to get the tasks. When all of the map task haven't finished, we only assign map task and idle task to wait until the intermediate files available. Only after the map task finish, we start to assign the reduce task.

We loop through each task check if it is available, if it is available, then change the status to pending and assign to the worker.

After assigning the task, we create another thread to keep track if the task finish in time. If the task did not finish, the task status will change back to available. In order it can be assigned to another worker.

```bash
type TaskRequestArgs struct {}

type TaskRequestReply struct {
    TaskType TaskType
    TaskId   int
    TaskFile string
}

func (c *Coordinator) RequestForTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
    c.lock.Lock()
    var task *Task
    if c.nMap > 0 {
        task = selectTask(c.mapTask)
    } else if c.nReduce > 0 {
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
```

On the worker side, the worker can use the code below to get the tasks.

```go
func requestTask() *TaskRequestReply {
	args := TaskRequestArgs{}
	reply := TaskRequestReply{}
	ok := call("Coordinator.RequestForTask", &args, &reply)
	if !ok {
		log.Fatal("Cannot get the task from the coordinator")
	}
	return &reply
}
```

### Report task done

After finish the task we have to report the task finish status and also get the MapReduce status. These two requests can be combined into one request. We can retrieve the task simply use the task id.

The task will then be marked as finish, the map/reduce task count will deduct by one. Then it will return if there's any tasks still not finish.

```go
type TaskFinishReportArgs struct {
    TaskId   int
    TaskType TaskType
    Finished int
}

type TaskFinishReportReply struct {
    Finish bool
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
```

The workers can report the task finish status by sending the task id and task type

```go
func reportTaskDone(taskId int, taskType TaskType) bool {
    args := TaskFinishReportArgs{
        TaskId:   taskId,
        TaskType: taskType,
        WorkerId: os.Getpid(),
    }
    reply := TaskFinishReportReply{}
    ok := call("Coordinator.ReportTaskDone", &args, &reply)
    if !ok {
        log.Fatal("Failed to report the task finish")
    }
    return reply.Finish
}
```

### Run the map function

Finally, we reached  the step to actually run the map function. The whole process can be split into:

1. read the input file

2. get intermediate

3. group and write intermediate into corresponding files

```go
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
```

As suggested by the guide, we can write the intermediate using json encoder. We use the `ihash` function provided to group the keys, the same key will be passed to the same reducer. To prevent race condition of two different workers writing the same file, the content is written to the temporary file ending with the process id first. After the writing finish, the file is renamed in the format `tmp/mr-mapperId-reducerId`.

```go
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
```

### Run the reduce function

Since the reducers starts running after the mappers finished running, so all of the files should be available. We list all the file in the format of `tmp/mr-*-reducerId` . Which is all the files that is supposed to pass to the same reducer. The keys should be in the same group.

We then gather all the values with the same key into a list. The result turn into the form of `key: [value 1, value 2, ... , value n]`. Then this key and array of values will be passed to the reducer and write to the output file.

```go
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
```

The keys are sorted first, this step is not required, since the test script will also do this, but this might be useful to debug. After running the reducer, the values are combined, and we can get a new key value pair. Similar to write the mapper result, the value pairs will be write to the temporary file first and renamed to the final result afterwards.

```go
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
```

Now we have finished all the required part of this lab. By running the test script, wait for a while and all the test will be passed.

```bash
bash test-mr.sh 
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```
