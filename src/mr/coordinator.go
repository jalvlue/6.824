package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks     []task
	reduceTasks  []task
	nReduce      int
	canReduce    bool
	mux          sync.Mutex
	onGoingTasks []int
}

// finished: 0: not finished, 1: on going, 2: finished
type task struct {
	fileName      string
	finished      byte
	taskID        int
	lastHeartBeat int
	assignAttmept int
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *struct{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	isDuplicated := true
	if !c.canReduce {
		// map phase
		if c.mapTasks[args.TaskID].assignAttmept == args.Attempt {
			c.mapTasks[args.TaskID].lastHeartBeat = 0
			isDuplicated = false
		}
	} else {
		// reduce phase
		if c.reduceTasks[args.TaskID].assignAttmept == args.Attempt {
			c.reduceTasks[args.TaskID].lastHeartBeat = 0
			isDuplicated = false
		}
	}
	if isDuplicated {
		fmt.Println("coordinator>: duplicated heartbeat")
		fmt.Printf("coordinator>: AssignAttmept: %v, Attempt: %v\n", c.mapTasks[args.TaskID].assignAttmept, args.Attempt)
	} else {
		fmt.Printf("coordinator>: heartbeat from task: %v, attempt: %v\n", args.TaskID, args.Attempt)
	}

	return nil
}

func (c *Coordinator) AssignMapTask(args *Args, reply *Reply) error {
	fmt.Println("coordinator>: try AssignMapTask call")
	isOnGoing := false
	c.mux.Lock()
	defer c.mux.Unlock()
	for i, task := range c.mapTasks {
		if task.finished == 0 {
			c.mapTasks[i].finished = 1
			c.mapTasks[i].assignAttmept++
			c.onGoingTasks = append(c.onGoingTasks, task.taskID)

			reply.FileName = task.fileName
			reply.NReduce = c.nReduce
			reply.TaskID = task.taskID
			reply.Attempt = c.mapTasks[i].assignAttmept
			fmt.Printf("coordinator>: assigned map task: %v, %s\n", task.taskID, task.fileName)
			fmt.Printf("coordinator>: AssignAttmept: %v, Attempt: %v\n", c.mapTasks[i].assignAttmept, task.assignAttmept)
			return nil
		} else if task.finished == 1 {
			isOnGoing = true
		}
	}
	if isOnGoing {
		reply.TaskID = -1
		fmt.Println("coordinator>: on going map tasks")
		return nil
	}
	reply.TaskID = -2
	fmt.Println("coordinator>: all map tasks finished, you can reduce attempt")
	return nil
}

func (c *Coordinator) AssignReduceTask(args *Args, rReply *Reply) error {
	fmt.Println("coordinator>: try AssignReduceTask call")
	isOnGoing := false
	c.mux.Lock()
	defer c.mux.Unlock()
	if !c.canReduce {
		rReply.FileName = ""
		fmt.Println("coordinator>: not ready to reduce")
		return nil
	}

	for i, task := range c.reduceTasks {
		if task.finished == 0 {
			c.reduceTasks[i].finished = 1
			c.reduceTasks[i].assignAttmept++
			c.onGoingTasks = append(c.onGoingTasks, task.taskID)

			rReply.FileName = task.fileName
			rReply.TaskID = task.taskID
			rReply.Attempt = c.reduceTasks[i].assignAttmept
			fmt.Printf("coordinator>: assigned reduce task: %v, %s\n", task.taskID, task.fileName)
			return nil
		} else if task.finished == 1 {
			isOnGoing = true
		}
	}
	if isOnGoing {
		rReply.TaskID = -1
		fmt.Println("coordinator>: on going reduce tasks")
		return nil
	}
	rReply.TaskID = -2
	fmt.Println("coordinator>: all reduce tasks finished")
	return nil
}

func removeElement(slice []int, elem int) []int {
	for i, e := range slice {
		if e == elem {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func (c *Coordinator) FinishMapTask(args *NoticeFinishTaskArgs, reply *NoticeFinishTaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	taskID := args.TaskID
	// TODO: check whether the timeout task have been reassigned to other worker
	if c.mapTasks[taskID].assignAttmept != args.Attempt {
		fmt.Println("coordinator>: duplicated attempt")
		reply.IsDuplicate = true
		reply.CanReduce = false
		return nil
	} else {
		c.onGoingTasks = removeElement(c.onGoingTasks, taskID)
	}
	c.mapTasks[taskID].finished = 2
	fmt.Println("coordinator>: finished map task:", taskID)

	reply.IsDuplicate = false
	for _, task := range c.mapTasks {
		if task.finished != 2 {
			reply.CanReduce = false
			return nil
		}
	}

	c.canReduce = true
	reply.CanReduce = true
	return nil
}

func (c *Coordinator) FinishReduceTask(args *NoticeFinishTaskArgs, reply *NoticeFinishTaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	reduceTaskID := args.TaskID
	if c.reduceTasks[reduceTaskID].assignAttmept != args.Attempt {
		fmt.Println("coordinator>: duplicated attempt")
		reply.IsDuplicate = true
		return nil
	} else {
		c.onGoingTasks = removeElement(c.onGoingTasks, reduceTaskID)
	}
	c.reduceTasks[reduceTaskID].finished = 2
	reply.IsDuplicate = false
	fmt.Println("coordinator>: finished reduce task:", reduceTaskID)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("rpc example")
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

	// Your code here.
	c.mux.Lock()
	defer c.mux.Unlock()

	// check heartbeat timeout
	fmt.Println("coordinator>: onGoingTasks:", c.onGoingTasks)
	timeoutTasks := make([]int, 0)
	if !c.canReduce {
		for _, taskID := range c.onGoingTasks {
			if c.mapTasks[taskID].lastHeartBeat > 10 {
				fmt.Println("coordinator>: map task:", taskID, "timeout")
				c.mapTasks[taskID].finished = 0
				c.mapTasks[taskID].lastHeartBeat = 0
				timeoutTasks = append(timeoutTasks, taskID)
			} else {
				c.mapTasks[taskID].lastHeartBeat++
				fmt.Println("coordinator>: map task:", taskID, "lastHeartBeat:", c.mapTasks[taskID].lastHeartBeat)
			}
		}
	} else {
		for _, taskID := range c.onGoingTasks {
			if c.reduceTasks[taskID].lastHeartBeat > 10 {
				fmt.Println("coordinator>: reduce task:", taskID, "timeout")
				c.reduceTasks[taskID].finished = 0
				c.reduceTasks[taskID].lastHeartBeat = 0
				timeoutTasks = append(timeoutTasks, taskID)
			} else {
				c.reduceTasks[taskID].lastHeartBeat++
				fmt.Println("coordinator>: reduce task:", taskID, "lastHeartBeat:", c.reduceTasks[taskID].lastHeartBeat)
			}
		}
	}
	for _, task := range timeoutTasks {
		c.onGoingTasks = removeElement(c.onGoingTasks, task)
	}

	for _, task := range c.mapTasks {
		if task.finished != 2 {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.finished != 2 {
			return false
		}
	}

	fmt.Println("coordinator>: all finished")
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.canReduce = false
	for i, filename := range files {
		c.mapTasks = append(c.mapTasks, task{
			fileName:      filename,
			finished:      0,
			taskID:        i,
			lastHeartBeat: 0,
			assignAttmept: 0,
		})
	}

	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-inter-%v", i)
		c.reduceTasks = append(c.reduceTasks, task{
			fileName:      filename,
			finished:      0,
			taskID:        i,
			lastHeartBeat: 0,
			assignAttmept: 0,
		})
	}

	c.server()
	return &c
}
