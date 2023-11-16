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
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *struct{}) error {
	fmt.Println("heartbeat from task:", args.TaskID)
	c.mux.Lock()
	defer c.mux.Unlock()

	if !c.canReduce {
		// map phase
		c.mapTasks[args.TaskID].lastHeartBeat = 0
	} else {
		// reduce phase
		c.reduceTasks[args.TaskID].lastHeartBeat = 0
	}

	return nil
}

func (c *Coordinator) AssignMapTask(args *Args, reply *Reply) error {
	fmt.Println("try AssignMapTask call")
	c.mux.Lock()
	defer c.mux.Unlock()
	for i, task := range c.mapTasks {
		if task.finished == 0 {
			reply.FileName = task.fileName
			reply.NReduce = c.nReduce
			reply.TaskType = args.TaskType
			reply.TaskID = task.taskID
			fmt.Printf("assigned map task: %v, %s\n", task.taskID, task.fileName)

			c.mapTasks[i].finished = 1
			c.onGoingTasks = append(c.onGoingTasks, task.taskID)
			return nil
		}
	}
	reply.FileName = ""
	fmt.Println("all map tasks finished")
	return nil
}

func (c *Coordinator) AssignReduceTask(args *Args, rReply *Reply) error {
	fmt.Println("try AssignReduceTask call")
	c.mux.Lock()
	defer c.mux.Unlock()
	if !c.canReduce {
		rReply.FileName = ""
		fmt.Println("not ready to reduce")
		return nil
	}

	for i, task := range c.reduceTasks {
		if task.finished == 0 {
			rReply.FileName = task.fileName
			rReply.TaskType = args.TaskType
			rReply.TaskID = task.taskID
			fmt.Printf("assigned reduce task: %v, %s\n", task.taskID, task.fileName)

			c.reduceTasks[i].finished = 1
			c.onGoingTasks = append(c.onGoingTasks, task.taskID)
			return nil
		}
	}
	rReply.TaskID = -1
	fmt.Println("all reduce tasks finished")
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

func (c *Coordinator) FinishMapTask(mapTaskID int, reply *NoticeFinishMapReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.mapTasks[mapTaskID].finished = 2
	c.onGoingTasks = removeElement(c.onGoingTasks, mapTaskID)
	fmt.Println("finished map task:", mapTaskID)

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

func (c *Coordinator) FinishReduceTask(reduceTaskID int, reply *struct{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.reduceTasks[reduceTaskID].finished = 2
	c.onGoingTasks = removeElement(c.onGoingTasks, reduceTaskID)
	fmt.Println("finished reduce task:", reduceTaskID)
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
	fmt.Println("onGoingTasks:", c.onGoingTasks)
	timeoutTasks := make([]int, 0)
	if !c.canReduce {
		for _, taskID := range c.onGoingTasks {
			if c.mapTasks[taskID].lastHeartBeat > 10 {
				fmt.Println("map task:", taskID, "timeout")
				c.mapTasks[taskID].finished = 0
				c.mapTasks[taskID].lastHeartBeat = 0
				timeoutTasks = append(timeoutTasks, taskID)
			} else {
				c.mapTasks[taskID].lastHeartBeat++
			}
		}
	} else {
		for _, taskID := range c.onGoingTasks {
			if c.reduceTasks[taskID].lastHeartBeat > 10 {
				fmt.Println("reduce task:", taskID, "timeout")
				c.reduceTasks[taskID].finished = 0
				c.reduceTasks[taskID].lastHeartBeat = 0
				timeoutTasks = append(timeoutTasks, taskID)
			} else {
				c.reduceTasks[taskID].lastHeartBeat++
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

	fmt.Println("all finished")
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
		})
	}

	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-inter-%v", i)
		c.reduceTasks = append(c.reduceTasks, task{
			fileName:      filename,
			finished:      0,
			taskID:        i,
			lastHeartBeat: 0,
		})
	}

	c.server()
	return &c
}
