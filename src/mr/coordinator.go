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
	fileName string
	finished byte
	taskID   int
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
			fmt.Println("assigned task:", task.fileName)

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
			fmt.Println("assigned task:", task.fileName)

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

func (c *Coordinator) FinishMapTask(mapTaskID int, reply *struct{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.mapTasks[mapTaskID].finished = 2
	removeElement(c.onGoingTasks, mapTaskID)

	for _, task := range c.mapTasks {
		if task.finished != 2 {
			return nil
		}
	}

	c.canReduce = true
	return nil
}

func (c *Coordinator) FinishReduceTask(reduceTaskID int, reply *struct{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.reduceTasks[reduceTaskID].finished = 2
	removeElement(c.onGoingTasks, reduceTaskID)
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
	for i, filename := range files {
		c.mapTasks = append(c.mapTasks, task{filename, 0, i})
	}
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-inter-%v", i)
		c.reduceTasks = append(c.reduceTasks, task{filename, 0, i})
	}

	c.server()
	return &c
}
