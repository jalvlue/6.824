package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []task
	reduceTasks []task
	nReduce     int
	workerState []int
}

type task struct {
	fileName string
	finished bool
	taskID   int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignMapTask(args interface{}, mReply *askToMapReply) error {
	for _, task := range c.mapTasks {
		if !task.finished {
			mReply.fileName = task.fileName
			mReply.nReduce = c.nReduce
			mReply.mapTaskID = task.taskID
			return nil
		}
	}
	mReply.fileName = ""
	return nil
}

func (c *Coordinator) AssignReduceTask(args interface{}, rReply *askToReduceReply) error {
	for _, task := range c.reduceTasks {
		if !task.finished {
			rReply.fileName = task.fileName
			rReply.reduceTaskID = task.taskID
			return nil
		}
	}
	rReply.fileName = ""
	return nil
}

func (c *Coordinator) FinishMapTask(mapTaskID int, reply *struct{}) error {
	c.mapTasks[mapTaskID].finished = true
	return nil
}

func (c *Coordinator) FinishReduceTask(reduceTaskID int, reply *struct{}) error {
	c.reduceTasks[reduceTaskID].finished = true
	return nil
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
	for _, task := range c.mapTasks {
		if !task.finished {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if !task.finished {
			return false
		}
	}

	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	for i, filename := range files {
		c.mapTasks = append(c.mapTasks, task{filename, false, i})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, task{"", false, i})
	}

	c.server()
	return &c
}
