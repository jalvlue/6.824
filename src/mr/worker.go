package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

var DoneChan = make(chan struct{})
var canReduce = false
var isDuplicated bool

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Your worker implementation here.
	// do map task
	for !canReduce {
		reply, ok := askToMap()
		if !ok {
			log.Println("cannot contact the coordinator")
			return
		}

		mapTaskID := reply.TaskID
		if mapTaskID == -1 {
			log.Println("no map task to do! wait for other map tasks to finish")
			time.Sleep(time.Second)
		} else if mapTaskID == -2 {
			log.Println("All tasks finished!")
			canReduce = true
			continue
		} else {
			nReduce := reply.NReduce
			filename := reply.FileName
			attempt := reply.Attempt

			go doHeartBeat(mapTaskID, attempt)
			doMapTask(filename, nReduce, mapTaskID, attempt, mapf)
			DoneChan <- struct{}{}
			finishMapArgs := NoticeFinishTaskArgs{
				TaskID:  mapTaskID,
				Attempt: attempt,
			}
			canReduce, isDuplicated = noticeFinishMap(finishMapArgs)
			if isDuplicated {
				log.Println("map task duplicated!")
				for i := 0; i < nReduce; i++ {
					interName := fmt.Sprintf("mr-inter-%v-%v-%v", mapTaskID, attempt, i)
					os.Remove(interName)
				}
			} else {
				log.Println("finish map task:", mapTaskID)
			}
		}
	}

	for {
		reply, ok := askToReduce()
		if !ok {
			log.Println("cannot contact the coordinator")
			return
		}

		reduceTaskID := reply.TaskID
		if reduceTaskID == -1 {
			log.Println("no reduce task to do! wait for other reduce tasks to finish")
			time.Sleep(time.Second)
		} else if reduceTaskID == -2 {
			log.Println("All tasks finished!")
			return
		} else {
			// do reduce task
			attempt := reply.Attempt

			go doHeartBeat(reduceTaskID, attempt)
			ofile := doReduceTask(reduceTaskID, attempt, reducef)
			DoneChan <- struct{}{}
			finishReduceArgs := NoticeFinishTaskArgs{
				TaskID:  reduceTaskID,
				Attempt: attempt,
			}
			isDuplicated := noticeFinishReduce(finishReduceArgs)
			if isDuplicated {
				os.Remove(ofile)
			}
		}
	}
}

func doHeartBeat(taskID int, attempt int) {
	args := HeartBeatArgs{}
	args.TaskID = taskID
	args.Attempt = attempt

	for {
		select {
		case <-DoneChan:
			return
		default:
			ok := call("Coordinator.HeartBeat", &args, &struct{}{})
			if ok {
				log.Printf("HeartBeat task: %v attempt: %v\n", taskID, attempt)
				time.Sleep(time.Second)
			} else {
				log.Printf("Can not contact coordinator, HeartBeat failed!\n")
			}
		}
	}
}

func askToMap() (Reply, bool) {
	args := Args{}
	args.TaskType = "map"
	reply := Reply{}

	//ok := call("Coordinator.Example", &args, &reply)
	ok := call("Coordinator.AssignMapTask", &args, &reply)
	if ok {
		log.Printf("reply.FileName %v\n", reply.FileName)
	} else {
		log.Printf("call failed!\n")
	}

	return reply, ok
}

func noticeFinishMap(args NoticeFinishTaskArgs) (bool, bool) {
	reply := NoticeFinishTaskReply{}

	ok := call("Coordinator.FinishMapTask", &args, &reply)
	if !ok {
		log.Printf("call failed!\n")
	}

	return reply.CanReduce, reply.IsDuplicate
}

func askToReduce() (Reply, bool) {
	Args := Args{}
	Args.TaskType = "reduce"
	reply := Reply{}

	ok := call("Coordinator.AssignReduceTask", &Args, &reply)
	if ok {
		log.Printf("reply.FileName %v\n", reply.FileName)
	} else {
		log.Printf("call failed!\n")
	}

	return reply, ok
}

func noticeFinishReduce(args NoticeFinishTaskArgs) bool {
	reply := NoticeFinishTaskReply{}

	ok := call("Coordinator.FinishReduceTask", &args, &reply)
	if ok {
		log.Printf("finished reduce task: %v\n", args.TaskID)
	} else {
		log.Printf("call failed!\n")
	}

	return reply.IsDuplicate
}

func doMapTask(filename string, nReduce, mapTaskID, attempt int, mapf func(string, string) []KeyValue) {

	// read kvs from file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate := [10][]KeyValue{}
	//intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		rNum := ihash(kv.Key) % nReduce
		intermediate[rNum] = append(intermediate[rNum], kv)
	}

	// write intermediate kvs to file
	for i, inter := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v-%v.tmp", mapTaskID, attempt, i)
		tempFile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		enc := json.NewEncoder(tempFile)
		for _, kv := range inter {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = tempFile.Close()
		if err != nil {
			log.Fatal(err)
		}

		newName := fmt.Sprintf("mr-inter-%v-%v-%v", mapTaskID, attempt, i)
		err = os.Rename(tempFile.Name(), newName)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func collectTempFilesWithSuffix(suffix string) ([]string, error) {
	pattern := "*" + suffix
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}
	return files, err
}

func doReduceTask(reduceTaskID, attempt int, reducef func(string, []string) string) string {
	tmpFiles, err := collectTempFilesWithSuffix(fmt.Sprint(reduceTaskID))
	if err != nil {
		log.Fatal(err)
	}
	kva := []KeyValue{}
	for _, filename := range tmpFiles {
		ofile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		ofile.Close()
	}

	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v-%v.tmp", reduceTaskID, attempt)
	tempFile, err := os.CreateTemp("", oname)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	err = tempFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	newName := fmt.Sprintf("mr-out-%v-%v", reduceTaskID, attempt)
	err = os.Rename(tempFile.Name(), newName)
	if err != nil {
		log.Fatal(err)
	}

	return newName
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
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
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
