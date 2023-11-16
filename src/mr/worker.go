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

var (
	heartBeatChan = make(chan struct{})
	DoneChan      = make(chan struct{})
)

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// uncomment to send the Example RPC to the coordinator.
	CallExample()

	// Your worker implementation here.
	for {
		reply, ok := askToMap()
		if !ok {
			fmt.Println("cannot contact the coordinator")
			return
		}

		filename := reply.FileName
		nReduce := reply.NReduce
		mapTaskID := reply.TaskID

		// failed to ask for map task
		// try to ask for reduce task
		if filename == "" {
			var reduceTaskID int
			reduceTaskID, ok = askToReduce()
			if !ok {
				fmt.Println("cannot contact the coordinator")
				return
			}

			if reduceTaskID == -1 {
				fmt.Println("No reduce tasks to do")
				break
			} else {
				// do reduce task
				go doHeartBeat(reduceTaskID)
				doReduceTask(reduceTaskID, reducef)
				DoneChan <- struct{}{}
			}
		} else {
			// do map task
			go doHeartBeat(mapTaskID)
			doMapTask(filename, nReduce, mapTaskID, mapf)
			DoneChan <- struct{}{}
		}
	}

}

func doHeartBeat(taskID int) {
}

func askToMap() (Reply, bool) {
	args := Args{}
	args.TaskType = "map"
	reply := Reply{}

	//ok := call("Coordinator.Example", &args, &reply)
	ok := call("Coordinator.AssignMapTask", &args, &reply)
	if ok {
		fmt.Printf("reply.FileName %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply, ok
}

func noticeFinishMap(mapTaskID int) {
	reply := struct{}{}

	ok := call("Coordinator.FinishMapTask", mapTaskID, &reply)
	if ok {
		fmt.Printf("reply.FileName %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func askToReduce() (int, bool) {
	reply := Reply{}

	ok := call("Coordinator.AssignReduceTask", struct{}{}, &reply)
	if ok {
		fmt.Printf("reply.FileName %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.TaskID, ok
}

func noticeFinishReduce(reduceTaskID int) {
	reply := struct{}{}

	ok := call("Coordinator.FinishReduceTask", reduceTaskID, &reply)
	if ok {
		fmt.Printf("reply.FileName %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func doMapTask(filename string, nReduce int, mapTaskID int, mapf func(string, string) []KeyValue) {

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
	for _, kv := range kva {
		rNum := ihash(kv.Key) % nReduce
		intermediate[rNum] = append(intermediate[rNum], kv)
	}

	// write intermediate kvs to file
	for i, inter := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v.tmp", mapTaskID, i)
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

		newName := fmt.Sprintf("mr-inter-%v-%v", mapTaskID, i)
		err = os.Rename(tempFile.Name(), newName)
		if err != nil {
			log.Fatal(err)
		}
	}

	noticeFinishMap(mapTaskID)
}

func collectTempFilesWithSuffix(suffix string) ([]string, error) {
	pattern := "*" + suffix
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}
	return files, err
}

func doReduceTask(reduceTaskID int, reducef func(string, []string) string) {
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
	oname := fmt.Sprintf("mr-out-%v.tmp", reduceTaskID)
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

	newName := fmt.Sprintf("mr-out-%v", reduceTaskID)
	err = os.Rename(tempFile.Name(), newName)
	if err != nil {
		log.Fatal(err)
	}

	noticeFinishReduce(reduceTaskID)
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
