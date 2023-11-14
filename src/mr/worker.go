package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
			filename, reduceTaskID, ok = askToReduce()
			if !ok {
				fmt.Println("cannot contact the coordinator")
				return
			}

			if filename == "" {
				fmt.Println("No reduce tasks to do")
				break
			} else {
				// do reduce task
				doReduceTask(filename, reduceTaskID, reducef)
			}
		} else {
			// do map task
			doMapTask(filename, nReduce, mapTaskID, mapf)
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

func askToReduce() (string, int, bool) {
	reply := Reply{}

	ok := call("Coordinator.AssignReduceTask", struct{}{}, &reply)
	if ok {
		fmt.Printf("reply.FileName %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.FileName, reply.TaskID, ok
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
		oname := fmt.Sprintf("mr-inter-%v", i)
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range inter {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		ofile.Close()
	}

	noticeFinishMap(mapTaskID)
}

func doReduceTask(filename string, reduceTaskID int, reducef func(string, []string) string) {
	ofile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(ofile)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v", reduceTaskID)
	ofile, _ = os.Create(oname)

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

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
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
