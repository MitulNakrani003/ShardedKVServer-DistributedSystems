package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}
		ok := call("Master.GetTask", &args, &reply)
		if !ok {
			return
		}

		switch reply.TaskType {
		case MapTask:
			performMapTask(mapf, reply.Filename, reply.MapID, reply.NReduce)
			reportTaskCompletion(MapTask, reply.MapID, true)
		case ReduceTask:
			performReduceTask(reducef, reply.ReduceID)
			reportTaskCompletion(ReduceTask, reply.ReduceID, true)
		case WaitTask:
			time.Sleep(1 * time.Second)
		case ExitTask:
			return
		}
	}
}

func performMapTask(mapf func(string, string) []KeyValue, filename string, mapID int, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	partitions := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		p := ihash(kv.Key) % nReduce
		partitions[p] = append(partitions[p], kv)
	}

	// Write partitions to temp files
	for i := 0; i < nReduce; i++ {
		tempFile, _ := ioutil.TempFile("", "mr-tmp-*")

		enc := json.NewEncoder(tempFile)
		for _, kv := range partitions[i] {
			enc.Encode(&kv)
		}
		tempFile.Close()
		outFile := fmt.Sprintf("mr-%d-%d", mapID, i)
		os.Rename(tempFile.Name(), outFile)
	}
}

func performReduceTask(reducef func(string, []string) string, reduceID int) {
	var kvarr []KeyValue

	files, _ := os.ReadDir(".")
	for _, file := range files {
		var mapID int
		var fileReduceID int
		n, err := fmt.Sscanf(file.Name(), "mr-%d-%d", &mapID, &fileReduceID)
		if err == nil && n == 2 && fileReduceID == reduceID {
			thisfile, _ := os.Open(file.Name())
			dec := json.NewDecoder(thisfile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvarr = append(kvarr, kv)
			}
			thisfile.Close()
		}
	}

	sort.Sort(ByKey(kvarr))

	// Use a temporary file for atomic write
	tempFile, err := os.CreateTemp("", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file for reduce output: %v", err)
	}

	i := 0
	for i < len(kvarr) {
		j := i + 1
		for j < len(kvarr) && kvarr[j].Key == kvarr[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvarr[k].Value)
		}
		output := reducef(kvarr[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kvarr[i].Key, output)
		i = j
	}

	tempFile.Close()
	outFile := fmt.Sprintf("mr-out-%d", reduceID)
	// Atomically rename the temporary file to the final output file
	os.Rename(tempFile.Name(), outFile)
}

// worker.go
func reportTaskCompletion(taskType TaskType, taskId int, success bool) {
	args := TaskReportArgs{
		TaskType: taskType,
		TaskID:   taskId,
		Success:  success,
	}
	reply := TaskReportReply{}
	call("Master.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
