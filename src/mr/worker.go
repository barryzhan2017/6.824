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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		var filenames, taskType, taskNum, nReduce = CallCoordinatorForTask()
		switch taskType {
		case "TaskComplete":
			fmt.Printf("task complete, so quit \n")
			return
		case "Map":
			fmt.Printf("start map task %v!\n", taskNum)
			Map(mapf, filenames, taskNum, nReduce, taskType)
		case "Reduce":
			fmt.Printf("start reduce task %v!\n", taskNum)
			Reduce(reducef, filenames, taskNum, taskType)
		case "NotReady":
			fmt.Printf("All the task is assigned, take some rest!\n")
			time.Sleep(time.Second * 3)
		default:
			fmt.Printf("unkonw task type %v!\n", taskNum)
			return
		}
	}
}

//
// use user-defined map function to generate key value pairs and store into file named mr-MapTaskNunber-ReduceTaskNumber
//
func Map(mapf func(string, string) []KeyValue, filenames []string, taskNum int, nReduce int, taskType string) {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []KeyValue{}
	for _, filename := range filenames {
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
		intermediate = append(intermediate, kva...)
	}
	var intermediateLocs = make(map[int][]KeyValue)
	for _, kv := range intermediate {
		intermediateLocs[ihash(kv.Key)%nReduce] = append(intermediateLocs[ihash(kv.Key)%nReduce], kv)
	}
	intermediateFilenames := []string{}
	for location, intermediate := range intermediateLocs {
		// create temp file
		tmpFile, err := ioutil.TempFile(os.TempDir(), "temp")
		if err != nil {
			log.Fatal("Cannot create temporary file", err)
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("Cannot write to temporary file", err)
			}
		}
		newFilename := "mr-" + strconv.Itoa(taskNum) + "-" + strconv.Itoa(location)
		e := os.Rename(tmpFile.Name(), newFilename)
		if e != nil {
			log.Fatal("Cannot rename temp file", e)
		}
		intermediateFilenames = append(intermediateFilenames, newFilename)
		tmpFile.Close()
	}
	CallCoordinatorForTaskCompletion(taskType, intermediateFilenames, taskNum)
}

//
// use user-defined reduce function to generate key vault results and store into file named mr-out-ReduceTaskNumber
//
func Reduce(reducef func(string, []string) string, filenames []string, taskNum int, taskType string) {
	// Read intermediate files from partitioned NxM buckets.
	intermediate := []KeyValue{}
	newFilename := "mr-out-" + strconv.Itoa(taskNum)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// create temp file
	tmpFile, err := ioutil.TempFile(os.TempDir(), "temp")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-TaskNum.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	e := os.Rename(tmpFile.Name(), newFilename)
	if e != nil {
		log.Fatal("Cannot rename temp file", e)
	}
	tmpFile.Close()
	CallCoordinatorForTaskCompletion(taskType, []string{newFilename}, taskNum)

}

//
// function to make an RPC call to the coordinator to get a task.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallCoordinatorForTask() ([]string, string, int, int) {

	// declare an argument structure.
	args := MRArgs{}

	// declare a reply structure.
	reply := MRReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.GetTask" tells the
	// receiving server that we'd like to call
	// the GetTask() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// there is some task ready to be worked on
		fmt.Printf("get task successfully!\n")
		return reply.FileNames, reply.TaskType, reply.TaskNumber, reply.NReduce
	} else {
		// server is closed, so no task availiable
		fmt.Printf("call failed!\n")
		return []string{}, "TaskComplete", -1, -1
	}
}

//
// function to make an RPC call to the coordinator to get a task.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallCoordinatorForTaskCompletion(taskType string, filenames []string, taskNum int) {
	args := MRCompleteArgs{taskType, filenames, taskNum}
	// declare a reply structure.
	reply := MRCompleteReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.PostTaskComplete" tells the
	// receiving server that we'd like to call
	// the PostTaskComplete() method of struct Coordinator.
	ok := call("Coordinator.PostTaskComplete", &args, &reply)
	if ok {
		// there is some task ready to be worked on
		fmt.Printf("%v task %v complete sent\n", taskType, taskNum)
	} else {
		// server is closed, so no need to pass back file locations and complete status
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
