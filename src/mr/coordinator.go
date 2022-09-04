package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	NReduce     int
	MapTasks    []Task
	ReduceTasks []Task
	Files       []string
	// we need to arrange intermediate files as KEY (reduce num), VALUE (file name slices) for better access
	IntermediateFiles     map[int][]string
	MapLock               sync.Mutex
	ReduceLock            sync.Mutex
	IntermediateFilesLock sync.Mutex
}

type Task struct {
	CreatedTime time.Time
	TaskState   string
}

// send task to idle workers
func (c *Coordinator) GetTask(args *MRArgs, reply *MRReply) error {
	var mapTaskLen = len(c.Files)
	var isMapDone = true
	fmt.Printf("start to assign task \n")
	c.MapLock.Lock()
	// try assign a map task
	// here we assume the task num is equal to index of the file we want to map in files slice
	for i := 0; i < mapTaskLen; i++ {
		// if the task is not picked or it's expired (10 second not in complete state)
		if c.MapTasks[i].TaskState == "" ||
			(c.MapTasks[i].TaskState != "Complete" &&
				c.MapTasks[i].CreatedTime.Add(time.Second*10).Before(time.Now())) {
			fmt.Printf("find available map task %v to assign \n", i)
			c.MapTasks[i].TaskState = "Start"
			c.MapTasks[i].CreatedTime = time.Now()
			reply.FileNames = []string{c.Files[i]}
			reply.TaskType = "Map"
			reply.NReduce = c.NReduce
			reply.TaskNumber = i
			c.MapLock.Unlock()
			return nil
		}
		if c.MapTasks[i].TaskState != "Complete" {
			isMapDone = false
		}
	}
	c.MapLock.Unlock()

	if !isMapDone {
		reply.FileNames = []string{}
		reply.TaskType = "NotReady"
		reply.NReduce = c.NReduce
		reply.TaskNumber = -1
		return nil
	}

	var isReduceDone = true

	c.ReduceLock.Lock()
	// map task is all in complete state, so try assign reduce task
	// here we assume the task num starts from 0 to nReduce
	for i := 0; i < c.NReduce; i++ {
		// if the task is not picked or it's expired (10 second not in complete state)
		if c.ReduceTasks[i].TaskState == "" ||
			(c.ReduceTasks[i].TaskState != "Complete" &&
				c.ReduceTasks[i].CreatedTime.Add(time.Second*10).Before(time.Now())) {
			fmt.Printf("find available reduce task %v to assign \n", i)
			fmt.Printf("find %v files to assign \n", len(c.IntermediateFiles[i]))
			c.ReduceTasks[i].TaskState = "Start"
			c.ReduceTasks[i].CreatedTime = time.Now()
			// get the intermediate file names to do reduce
			reply.FileNames = c.IntermediateFiles[i]
			reply.TaskType = "Reduce"
			reply.NReduce = c.NReduce
			reply.TaskNumber = i
			c.ReduceLock.Unlock()
			return nil
		}
		if c.ReduceTasks[i].TaskState != "Complete" {
			isReduceDone = false
		}
	}
	c.ReduceLock.Unlock()

	// check if reduce task is also done.
	if !isReduceDone {
		reply.FileNames = []string{}
		reply.TaskType = "NotReady"
		reply.NReduce = c.NReduce
		reply.TaskNumber = -1
	} else {
		reply.FileNames = []string{}
		reply.TaskType = "TaskComplete"
		reply.NReduce = c.NReduce
		reply.TaskNumber = -1
	}
	return nil
}

// mark the task is done and save the file locations of map jobs
func (c *Coordinator) PostTaskComplete(args *MRCompleteArgs, reply *MRCompleteReply) error {
	if args.TaskType == "Map" {
		c.MapLock.Lock()
		fmt.Printf("map task %v is complete \n", args.TaskNumber)
		if c.MapTasks[args.TaskNumber].TaskState == "Complete" {
			c.MapLock.Unlock()
			return nil
		} else {
			c.MapTasks[args.TaskNumber].TaskState = "Complete"
		}
		c.MapLock.Unlock()

		// parse the file name from map job and add the file in corresponding queue for reduce job
		c.IntermediateFilesLock.Lock()
		fmt.Printf("get %v intermediate files from map task %v \n", len(args.FileNames), args.TaskNumber)
		for _, file := range args.FileNames {
			reduceTaskNum, err := strconv.Atoi(file[strings.LastIndex(file, "-")+1:])
			if err != nil {
				log.Fatal("Cannot parse file name to integer reduce task number", err)
			}
			c.IntermediateFiles[reduceTaskNum] = append(c.IntermediateFiles[reduceTaskNum], file)
		}
		c.IntermediateFilesLock.Unlock()
	} else if args.TaskType == "Reduce" {
		c.ReduceLock.Lock()
		fmt.Printf("reduce task %v is complete \n", args.TaskNumber)
		if c.ReduceTasks[args.TaskNumber].TaskState == "Complete" {
			c.ReduceLock.Unlock()
			return nil
		} else {
			c.ReduceTasks[args.TaskNumber].TaskState = "Complete"
		}
		c.ReduceLock.Unlock()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	var isReduceDone = true

	c.ReduceLock.Lock()
	for i := 0; i < c.NReduce; i++ {
		if c.ReduceTasks[i].TaskState != "Complete" {
			isReduceDone = false
		}
	}
	c.ReduceLock.Unlock()

	return isReduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// need to store the expiration time of each task.
	// so in getTask, we can expire some task and reassign them.
	c := Coordinator{
		NReduce:           nReduce,
		MapTasks:          make([]Task, len(files)),
		ReduceTasks:       make([]Task, nReduce),
		Files:             files,
		IntermediateFiles: make(map[int][]string),
	}

	// Your code here.

	c.server()
	return &c
}
