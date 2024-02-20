package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	taskTracker *TaskTracker
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetFile(args *GetFileRequest, reply *GetFileReply) error {
	task, error := c.taskTracker.AssignAvailableTask()

	reply.NumReducer = c.nReduce
	if error == nil {
		reply.FileName = task.name
		reply.TaskId = task.id
		reply.IsAvailable = true
	} else {
		reply.IsAvailable = false
	}
	log.Printf("Reply from Server: %v\n", reply)
	return nil
}

func (c *Coordinator) UpdateTaskCompletion(request *TaskCompletionRequest, reply *TaskCompletionResponse) error {
	ok := c.taskTracker.UpdateMapTaskCompletion(request.Id)
	if !ok {
		return fmt.Errorf("no map task found with id: %v", request.Id)
	}
	reply.Ack = true
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
	mapPhaseCompleted := c.taskTracker.AllMapTasksCompleted()
	reducePhaseCompleted := c.taskTracker.AllReduceTasksCompleted()
	return mapPhaseCompleted && reducePhaseCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetFlags(log.Lshortfile)
	log.Printf("Received args: %v and nReduce: %v", files, nReduce)
	ts := NewTaskScheduler(files)
	c := Coordinator{taskTracker: ts, nReduce: nReduce}

	// Your code here.

	c.server()
	for !c.Done() {
		log.Printf("MapPhase Result: %v, Reduce : %v\n", c.taskTracker.AllMapTasksCompleted(), c.taskTracker.AllReduceTasksCompleted())
		time.Sleep(5 * time.Second)
	}
	return &c
}
