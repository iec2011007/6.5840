package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetFileRequest struct{}
type GetFileReply struct {
	TaskId      int
	FileName    string
	IsAvailable bool
	NumReducer  int
}

type TaskCompletionRequest struct {
	Id   int
	Type ProcessType
}
type TaskCompletionResponse struct {
	Ack bool
}

type GetReduceTaskRequest struct{}
type GetReduceTaskResponse struct {
	ReducerId   int
	IsAvailable bool
}

func (reply GetFileReply) String() string {
	return fmt.Sprintf("{id: %v, fileName: %v, numReducer: %v, isAvailable: %v}", reply.TaskId, reply.FileName, reply.NumReducer, reply.IsAvailable)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
