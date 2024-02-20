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
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

const TASK_REQUEST_TIME = 1 * time.Second
const TASK_REQUEST_RETRY = 3

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.SetFlags(log.Lshortfile)
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	processId := os.Getuid()
	requestRetry := 0

	// Loop break condition is susceptible to transient network issue.
	// TODO: find a more robust condition.
	for reply, apiCallSuccess := getNextFile(); apiCallSuccess && reply.IsAvailable && requestRetry < TASK_REQUEST_RETRY; reply, apiCallSuccess = getNextFile() {
		filename := reply.FileName
		taskId := reply.TaskId
		nReducer := reply.NumReducer
		var mapStore map[int][]KeyValue = make(map[int][]KeyValue)
		fmt.Printf("Processing fileName: %v nReducer: %v, status: %v", filename, nReducer, apiCallSuccess)

		// This states that even though API replied 200 there can be empty result. This condition solves this issue
		if reply.IsAvailable {
			content := getFileContent(filename)
			kva := mapf(filename, content)

			// grouping data for each reducer
			for _, kv := range kva {
				idx := ihash(kv.Key) % nReducer
				oldValue, ok := mapStore[idx]
				if ok {
					mapStore[idx] = append(oldValue, kv)
				} else {
					mapStore[idx] = []KeyValue{kv}
				}
			}

			for reducerKey, value := range mapStore {
				outputFileName := fmt.Sprintf("mr-%v-%v-%v-tmp", processId, reducerKey, taskId)
				ofile, _ := os.Create(outputFileName)
				enc := json.NewEncoder(ofile)
				sort.Sort(ByKey(value))
				enc.Encode(value)
				defer ofile.Close()
			}
			requestRetry = 0
			taskCompletionResp, taskApiStatus := updateMapTaskStatus(taskId)
			if !taskApiStatus || !taskCompletionResp.Ack {
				log.Fatalf("could not update map task status reponse: %v\n", taskCompletionResp)
			}
		} else {
			requestRetry++
		}
		// wait for some time before requesting new tasks.
		time.Sleep(TASK_REQUEST_TIME)
	}
	// All the MAP tasks are completed and we can start the reducer workflow

}

func getFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	return string(content)
}

func getNextFile() (apiReply GetFileReply, apiRes bool) {
	args := GetFileRequest{}
	resp := GetFileReply{}
	callResult := call("Coordinator.GetFile", &args, &resp)
	log.Printf("Response for getNextFile from server: %v\n", resp)
	return resp, callResult
}

func updateMapTaskStatus(taskId int) (TaskCompletionResponse, bool) {
	args := TaskCompletionRequest{Id: taskId, Type: "map"}
	resp := TaskCompletionResponse{}
	log.Printf("Update TaskCompletion %v\n", args)
	callResult := call("Coordinator.UpdateTaskCompletion", &args, &resp)
	log.Printf("Response for updateMapTaskStatus from server: %v\n", resp)
	return resp, callResult
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
