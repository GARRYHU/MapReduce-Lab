package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	TmpFilePath := "./"

	// Your worker implementation here.
	for {
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
		reply := CallTask()
		if reply.MsgType == MapTaskAlloc {
			err := HandleMapTask(mapf, reply, TmpFilePath)
			if err == nil { // map task success
				for CallReplyTask(MapSuccess, reply.TaskID, reply.TaskFileName) != nil {
				}
			} else { // map task failed
				for CallReplyTask(MapFailed, reply.TaskID, reply.TaskFileName) != nil {
				}
			}
		}
		if reply.MsgType == ReduceTaskAlloc {
			err := HandleReduceTask(reducef, reply, TmpFilePath)
			if err == nil { // reduce task success
				for CallReplyTask(ReduceSuccess, reply.TaskID, reply.TaskFileName) != nil {
				}
				// delete the intermediate files
				for DelPatternFile(reply.TaskID, TmpFilePath) != nil {
				}
			} else { // reduce task failed
				for CallReplyTask(ReduceFailed, reply.TaskID, reply.TaskFileName) != nil {
				}
			}
		}
		if reply.MsgType == Wait {
			time.Sleep(time.Millisecond * 200) // sleep 200ms
		}
		if reply.MsgType == Shutdown {
			os.Exit(0)
		}
	}
}

func CallReplyTask(msgtype MsgType, taskid int, filename string) error {
	args := MessageSend{MsgType: msgtype, TaskID: taskid, TaskFileName: filename}
	reply := MessageReply{}
	ok := call("Coordinator.ReplyTask", &args, &reply)
	if ok {
		// log.Printf("reply.TaskType %v\n", reply.MsgType)
		return nil
	} else {
		log.Printf("call failed!\n")
		return errors.New("CallReplyTask failed")
	}
}

func CallTask() MessageReply {
	args := MessageSend{MsgType: AskForTask}
	reply := MessageReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// log.Printf("reply.TaskType %v\n", reply.MsgType)
	} else {
		log.Printf("call failed!\n")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func HandleMapTask(mapf func(string, string) []KeyValue, reply MessageReply, TmpfilePath string) error {
	filename := reply.TaskFileName
	nReduce := reply.NReduce
	taskID := reply.TaskID
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", filename)
		return err
	}
	kva := mapf(filename, string(content))

	// map kva to reduce Tasks
	reduceKVA := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceTaskID := ihash(kv.Key) % nReduce
		reduceKVA[reduceTaskID] = append(reduceKVA[reduceTaskID], kv)
	}

	for reduceTaskID, kva := range reduceKVA {
		//
		// To avoid multiple workers from writing to the same file when executing the same map task:
		// first create a temporary file, then atomically rename the temporary file to the intermediate file.
		//

		// create a temporary file
		tmpFile, err := os.CreateTemp(TmpfilePath, "example")
		if err != nil {
			log.Fatal(err)
			return err
		}
		// marshal kva to json
		marshal, err := json.Marshal(kva)
		if err != nil {
			log.Fatal(err)
			return err
		}
		// write kva to the temporary file
		if _, err := tmpFile.Write(marshal); err != nil {
			log.Fatal(err)
			return err
		}

		if err := tmpFile.Close(); err != nil {
			log.Fatal(err)
			return err
		}

		intermediateFileName := fmt.Sprintf("%smr-%v-%v", TmpfilePath, taskID, reduceTaskID)
		// rename the temporary file to the intermediate file actomically
		err = os.Rename(tmpFile.Name(), intermediateFileName)
		if err != nil {
			log.Fatalf("cannot rename %v", intermediateFileName)
			return err
		}
	}
	return nil
}

func HandleReduceTask(reducef func(string, []string) string, reply MessageReply, TmpFilePath string) error {
	taskID := reply.TaskID
	intermediate := make([]KeyValue, 0)
	// find all files in the directory that match the pattern: mr-*-i and open them
	filelist, err := FindPatternFile(taskID, TmpFilePath)
	if err != nil || len(filelist) == 0 {
		log.Fatalf("cannot find any pattern file")
		return err
	}
	for _, file := range filelist {
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", file.Name())
			return err
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", file.Name())
			return err
		}
		var kvaTmp []KeyValue
		err = json.Unmarshal(content, &kvaTmp)
		if err != nil {
			log.Fatalf("cannot unmarshal %v", file.Name())
			return err
		}
		intermediate = append(intermediate, kvaTmp...)
	}
	// sort intermediate by key
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", taskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
		return err
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = ofile.Close()
	if err != nil {
		log.Fatalf("cannot close %v", oname)
		return err
	}
	return nil
}

// find all files in the directory that match the pattern: mr-*-i and return the file pointer list
func FindPatternFile(i int, path string) (fileList []*os.File, err error) {
	pattern := fmt.Sprintf(`^mr-\d+-%d$`, i)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// get all files in the directory
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// filter files: match the pattern: mr-*-i
	for _, fileEntry := range files {
		if fileEntry.IsDir() {
			continue // skip directories
		}
		fileName := fileEntry.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, fileEntry.Name())
			// open the file in read-only mode
			file, err := os.Open(filePath)
			if err != nil {
				for _, oFile := range fileList {
					if err := oFile.Close(); err != nil {
						log.Fatalf("cannot close %v", oFile.Name())
						return nil, err
					}
				}
				log.Fatalf("cannot open %v", filePath)
				return nil, err
			}
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}

// delete all files in the directory that match the pattern: mr-*-i
func DelPatternFile(i int, path string) error {
	pattern := fmt.Sprintf(`^mr-\d+-%d$`, i)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// get all files in the directory
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// filter files: match the pattern: mr-*-i
	for _, fileEntry := range files {
		if fileEntry.IsDir() {
			continue // skip directories
		}
		fileName := fileEntry.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, fileName)
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
