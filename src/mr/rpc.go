package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type MsgType int

const (
	AskForTask      MsgType = iota // `Worker` ask for task
	MapTaskAlloc                   // `Coordinator` allocate a map task to `Worker`
	ReduceTaskAlloc                // `Coordinator` allocate a reduce task to `Worker`
	MapSuccess                     // `Worker` report its map task success
	MapFailed                      // `Worker` report its map task failed
	ReduceSuccess                  // `Worker` report its reduce task success
	ReduceFailed                   //`Worker` report its reduce task failed
	Shutdown                       // `Coordinator` tell `Worker` to shutdown (all map and reduce tasks are done)
	Wait                           //`Coordinator` tell `Worker` to wait (no idle task AND no running task lasting more than 10s)
)

type MessageSend struct {
	MsgType      MsgType
	TaskID       int    // task id for reduce task
	TaskFileName string // filename for map task
}

type MessageReply struct {
	MsgType      MsgType
	NReduce      int
	TaskID       int
	TaskFileName string // filename for map task
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
