package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type taskStatus int

// Task 状态
const (
	IDLE taskStatus = iota
	RUNNING
	FINISHED
)

// Map Task states
type MapTaskInfo struct {
	TaskId    int
	Status    taskStatus
	StartTime int64
}

// Reduce Task states
type ReduceTaskInfo struct {
	Status    taskStatus
	StartTime int64
}

type Coordinator struct {
	// Your definitions here.
	NReduce     int
	MapDone     bool                   // whether all map tasks are done
	MapTasks    map[string]MapTaskInfo // filename -> MapTaskInfo
	ReduceTasks []ReduceTaskInfo       // # nReduce of ReduceTaskInfo
	mutex       sync.Mutex             // to protect MapTasks and ReduceTasks
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// handle ReplyTask RPC call from worker.go
func (c *Coordinator) ReplyTask(args *MessageSend, reply *MessageReply) error {
	// lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// update task status
	if args.MsgType == MapSuccess {
		mapTaskInfo := c.MapTasks[args.TaskFileName]
		mapTaskInfo.Status = FINISHED
		c.MapTasks[args.TaskFileName] = mapTaskInfo
		return nil
	} else if args.MsgType == MapFailed {
		mapTaskInfo := c.MapTasks[args.TaskFileName]
		mapTaskInfo.Status = IDLE
		c.MapTasks[args.TaskFileName] = mapTaskInfo
		return nil
	}

	if args.MsgType == ReduceSuccess {
		reduceTaskInfo := c.ReduceTasks[args.TaskID]
		reduceTaskInfo.Status = FINISHED
		c.ReduceTasks[args.TaskID] = reduceTaskInfo
		return nil
	} else if args.MsgType == ReduceFailed {
		reduceTaskInfo := c.ReduceTasks[args.TaskID]
		reduceTaskInfo.Status = IDLE
		c.ReduceTasks[args.TaskID] = reduceTaskInfo
		return nil
	}

	return errors.New("Wrong MsgType in ReplyTask.")

}

// handle GetTask RPC call from worker.go
func (c *Coordinator) GetTask(args *MessageSend, reply *MessageReply) error {
	// lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// assign map task before all map tasks are done
	if c.MapDone == false {
		alldone := true
		// first assign map task
		for filename, mapTaskInfo := range c.MapTasks {
			if mapTaskInfo.Status == FINISHED {
				continue
			}
			alldone = false
			if mapTaskInfo.Status == IDLE {
				alldone = false
				mapTaskInfo.Status = RUNNING
				mapTaskInfo.StartTime = time.Now().Unix()
				c.MapTasks[filename] = mapTaskInfo
				// construct reply
				reply.MsgType = MapTaskAlloc
				reply.TaskID = mapTaskInfo.TaskId
				reply.NReduce = c.NReduce
				reply.TaskFileName = filename
				return nil
			}
			if mapTaskInfo.Status == RUNNING {
				alldone = false
				if time.Now().Unix()-mapTaskInfo.StartTime > 10 {
					// timeout, reset the task
					mapTaskInfo.StartTime = time.Now().Unix()
					c.MapTasks[filename] = mapTaskInfo
					// construct reply
					reply.MsgType = MapTaskAlloc
					reply.TaskID = mapTaskInfo.TaskId
					reply.NReduce = c.NReduce
					reply.TaskFileName = filename
					return nil
				}
			}
		}
		if alldone { // all map tasks are done
			c.MapDone = true
			log.Println("Map tasks are all done.")
		} else {
			reply.MsgType = Wait
			return nil
		}
	}

	// assign reduce task after all map tasks are done
	if c.MapDone == false {
		err := errors.New("Map tasks are not done yet when assigning reduce task.")
		log.Fatal(err)
	}
	for i, reduceTaskInfo := range c.ReduceTasks {
		if reduceTaskInfo.Status == FINISHED {
			continue
		}
		if reduceTaskInfo.Status == IDLE {
			reduceTaskInfo.Status = RUNNING
			reduceTaskInfo.StartTime = time.Now().Unix()
			c.ReduceTasks[i] = reduceTaskInfo
			// construct reply
			reply.MsgType = ReduceTaskAlloc
			reply.TaskID = i
			return nil
		}
		if reduceTaskInfo.Status == RUNNING && time.Now().Unix()-reduceTaskInfo.StartTime > 10 {
			// timeout, reset the task
			reduceTaskInfo.StartTime = time.Now().Unix()
			c.ReduceTasks[i] = reduceTaskInfo
			// construct reply
			reply.MsgType = ReduceTaskAlloc
			reply.TaskID = i
			return nil
		}
	}
	reply.MsgType = Wait
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
	// Your code here.
	// check if all map tasks are done
	if c.MapDone == false {
		return false
	}

	// check if all reduce tasks are done
	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.Status != FINISHED {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.MapDone = false
	c.MapTasks = make(map[string]MapTaskInfo)
	c.ReduceTasks = make([]ReduceTaskInfo, nReduce)

	// initailize ReduceTasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = ReduceTaskInfo{Status: IDLE, StartTime: 0}
	}

	// initailize MapTasks
	for i, filename := range files {
		c.MapTasks[filename] = MapTaskInfo{TaskId: i, Status: IDLE, StartTime: 0}
	}

	c.server()
	return &c
}
