package mr

import "log"
//import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"

var idt int

type Coordinator struct {
	// Your definitions here.
	Filename [] string
	Complete_flag bool
	Complete [] int
	Nreduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(args *Args, reply *Reply) error {
	if (c.Complete_flag == true) {
		reply.Complete = true
		return nil
	}
	if (args.Workerid == -1) {
		reply.Workerid = idt
		idt++
	}
	for i := 0; i < len(c.Filename); i++ {
		if c.Complete[i] == 0 {
			reply.Filename = c.Filename[i]
			break
		}
	}
	reply.Complete = false
	reply.NReduce = c.Nreduce
	return nil
}

func (c *Coordinator) Complete_task(args *Args, reply *Reply) error {
	for i := 0; i < len(c.Filename); i++ {
		if (args.Filename == c.Filename[i] ) {
			c.Complete[i] = 1
		}
	}
	for i := 0; i < len(c.Filename); i++ {
		if c.Complete[i] == 0 {
			return nil
		}
	}
	c.Complete_flag = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := true

	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	idt = 1
	c := Coordinator{
		Filename: files,
		Complete: make([]int, len(files)),
		Nreduce: nReduce,
		Complete_flag: false,
	}

	// Your code here.


	c.server()
	return &c
}
