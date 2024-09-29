package mr

import (
	"time"
	"log"
	"fmt"
	"net"
	"os"
	"sync"
	"net/rpc"
	"net/http"
)

var idt int

type Coordinator struct {
	// Your definitions here.
	Filename [] string
	Complete_map_flag bool
	Complete_map [] int
	Complete_reduce [] int
	Complete_reduce_flag bool
	Nreduce int
	mu sync.Mutex
	quit chan struct{}
	wg sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(args *Args, reply *Reply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	if c.Complete_map_flag && c.Complete_reduce_flag {
		reply.Complete = true
		return nil
	}
	reply.Worktype = "none"
	if (args.Workerid == -1) {
		reply.Workerid = idt
		idt++
	}
	if !c.Complete_map_flag {
		for i := 0; i < len(c.Filename); i++ {
			if c.Complete_map[i] == 0 {
				c.Complete_map[i] = 2
				reply.Filename = c.Filename[i]
				reply.Worktype = "map"
				break
			}
		}
		reply.Complete = false
		reply.NReduce = c.Nreduce
		return nil
	} else {
		if !c.Complete_reduce_flag {
			for i := 0; i < c.Nreduce; i++ {
				if c.Complete_reduce[i] == 0 {
					c.Complete_reduce[i] = 2
					reply.Worktype = "reduce"
					reply.Filename = fmt.Sprintf("/home/jcw/jcw/6.824/src/main/mr-tmp/mr-mid/mr-*-%d.json", i)
					reply.Reduce_num = i
					break
				}
			}
		}
	}

	return nil
}

func (c *Coordinator) Complete_map_task(args *Args, reply *Reply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	for i := 0; i < len(c.Filename); i++ {
		if (args.Filename == c.Filename[i] ) {
			c.Complete_map[i] = 1
		}
	}
	for i := 0; i < len(c.Filename); i++ {
		if c.Complete_map[i] != 1 {
			return nil
		}
	}
	c.Complete_map_flag = true
	return nil
}

func (c *Coordinator) Complete_reduce_task(args *Args, reply *Reply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	c.Complete_reduce[args.Reduce_num] = 1
	for i := 0; i < c.Nreduce; i++ {
		if c.Complete_reduce[i] != 1 {
			return nil
		}
	}
	time.Sleep(time.Second)
	c.Complete_reduce_flag = true
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
	c.mu.Lock()
    defer c.mu.Unlock()
	ret := true

	// Your code here.
	if c.Complete_map_flag && c.Complete_reduce_flag {
		ret = false
		os.RemoveAll("/home/jcw/jcw/6.824/src/main/mr-tmp/mr-mid")
		close(c.quit)
		c.wg.Wait()
	}
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
		Complete_map: make([]int, len(files)),
		Complete_reduce: make([]int, nReduce + 1),
		Nreduce: nReduce,
		Complete_map_flag: false,  
		Complete_reduce_flag: false,
		quit:  make(chan struct{}),
	}

	// Your code here.
	os.Mkdir("/home/jcw/jcw/6.824/src/main/mr-tmp/mr-mid", 0755)


	c.server()
	return &c
}
