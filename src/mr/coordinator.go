package mr

import (
	"time"
	"log"
	"fmt"
	"net"
	"os"
	"sync"
	"path/filepath"
	"net/rpc"
	"net/http"
)

type Coordinator struct {
	// Your definitions here.
	idt int
	Filename [] string
	Complete_map_flag bool
	Complete_map [] int
	Complete_reduce [] int
	Complete_reduce_flag bool
	map_start []time.Time
	map_id []int
	reduce_start []time.Time
	reduce_id [] int
	Nreduce int
	mu sync.Mutex
	quit chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(args *Args, reply *Reply) error {
	c.mu.Lock()
	if c.Complete_map_flag && c.Complete_reduce_flag {
		reply.Complete = true
		c.mu.Unlock()
		return nil
	}
	reply.Worktype = "none"
	if !c.Complete_map_flag {
		reply.Workerid = c.idt
		c.idt++
		for i := 1; i <= len(c.Filename); i++ {
			if c.Complete_map[i] == 0 {
				c.Complete_map[i] = 2
				c.map_start[i] = time.Now()
				c.map_id[i] = reply.Workerid
				reply.Filename = c.Filename[i - 1]
				reply.Worktype = "map"
				break
			}
		}
		reply.Complete = false
		reply.NReduce = c.Nreduce
		c.mu.Unlock()
		return nil
	} else {
		if c.Complete_map_flag && !c.Complete_reduce_flag {
			reply.Workerid = c.idt
			c.idt++
			if !c.Complete_reduce_flag {
				for i := 1; i <= c.Nreduce; i++ {
					if c.Complete_reduce[i] == 0 {
						c.Complete_reduce[i] = 2
						c.reduce_start[i] = time.Now()
						c.reduce_id[i] = reply.Workerid
						reply.Worktype = "reduce"
						reply.Filename = fmt.Sprintf("mr-*-%d.json", i)
						reply.Reduce_num = i
						break
					}
				}
			}
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Complete_map_task(args *Args, reply *Reply) error {

	c.mu.Lock()
	for i := 1; i <= len(c.Filename); i++ {
		if (args.Filename == c.Filename[i - 1] && c.map_id[i] == args.Workerid) {
			c.Complete_map[i] = 1
		}
	}
	for i := 1; i <= len(c.Filename); i++ {
		if c.Complete_map[i] != 1 {
			c.mu.Unlock()
			return nil
		}
	}
	c.Complete_map_flag = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Complete_reduce_task(args *Args, reply *Reply) error {
	c.mu.Lock()
	if args.Workerid == c.reduce_id[args.Reduce_num] {
		c.Complete_reduce[args.Reduce_num] = 1
		filename1 := fmt.Sprintf("mr-out-%d-%d", args.Reduce_num, args.Workerid)
		filename2 := fmt.Sprintf("mr-out-%d", args.Reduce_num)
		_ = os.Rename(filename1, filename2)
	}
	for i := 1; i <= c.Nreduce; i++ {
		if c.Complete_reduce[i] != 1 {
			c.mu.Unlock()
			return nil
		}
	}
	c.Complete_reduce_flag = true
	c.mu.Unlock()
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
	ret := true

	// Your code here.
	if c.Complete_map_flag && c.Complete_reduce_flag {
		ret = false
		//os.RemoveAll("/home/jcw/jcw/6.824/src/main/mr-tmp/mr-mid")
		close(c.quit)
		time.Sleep(time.Second * 11)
	}
	c.mu.Unlock()
	return ret
}

func (c *Coordinator) monitor() {
	interval := time.Second / 10
	timeout := time.Second * 10
	for {
		select {
		case <-c.quit:
			return
		case <-time.After(interval):
			c.mu.Lock()
			if !c.Complete_map_flag {
				for i, starttime := range c.map_start {
					if i == 0 {
						continue
					}
					if c.Complete_map[i] == 2 && time.Since(starttime) > timeout {
						filename := fmt.Sprintf("mr-%d-*.json", c.map_id[i])
						files, _ := filepath.Glob(filename)
						for _, file := range files {
							_ = os.Remove(file)
						}
						c.Complete_map[i] = 0
					}
				}
			}
			if c.Complete_map_flag && !c.Complete_reduce_flag {
				for i, starttime := range c.reduce_start {
					if i == 0 {
						continue
					}
					if c.Complete_reduce[i] == 2 && time.Since(starttime) > timeout {
						//fmt.Printf("resign %d\n", c.reduce_id[i])
						filename := fmt.Sprintf("mr-out-%d-%d", i, c.reduce_id[i])
						files, _ := filepath.Glob(filename)
						for _, file := range files {
							_ = os.Remove(file)
						}
						c.Complete_reduce[i] = 0
						c.reduce_id[i] = -1
					}
				}
			}
			c.mu.Unlock()
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		idt: 1,
		Filename: files,
		Complete_map: make([]int, len(files) + 1),
		Complete_reduce: make([]int, nReduce + 1),
		Nreduce: nReduce,
		Complete_map_flag: false,  
		Complete_reduce_flag: false,
		map_start: make([]time.Time, len(files) + 1),
		map_id: make([] int, len(files) + 1),
		reduce_start: make([]time.Time, nReduce + 1),
		reduce_id: make([]int, nReduce + 1),
		quit:  make(chan struct{}),
	}

	// Your code here.
	go c.monitor()

	c.server()
	return &c
}
