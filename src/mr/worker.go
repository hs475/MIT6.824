package mr

import (
	//"time"
	"os"
	"fmt"
	"log"
//import "time"
	"sort"
	"path/filepath"
	"encoding/json"
	"io/ioutil"
	"net/rpc"
	"hash/fnv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	state := Reply{Workerid: -1}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for !state.Complete {
		err := request_task(&state)
		if (state.Complete || !err) {
			break
		}

		//map function
		if state.Worktype == "map" {
			tmp := []KeyValue{}
			file, err := os.Open(state.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", state.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", state.Filename)
			}
			file.Close()
			kva := mapf(state.Filename, string(content))
			tmp = append(tmp, kva...)
			kva = tmp
			//写入文件
			for i := 0; i < state.NReduce; i++ {
				filename := fmt.Sprintf("/home/jcw/jcw/6.824/src/main/mr-tmp/mr-mid/mr-%d-%d.json", state.Workerid, i)
				if _, err := os.Stat(filename); err == nil {
					file, _ = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				} else {
					file, err = os.Create(filename)
					if err != nil {
						fmt.Println("Error creating file:", err)
						return
					}
				}
				enc := json.NewEncoder(file)
				for _, v := range kva {
					tmp := ihash(v.Key) % state.NReduce
					if tmp == i {
						_ = enc.Encode(&v)
					}
				}
				defer file.Close()
				//file.Seek(0, 0)
			}
			complete_map_task(&state)
		//reduce function
		} else {
			if state.Worktype == "reduce" {
				files, err := filepath.Glob(state.Filename)
				if err != nil {
					log.Fatal(err)
				}
				var kva []KeyValue
				for _, file := range(files) {
					file, err := os.Open(file)
					if err != nil {
						log.Fatal(err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
						break
						}
						kva = append(kva, kv)
					}
					file.Close()
				}
				sort.Sort(ByKey(kva))
				intermediate := kva
				i := 0
				var ofile *os.File
				oname := fmt.Sprintf("mr-out-%d", state.Reduce_num)
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
					if _, err := os.Stat(oname); err == nil {
						ofile, _ = os.OpenFile(oname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
						fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
						ofile.Close()
					} else {
						ofile, _ = os.Create(oname)
						fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
						ofile.Close()
					}
					ofile.Close()
					i = j
				}
				ofile.Close()
				complete_reduce_task(&state)
			}
		}
	}
}

func request_task(state *Reply) bool{
	args := Args{Workerid : state.Workerid}
	err := call("Coordinator.Distribute", &args, state)
	return err
}

func complete_map_task(state *Reply) {
	args := Args{Filename: state.Filename}
	call("Coordinator.Complete_map_task", &args, state)
}

func complete_reduce_task(state *Reply) {
	args := Args{Reduce_num: state.Reduce_num}
	call("Coordinator.Complete_reduce_task", &args, state)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
