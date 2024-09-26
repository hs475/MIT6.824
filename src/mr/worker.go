package mr

import "os"
import "fmt"
import "log"
import "time"
import "encoding/json"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	for state.Complete == false {
		request_task(&state)
		if (state.Complete == true) {
			break
		}
		fmt.Printf("workerid: %d\n", state.Workerid)
		fmt.Printf("Filename: %s\n", state.Filename)

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

		//写入文件
		for i := 0; i < state.NReduce; i++ {
			filename := fmt.Sprintf("mr-intermidiate/mr-%d-%d.json", state.Workerid, i)
			if _, err := os.Stat(filename); err == nil {
				file, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
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
					err = enc.Encode(&v)
				}
			}

			//file.Seek(0, 0)


			file.Close()
		}
		complete_task(&state)
		fmt.Printf("\n")

		time.Sleep(time.Second)
	}
}

func request_task(state *Reply) {
	args := Args{Workerid : state.Workerid}
	call("Coordinator.Distribute", &args, state)
}

func complete_task(state *Reply) {
	fmt.Printf("complete %s\n", state.Filename)
	args := Args{Filename: state.Filename}
	call("Coordinator.Complete_task", &args, state)
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
