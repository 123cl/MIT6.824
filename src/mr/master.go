package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
import "time"

var maptasks chan string //chan for map task
var reducetasks chan int //chan for reduce task

const (
	UnAllocated = iota
	Allocated
	Finished
)

type Master struct {
	// Your definitions here.
	AllFilesName     map[string]int
	MapTaskNumCount int
	NReduce         int
	
	InterFIlename       [][]string        // store location of intermediate files
	MapFinished     bool
	ReduceTaskStatus map[int]int
	ReduceFinished bool
	mutex     *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master)MyCall(args *MyArgs, reply *MyReply) error {
	msgType := args.MessageType
	switch(msgType){
	case AskForTask:
		select {
		case filename := <- maptasks:
			reply.Filename = filename
			reply.TaskType = "map"
			reply.NReduce = m.NReduce
			reply.MapNumAllocated = m.MapTaskNumCount

			m.mutex.Lock()
			m.AllFilesName[filename] = Allocated
			m.MapTaskNumCount ++
			m.mutex.Unlock()
			go m.timer("map", filename)
			return nil

			case reduceNum := <- reducetasks:
				reply.NReduce = m.NReduce
				reply.ReduceNumAllocated = reduceNum
				reply.TaskType = "reduce"
				reply.ReduceFileList = m.InterFIlename[reduceNum]

				m.mutex.Lock()
				m.ReduceTaskStatus[reduceNum] = Allocated
				m.mutex.Unlock()
				go m.timer("reduce", strconv.Itoa(reduceNum))
				return nil
		}
	case MapFinished:
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.AllFilesName[args.MessageCnt] = Finished
	case ReduceFinished:
		index, _ := strconv.Atoi(args.MessageCnt)
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.ReduceTaskStatus[index] = Finished
	}
	return nil
}

func (m *Master) MyInnerFileCall(args *MyIntermediateFile, reply *MyReply) error {
	nReduceNum := args.NReduceType
	filename := args.MessageCnt
	m.InterFIlename[nReduceNum] = append(m.InterFIlename[nReduceNum], filename)
	return nil
}

func (m *Master) timer(taskType, identify string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select{
		case <- ticker.C:
			if taskType == "map" {
				m.mutex.Lock()
				m.AllFilesName[identify] = UnAllocated
				m.mutex.Unlock()
				maptasks <- identify
			}else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.mutex.Lock()
				m.ReduceTaskStatus[index] = UnAllocated
				m.mutex.Unlock()
				reducetasks <- index
			}
			return
			default:
				if taskType == "map" {
					m.mutex.Lock()
					if m.AllFilesName[identify] == Finished {
						m.mutex.Unlock()
						return
					} else {
						m.mutex.Unlock()
					}
				} else if taskType == "reduce" {
					index, _ := strconv.Atoi(identify)
					m.mutex.Lock()
					if m.ReduceTaskStatus[index] == Finished {
						m.mutex.Unlock()
						return
					} else {
						m.mutex.Unlock()
					}
				}
		}

	}
}

func (m *Master) generateTask() {
	for k, v := range m.AllFilesName {
		if v == UnAllocated {
			maptasks <- k
		}
	}

	ok := false
	for !ok {
		ok = checkAllMapTask(m)
	}
	m.MapFinished = true

	for k, v := range m.ReduceTaskStatus {
		if v == UnAllocated {
			reducetasks <- k
		}
	}
	ok = false
	for !ok {
		ok = checkAllReduceTask(m)
	}
	m.ReduceFinished = true
}

func checkAllMapTask(m *Master) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, v := range m.AllFilesName {
		if v != Finished {
			return false
		}
	}
	return true
}

func checkAllReduceTask(m *Master) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, v := range m.ReduceTaskStatus {
		if v != Finished {
			return false
		}
	}
	return true
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	maptasks = make(chan string, 5)
	reducetasks = make(chan int, 5)
	
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
		
	go m.generateTask()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//在map执行完之前，reduce任务是不会执行的


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.ReduceFinished
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.MapTaskNumCount = 0
	m.NReduce = nReduce
	m.MapFinished = false
	m.ReduceFinished = false
	m.ReduceTaskStatus = make(map[int]int)
	m.AllFilesName = make(map[string]int)
	m.InterFIlename = make([][]string, m.NReduce)
	m.mutex = new(sync.RWMutex)
	// Your code here.

	for _, v := range files {
		m.AllFilesName[v] = UnAllocated
	}
	for i := 0; i < nReduce; i ++ {
		m.ReduceTaskStatus[i] = UnAllocated
	}

	m.server()
	return &m
}
