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

const (
	AskForTask= iota
	MsgForInterFileLoc
	MapFinished
	ReduceFinished
)

type MyArgs struct {
	MessageType int  //const type
	MessageCnt string  //filename
}

type MyIntermediateFile struct{
	MessageType int
	MessageCnt string
	NReduceType int
}

type MyReply struct {
	Filename string
	MapNumAllocated int
	NReduce int
	ReduceNumAllocated int
	ReduceFileList []string
	TaskType string
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
