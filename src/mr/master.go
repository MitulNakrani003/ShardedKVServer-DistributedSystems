package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Status    TaskStatus
	StartTime time.Time
}

type Master struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	files       []string
	nReduce     int
}

func (m *Master) GetTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//Assign map tasks first
	for i, task := range m.mapTasks {
		if task.Status == Idle {

			m.mapTasks[i].Status = InProgress
			m.mapTasks[i].StartTime = time.Now()

			reply.TaskType = MapTask
			reply.MapID = i
			reply.Filename = m.files[i]
			reply.NReduce = m.nReduce

			return nil
		}
	}

	allMapsDone := true
	for _, task := range m.mapTasks {
		if task.Status != Completed {
			allMapsDone = false
			break
		}
	}
	if !allMapsDone {
		reply.TaskType = WaitTask
		return nil
	}

	for i, task := range m.reduceTasks {
		if task.Status == Idle {
			m.reduceTasks[i].Status = InProgress
			m.reduceTasks[i].StartTime = time.Now()

			reply.TaskType = ReduceTask
			reply.ReduceID = i

			return nil
		}
	}

	reply.TaskType = ExitTask
	return nil
}

func (m *Master) ReportTask(args *TaskReportArgs, reply *TaskReportReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		// Only mark as completed if it's the original worker reporting.
		// A late report from a timed-out worker should be ignored.
		if m.mapTasks[args.TaskID].Status == InProgress {
			if args.Success {
				m.mapTasks[args.TaskID].Status = Completed
			} else {
				// Make task available again immediately on failure
				m.mapTasks[args.TaskID].Status = Idle
			}
		}
	case ReduceTask:
		if m.reduceTasks[args.TaskID].Status == InProgress {
			if args.Success {
				m.reduceTasks[args.TaskID].Status = Completed
			} else {
				// Make task available again immediately on failure
				m.reduceTasks[args.TaskID].Status = Idle
			}
		}
	}
	return nil
}

func (m *Master) checkTimeouts() {
	for !m.Done() {
		time.Sleep(500 * time.Millisecond)
		m.mu.Lock()

		// Check map timeouts
		for i, task := range m.mapTasks {
			if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
				m.mapTasks[i].Status = Idle
			}
		}

		// Check reduce timeouts
		for i, task := range m.reduceTasks {
			if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
				m.reduceTasks[i].Status = Idle
			}
		}

		m.mu.Unlock()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, task := range m.mapTasks {
		if task.Status != Completed {
			return false
		}
	}

	for _, task := range m.reduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}
	for i := range m.mapTasks {
		m.mapTasks[i].Status = Idle
	}
	for i := range m.reduceTasks {
		m.reduceTasks[i].Status = Idle
	}

	m.server()
	go m.checkTimeouts()
	return &m
}
