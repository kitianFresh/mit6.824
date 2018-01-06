package mapreduce

import (
	"fmt"
	"sync"
)

const (
	MaxFailure = 4
)

type FailedWorkerCount struct {
	lock   sync.Mutex
	counts map[string]int
}

func (fwc *FailedWorkerCount) Get(worker string) int {
	fwc.lock.Lock()
	defer fwc.lock.Unlock()
	count, ok := fwc.counts[worker]
	if ok {
		return count
	} else {
		return 0
	}
}

func (fwc *FailedWorkerCount) Inc(worker string) {
	fwc.lock.Lock()
	defer fwc.lock.Unlock()
	fwc.counts[worker]++
}

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var latch sync.WaitGroup
	tasks := make([]*DoTaskArgs, 0)
	for i := 0; i < ntasks; i++ {
		args := new(DoTaskArgs)
		args.JobName = jobName
		args.File = mapFiles[i]
		args.TaskNumber = i
		args.Phase = phase
		args.NumOtherPhase = n_other
		tasks = append(tasks, args)
	}
	// if you use non-buffer channle or used buffer size little than registered workers, then this will block the launchTask goroutine send idle worker to finishedChan, will lead to deadlock
	finishedChan := make(chan string, ntasks)
	retryChan := make(chan *DoTaskArgs, ntasks)
	failedWorkerCounts := FailedWorkerCount{counts: make(map[string]int)}

	launchTask := func(worker string, args *DoTaskArgs) {
		defer latch.Done()
		ok := call(worker, "Worker.DoTask", args, new(struct{}))
		finishedChan <- worker
		if ok {
			fmt.Printf("Worker %s finished task %v\n", worker, args)
		} else {
			fmt.Printf("Call Worker: RPC %s DoTask error, Task %v will retry\n", worker, args)
			retryChan <- args
			failedWorkerCounts.Inc(worker)
		}
	}

	for len(tasks) > 0 {
		select {
		case taskArgs := <-retryChan:
			// like a first in first out queue for some fairness
			tasks = append([]*DoTaskArgs{taskArgs}, tasks...)
		case newWorker := <-registerChan:
			finishedChan <- newWorker
		case idleWorker := <-finishedChan:
			if failedWorkerCounts.Get(idleWorker) < MaxFailure {
				index := len(tasks) - 1
				taskArg := tasks[index]
				tasks = tasks[:index]
				latch.Add(1)
				fmt.Printf("Task %v\n", taskArg)
				go launchTask(idleWorker, taskArg)
			} else {
				fmt.Printf("Worker %s have failed %d times, it will not be used to schedule\n", idleWorker, MaxFailure)
			}
		}
	}

	fmt.Println("I am finished schedule")
	latch.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
