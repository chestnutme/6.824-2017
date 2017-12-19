package mapreduce

import (
	"fmt"
	"sync"
)

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

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskNum int, n_other int, phase jobPhase) {
			debug("DEBUG: current task info: taskNum %v, n_other %v, phase %v\n", taskNum, n_other, phase)
			for {
				worker := <-registerChan
				debug("DEBUG: current worker: %v\n", worker)
				var args *DoTaskArgs = &DoTaskArgs{jobName, mapFiles[taskNum],
					phase, taskNum, n_other}
				ok := call(worker, "Worker.DoTask", args, new(struct{}))
				if ok {
					/*//case 1:succeed, switch to reduce phase
					wg.Done()
					registerChan <- worker
					*/

					/*//case 2: fail, block at the end of map
					// [unbuffered channel, producer produced, no comsumer]
					// beacuse sync.WaitGroup makes schedule(mapPhase)'s finish to wait until all ntasks' goroutine
					// finish, registerChan is full then block due to the second last goroutine sending done worker,
					// and schedule(reducePhase) hasn't start, which cannot receive from registerChan.
					// fix: ch := make(string, x) //in master.go, x >= number of workers
					registerChan <- worker
					wg.Done()*/

					//case 3: succeed, send without block registerChan.[message passing seperated with computing]
					go func() { registerChan <- worker }()
					wg.Done()

					/*//case4: succeed
					wg.Done()
					go func() { registerChan <- worker }()*/

					break
				}
			}
		}(i, n_other, phase)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
