package mapreduce

import (
	"fmt"
	"math/rand"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	<-mr.registerChannel
	rand.Seed(86)
	for task := 0; task < ntasks; {
		wkName := mr.workers[rand.Intn(len(mr.workers))]

		doTaskArgs := DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[task],
			Phase:         phase,
			TaskNumber:    task,
			NumOtherPhase: nios,
		}

		ok := call(wkName, "Worker.DoTask", doTaskArgs, new(struct{}))
		if !ok {
			fmt.Printf("Schedule: RPC call to Worker.DoTask failed")
		} else {
			task++
		}
	}

	debug("Schedule: %v phase done\n", phase)
}
