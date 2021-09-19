package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	contents, err := ioutil.ReadFile(inFile)
	checkError(err)

	keyValues := mapF(inFile, string(contents))

	reduceEncoders := []*json.Encoder{}
	reduceFiles := []*os.File{}
	for r := 0; r < nReduce; r++ {
		rFilename := reduceName(jobName, mapTaskNumber, r)
		rFile, err := os.Create(rFilename)
		checkError(err)
		reduceEncoders = append(reduceEncoders, json.NewEncoder(rFile))
		reduceFiles = append(reduceFiles, rFile)
	}

	for _, kv := range keyValues {
		rEnc := reduceEncoders[ihash(kv.Key)%uint32(nReduce)]
		err := rEnc.Encode(&kv)
		checkError(err)
	}

	for _, rf := range reduceFiles {
		rf.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
