package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	keyValues := []KeyValue{}
	for m := 0; m < nMap; m++ {
		rFile, err := os.Open(reduceName(jobName, m, reduceTaskNumber))
		checkError(err)

		dec := json.NewDecoder(rFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			keyValues = append(keyValues, kv)
		}
		rFile.Close()
	}

	keyValuesMap := map[string][]string{}
	for _, kv := range keyValues {
		keyValuesMap[kv.Key] = append(keyValuesMap[kv.Key], kv.Value)
	}

	mergeFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkError(err)

	enc := json.NewEncoder(mergeFile)
	for key, values := range keyValuesMap {
		checkError(enc.Encode(KeyValue{key, reduceF(key, values)}))
	}
	mergeFile.Close()
}
