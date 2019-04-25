package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	fmt.Println("Start Reduce")
	//kvMap is used for collecting kv pairs in all the intermediate files
	kvMap := make(map[string][]string)

	//read all the intermediate files
	//decode each line to get the kv pair
	//put them into kvMap
	for i := 0; i < nMap; i++ {
		fname := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fname)
		defer file.Close()
		if err != nil {
			panic(err)
		}

		var entry map[string][]string
		reader := bufio.NewReader(file)
		//in the intermediate file, each line is a map json string
		for {
			line, _, eof := reader.ReadLine()
			if eof == io.EOF {
				break
			}
			if err := json.Unmarshal(line, &entry); err != nil {
				panic(err)
			}
			for k, v := range entry {
				kvMap[k] = v
			}
		}
	}

	//sort the intermediate kv pairs
	keys := make([]string, len(kvMap))
	i := 0
	for k, _ := range kvMap {
		keys[i] = k
		i++
	}

	//call the user defined function to get results
	var results []string
	for _, k := range keys {
		result := reduceF(k, kvMap[k])
		results = append(results, result)
	}

	//encode and write results to disk
	file, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(file)
	for _, res := range results {
		err := enc.Encode(res)
		if err != nil {
			panic(err)
		}
	}
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
