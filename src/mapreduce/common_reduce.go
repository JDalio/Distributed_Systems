package mapreduce

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//kvChan is the kv Channel which Goroutines could
	//send kv pairs after reading and reducing
	kvChan := make(chan map[string][]string)

	//manageKvChan is for closing kvChan after
	//Goroutines finish reading and decoding
	manageKvChan := make(chan int)
	go func() {
		for i := 0; i < nMap; i++ {
			<-manageKvChan
		}
		close(kvChan)
	}()

	//kvMap is used for receiving kv pairs from kvChan
	kvMap := make(map[string][]string)

	//Step1: create nMap Goroutines
	//each goroutine read the file belonging -> decode it
	//-> send to the kvChan
	for i := 0; i < nMap; i++ {
		go func(nthMap int) {
			//get file name
			fname := reduceName(jobName, nthMap, reduceTask)
			file, err := os.Open(fname)
			if err != nil {
				log.Fatal("doReduce Step1 open file:", err)
			}
			defer file.Close()

			//read file line by line
			//decode and send to kvChan
			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)
			for scanner.Scan() {
				var entry map[string][]string
				lineByt := scanner.Bytes()
				//log.Print("Reading",string(lineByt))
				if err := json.Unmarshal(lineByt, &entry); err != nil {
					log.Fatal("doReduce Step1 decode:", err)
				}
				kvChan <- entry
			}
			manageKvChan <- 1
		}(i)
	}

	for kv := range kvChan {
		for k, v := range kv {
			kvMap[k] = append(kvMap[k], v...)
		}
	}

	keys := make([]string, len(kvMap))
	i := 0
	for k := range kvMap {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	//Step3: encode and write the results to the file
	file, err := os.OpenFile(outFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("doReduce Step3 open result file:", err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, k := range keys {
		//call the user defined function to get results
		result := reduceF(k, kvMap[k])
		err := enc.Encode(KeyValue{k, result})
		if err != nil {
			log.Fatal("doReduce Step1 write encode result:", err)
		}
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
