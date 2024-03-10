package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
)

type frequency struct {
	key string
	val int
}

func TestVanilla(t *testing.T) {
	setupTestData()
	CollectMapperOutput(1)
}

/*
*
mapper = 3, reducer = 2, samplePerFile = 2

Format
InputFileName --> key1	key2

0-0 --> a	b
0-1 --> p	q
1-0 --> a	c
1-1 --> p	r
2-0 --> c	d
2-1 --> r	r

Output --
Reducer 1 -> {a:2, b:1, c:2, d:1}
Reducer 2 -> {p:2, q:1, r: 3}
*
*/
func setupTestData() {
	file00Content := []frequency{{"a", 1}, {"b", 1}}
	writeJsonFile(&file00Content, "mr-out-0-0")
	// file01Content := []frequency{{"a", 1}, {"b", 1}}
	// writeJsonFile(file01Content, "mr-out-0-1")
	// file10Content := []frequency{{"a", 1}, {"b", 1}}
	// writeJsonFile(file10Content, "mr-out-1-0")
	// file11Content := []frequency{{"a", 1}, {"b", 1}}
	// writeJsonFile(file11Content, "mr-out-1-1")
	// file20Content := []frequency{{"a", 1}, {"b", 1}}
	// writeJsonFile(file20Content, "mr-out-2-0")
	// file21Content := []frequency{{"a", 1}, {"b", 1}}
	// writeJsonFile(file21Content, "mr-out-2-1")
}

func writeJsonFile(content *[]frequency, fileName string) {
	// ofile, _ := os.Create(fileName)
	// enc := json.NewEncoder(ofile)
	// enc.Encode(&content)
	// defer ofile.Close()
	file, _ := json.Marshal(content)
	ioutil.WriteFile(fileName, file, 0644)
	fmt.Print("********")
	fmt.Print(content)
}
