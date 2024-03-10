package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func CollectMapperOutput(reducerId int) {
	var kva []KeyValue
	for mapFileIdx := 0; mapFileIdx < MAX_MAP_TASKS; mapFileIdx++ {
		mapFileName := fmt.Sprintf(MAP_OUT_FILE_FORMAT, mapFileIdx, reducerId)
		file, err := os.Open(mapFileName)
		if err != nil {
			log.Fatalf("cannot open %v", mapFileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
}
