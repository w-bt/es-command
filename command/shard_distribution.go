package command

import (
	"encoding/csv"
	"fmt"
	"os"
)

func ShardDistribution() ([]byte, error) {
	// Open the CSV file
	file, err := os.Open("es.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return []byte{}, nil
	}
	defer file.Close()

	reader := csv.NewReader(file)

	no := 0
	dataCSV := make(map[string][]string)
	dataCSV["index"] = []string{}
	dataCSV["shard"] = []string{}
	dataCSV["prirep"] = []string{}
	dataCSV["docs"] = []string{}
	dataCSV["store"] = []string{}
	dataCSV["standard_store"] = []string{}
	dataCSV["ip"] = []string{}
	dataCSV["node"] = []string{}
	for {
		record, err := reader.Read()

		if err != nil {
			break
		}

		if no != 0 {
			for index, value := range record {
				switch index {
				case 0:
					dataCSV["index"] = append(dataCSV["index"], value)
				case 1:
					dataCSV["shard"] = append(dataCSV["shard"], value)
				case 2:
					dataCSV["prirep"] = append(dataCSV["prirep"], value)
				case 3:
					dataCSV["state"] = append(dataCSV["state"], value)
				case 4:
					dataCSV["docs"] = append(dataCSV["docs"], value)
				case 5:
					dataCSV["store"] = append(dataCSV["store"], value)
				case 6:
					dataCSV["standard_store"] = append(dataCSV["standard_store"], value)
				case 7:
					dataCSV["ip"] = append(dataCSV["ip"], value)
				case 8:
					dataCSV["node"] = append(dataCSV["node"], value)
				}
			}
		}
		no++
	}

	uIndex := uniqueIndex(dataCSV["index"])
	uNode := uniqueNode(dataCSV["node"])
	indexNodeMap := mapStoreNodeIndex(dataCSV["index"], dataCSV["node"], dataCSV["prirep"], dataCSV["shard"], dataCSV["standard_store"])
	fmt.Printf("testing %+v", indexNodeMap)

	// Open the CSV file for writing
	fileOutput, err := os.Create("output_es.csv")
	if err != nil {
		panic(err)
	}
	defer fileOutput.Close()

	// Create a CSV writer
	writer := csv.NewWriter(fileOutput)
	defer writer.Flush()

	// Write header
	header := []string{"Node"}
	for _, v := range uIndex {
		header = append(header, v, "shard")
	}

	err = writer.Write(header)
	if err != nil {
		panic(err)
	}

	// Write data rows
	data := mapStore(uIndex, uNode, indexNodeMap)

	for _, row := range data {
		err := writer.Write(row)
		if err != nil {
			panic(err)
		}
	}

	// Flush the writer to ensure all data is written to the file
	writer.Flush()

	return []byte{}, nil
}

func uniqueIndex(indices []string) (result []string) {
	resultMap := make(map[string]bool)
	for _, v := range indices {
		resultMap[v] = true
	}

	for k, _ := range resultMap {
		result = append(result, k)
	}

	return
}

func uniqueNode(nodes []string) (result []string) {
	resultMap := make(map[string]bool)
	for _, v := range nodes {
		resultMap[v] = true
	}

	for k, _ := range resultMap {
		result = append(result, k)
	}

	return
}

func mapStoreNodeIndex(index, node, prirep, shard, standardStore []string) map[string]map[string][]string {
	result := make(map[string]map[string][]string)
	for i, v := range index {
		_, okIndex := result[v]
		if !okIndex {
			result[v] = make(map[string][]string)
		}
		result[v][node[i]] = []string{standardStore[i], prirep[i] + shard[i]}
	}

	return result
}

func mapStore(uIndex, uNode []string, mapIndexNode map[string]map[string][]string) (result [][]string) {
	for _, node := range uNode {
		row := []string{node}
		for _, index := range uIndex {
			val := []string{"0", ""}
			v, ok := mapIndexNode[index][node]
			if ok {
				val = v
			}
			row = append(row, val...)
		}
		result = append(result, row)
	}

	return result
}
