package command

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func NodeLoad() {
	nodeMap := getNodeMap()
	err := mapTotalHitOnNodes(nodeMap)
	if err != nil {
		log.Println("failed to back fill nodes")
	}

	hitPerNode, err := collectHitPerNode()
	if err != nil {
		log.Println("failed to collect hit per node")
	}

	err = convertHitToCSV(hitPerNode)
	if err != nil {
		log.Println("failed to collect hit per node")
	}
}

func getNodeMap() map[string][]string {
	log.Println("Generating node map")
	file, err := os.Open("output_shard_distribution.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	no := 0
	nodeMap := make(map[string][]string)
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if no != 0 {
			var index, routing, firstNode, secondNode string
			for i, value := range record {
				switch i {
				case 0:
					index = value
				case 1:
					routing = value
				case 2:
					continue
				case 3:
					firstNode = value
				case 4:
					secondNode = value
				}
			}
			nodeMap[fmt.Sprintf("%s | %s", index, routing)] = []string{firstNode, secondNode}
		}

		no++
	}

	log.Println("Finish generating node map")

	return nodeMap
}

func mapTotalHitOnNodes(nodeMap map[string][]string) error {
	log.Println("Backfill nodes")
	file, err := os.Open("total_hit.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		return err
	}

	// Index,Time,Routing,Total,First Node,Second Shard
	for i, record := range records {
		if i == 0 {
			continue
		}
		copyRecord := record
		key := ""
		for k, v := range copyRecord {
			if k == 0 {
				key = v
			}
			if k == 2 {
				key = key + " | " + v
				nodes, ok := nodeMap[key]
				if ok {
					copyRecord[4] = nodes[0]
					copyRecord[5] = nodes[1]
				}
				key = ""
			}
		}
		records[i] = copyRecord
	}

	// Open the same file for writing
	file, err = os.Create("total_hit.csv")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return err
	}
	defer file.Close()

	// Write the modified data back to the CSV file
	writer := csv.NewWriter(file)
	err = writer.WriteAll(records)
	if err != nil {
		fmt.Println("Error writing CSV:", err)
		return err
	}

	fmt.Println("CSV file has been successfully modified and written.")

	return nil
}

func collectHitPerNode() (map[string]int, error) {
	log.Println("Backfilling nodes")
	file, err := os.Open("total_hit.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		return nil, err
	}

	// Index,Time,Routing,Total,First Node,Second Shard
	mapHitNode := make(map[string]int)
	for i, record := range records {
		if i == 0 {
			continue
		}
		copyRecord := record
		keyNode1 := ""
		keyNode2 := ""
		total := 0
		for k, v := range copyRecord {
			if k == 1 {
				keyNode1 = v
				keyNode2 = v
			}
			if k == 3 {
				total, err = strconv.Atoi(v)
				if err != nil {
					log.Fatalf("error converting total to integer")
				}
			}
			if k == 4 {
				keyNode1 = keyNode1 + " | " + v
				mapHitNode[keyNode1] += total
			}
			if k == 5 {
				keyNode2 = keyNode2 + " | " + v
				mapHitNode[keyNode2] += total
				keyNode1 = ""
				keyNode2 = ""
				total = 0
			}
		}
	}
	log.Println("Success backfilling nodes")

	return mapHitNode, nil
}

func convertHitToCSV(hitPerNode map[string]int) error {
	log.Println("Converting to csv")
	// Open the CSV file for writing
	fileOutput, err := os.Create("output_hit_per_node.csv")
	if err != nil {
		panic(err)
	}
	defer fileOutput.Close()

	// Create a CSV writer
	writer := csv.NewWriter(fileOutput)
	defer writer.Flush()

	// Write header
	header := []string{"time", "node", "total"}

	err = writer.Write(header)
	if err != nil {
		panic(err)
	}

	for k, v := range hitPerNode {
		row := strings.Split(k, " | ")
		row = append(row, fmt.Sprintf("%d", v))
		err := writer.Write(row)
		if err != nil {
			panic(err)
		}
	}

	// Flush the writer to ensure all data is written to the file
	writer.Flush()

	log.Println("Finish converting to csv")

	return nil
}
