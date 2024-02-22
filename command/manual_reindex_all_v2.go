package command

import (
	"crm-es/pkg/logger"
	"github.com/zenthangplus/goccm"
	"os"
)

func ManualReindexAllV2(refresh string) {
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	manualReindexAllV2(aliases, refresh)
	logger.Infof("SUCCESS")
}

func manualReindexAllV2(aliases []IndexAlias, refresh string) {
	f, err := os.OpenFile("processed_index_v2.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Fatalf("error get open file, err: %+v", err)
	}
	defer f.Close()

	c := goccm.New(3)
	for _, alias := range aliases {
		_, err := f.WriteString(alias.Alias + "\n")
		if err != nil {
			logger.Fatalf("Error write to doc")
		}
		c.Wait()
		go func(alias IndexAlias, refresh string) {
			err := getAndCreateBulkDocumentsV2(alias.Alias, refresh)
			if err != nil {
				logger.Fatalf("Error get and create document from index %s, err: %+v", alias.Alias, err)
			}
			c.Done()
		}(alias, refresh)
	}
}

func getAndCreateBulkDocumentsV2(indexName, refresh string) error {
	initialSort := getInitialSort(indexName)
	awsResult, err := searchIndex(indexName, initialSort)
	if err != nil {
		return err
	}
	var lastSort interface{}
	counter := getInitialCounter(indexName)
	for len(awsResult) > 0 {
		reachThreshold := checkSplitThreshold(indexName, lastSort)
		if reachThreshold {
			logger.Infof("reach threshold, finished")
			break
		}
		logger.Infof("Starting indexing %s for %d documents", indexName, len(awsResult))
		counterBulk := 0
		var bulkBody []string
		ci := goccm.New(10)
		for _, doc := range awsResult {
			counter = counter + 1

			for index := range doc.Sort {
				lastSort = doc.Sort[index]
				break
			}

			if counterBulk%5 == 0 {
				ci.Wait()
				go func(bulkBody []string, indexName, refresh string) {
					err := bulkIndexing(bulkBody, indexName, refresh)
					if err != nil {
						logger.Errorf("error during bulk indexing %s, last sort %+v, err %+v", indexName, lastSort, err)
					}
					ci.Done()
				}(bulkBody, indexName, refresh)

				bulkBody = []string{}
			}

			counterBulk += 1
			partBody, err := buildBodyPart(indexName, doc)
			if err != nil {
				logger.Errorf("error during building body part index %s, last sort %+v, err %+v", indexName, lastSort, err)
				continue
			}
			bulkBody = append(bulkBody, partBody...)

			logger.Infof("[%s] Counter %d, Last Sort %+v", indexName, counter, lastSort)
		}
		if counterBulk > 0 {
			// bulk indexing
			err = bulkIndexing(bulkBody, indexName, refresh)
			if err != nil {
				logger.Errorf("error during bulk indexing %s, last sort %+v, err %+v", indexName, lastSort, err)
				continue
			}
		}
		awsResult, err = searchIndex(indexName, lastSort)
		if err != nil {
			logger.Errorf("error during searching next page for index %s, last sort %+v, err %+v", indexName, lastSort, err)
			continue
		}
	}
	logger.Infof("Finish indexing %s", indexName)

	return nil
}
