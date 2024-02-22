package command

import (
	"crm-es/pkg/logger"
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
)

func Copy() {
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	copyAll(aliases)
	logger.Infof("SUCCESS")
}

func getAliasesFromFile() (aliases []IndexAlias, err error) {
	body, err := ioutil.ReadFile("aliases.json")
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}
	err = json.Unmarshal(body, &aliases)
	if err != nil {
		return []IndexAlias{}, err
	}

	return aliases, nil
}

func copyAll(aliases []IndexAlias) {
	for _, item := range aliases {
		indexName := item.Index
		isContained := strings.Contains(item.Index, "_20230526000000000")
		if isContained {
			indexName = strings.Replace(item.Index, "_20230526000000000", "", -1)
		}
		newConfig, err := getNewConfig(indexName, item.Alias)
		if err != nil {
			logger.Errorf("Error during get new config, index: %s, err: %+v", item.Alias, err)
			continue
		}

		result, err := createIndex(item.Index, newConfig)
		if err != nil {
			logger.Errorf("Error during get new config, new index name: %s, err: %+v", item.Index, err)
			continue
		}

		logger.Infof("Result for new index (%s) creation, result (%+v)", item.Index, string(result))
	}
}
