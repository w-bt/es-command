package command

import (
	"crm-es/pkg/logger"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

func Duplicate(indexName string) {
	alias, err := getAlias(indexName)
	if err != nil {
		logger.Fatalf("error get alias, err: %+v", err)
	}

	err = duplicateIndex(alias)
	if err != nil {
		logger.Fatalf("error duplicate index :%s, err: %+v", indexName, err)
	}
}

func getAlias(indexName string) (alias IndexAlias, err error) {
	resp, err := http.Get(fmt.Sprintf("%s/_cat/aliases/%s/?format=json", sourceHostName, indexName))
	if err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var aliases []IndexAlias
	err = json.Unmarshal(body, &aliases)
	if err != nil {
		return IndexAlias{}, err
	}

	if len(aliases) == 0 {
		return IndexAlias{}, errors.New("empty alias")
	}

	return aliases[0], nil
}

func duplicateIndex(alias IndexAlias) error {
	newConfig, err := getNewConfig(alias.Index, alias.Alias)
	if err != nil {
		logger.Errorf("Error during get new config, index: %s, err: %+v", alias.Alias, err)
		return err
	}

	newIndexName := fmt.Sprintf("migration_%+v_%s", alias.Alias, getTimeFormat())
	if alias.Alias == "stag_models_contacts" {
		logger.Infof("config %+v", string(newConfig))
	}
	result, err := createIndex(newIndexName, newConfig)
	if err != nil {
		logger.Errorf("Error during get new config, new index name: %s, err: %+v", newIndexName, err)
		return err
	}

	logger.Infof("Result for new index (%s) creation, result (%+v)", newIndexName, string(result))
	return nil
}
