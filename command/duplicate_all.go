package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type IndexAlias struct {
	Alias string `json:"alias"`
	Index string `json:"index"`
}

type IndexMap struct {
	Mapping interface{} `json:"mappings"`
	Setting struct {
		Index IndexSetting `json:"index"`
	} `json:"settings"`
}

type IndexSetting struct {
	MaxResultWindow interface{} `json:"max_result_window,omitempty"`
	MaxShingleDiff  interface{} `json:"max_shingle_diff,omitempty"`
	Analysis        interface{} `json:"analysis,omitempty"`
	Block           interface{} `json:"blocks,omitempty"`
	Mapping         interface{} `json:"mapping,omitempty"`
	Routing         interface{} `json:"routing,omitempty"`
	RefreshInterval interface{} `json:"refresh_interval,omitempty"`
	Indexing        interface{} `json:"indexing,omitempty"`
	Unassigned      interface{} `json:"unassigned,omitempty"`
	Search          interface{} `json:"search,omitempty"`
	NumberOfShard   string      `json:"number_of_shards,omitempty"`
	NumberOfReplica string      `json:"number_of_replicas,omitempty"`
}

func DuplicateAll() {
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}
	duplicateIndices(aliases)
}

func getAllIndices() (aliases []IndexAlias, err error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/_cat/aliases?format=json", sourceHostName), nil)
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(body, &aliases)
	if err != nil {
		return []IndexAlias{}, err
	}

	return aliases, nil
}

func duplicateIndices(aliases []IndexAlias) {
	for _, item := range aliases {
		indexName := item.Index
		if strings.Contains(indexName, "000000000") {
			indexName = item.Alias
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

func getNewConfig(indexName, aliasName string) ([]byte, error) {
	postBody := bytes.NewBuffer(nil)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%+v", sourceHostName, aliasName), postBody)
	if err != nil {
		return []byte{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}

	jsonString := string(body)
	// remove one level json object
	rawConfig := strings.Replace(jsonString, fmt.Sprintf("{\"%s\":{", indexName), "{", -1)
	rawConfig = strings.Replace(rawConfig, fmt.Sprintf("\"aliases\":{\"%s\":{}},", aliasName), "", -1)
	rawConfig = strings.Replace(rawConfig, fmt.Sprintf("\"aliases\":{},"), "", -1)
	newConfig := rawConfig[:len(rawConfig)-1]

	// marshall and unmarshall to remove some properties
	newConfigInBytes := IndexMap{}
	err = json.Unmarshal([]byte(newConfig), &newConfigInBytes)
	if err != nil {
		return []byte{}, err
	}

	result, err := json.Marshal(newConfigInBytes)
	if err != nil {
		return []byte{}, err
	}

	return result, nil
}

func getTimeFormat() string {
	t := time.Now()
	return fmt.Sprintf("%d%02d%02d", t.Year(), int(t.Month()), t.Day())
}

func createIndex(modelName string, bodyRequest []byte) ([]byte, error) {
	postBody := bytes.NewBuffer(bodyRequest)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%+v", destHostName, modelName), postBody)
	if err != nil {
		return []byte{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()

	bodyResult, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}

	return bodyResult, nil
}
