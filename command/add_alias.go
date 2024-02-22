package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type ReqAlias struct {
	Actions AliasCommand `json:"actions,omitempty"`
}

type AliasCommand struct {
	Add AliasAdd `json:"add,omitempty"`
}

type AliasAdd struct {
	Index string `json:"index,omitempty"`
	Alias string `json:"alias,omitempty"`
}

func AddAlias() {
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	addAliases(aliases)
	logger.Infof("SUCCESS")
}

func addAliases(aliases []IndexAlias) {
	for _, item := range aliases {
		result, err := addAlias(item)
		if err != nil {
			logger.Errorf("Error during adding alias, new index name: %s, err: %+v", item.Index, err)
			continue
		}

		logger.Infof("Result for new index (%s) alias, result (%+v)", item.Index, string(result))
	}
}

func addAlias(item IndexAlias) ([]byte, error) {
	aliasAdd := AliasAdd{
		Index: item.Index,
		Alias: item.Alias,
	}
	aliasCmd := AliasCommand{Add: aliasAdd}
	reqBody := ReqAlias{Actions: aliasCmd}
	reqByte, err := json.Marshal(reqBody)
	if err != nil {
		return []byte{}, err
	}
	postBody := bytes.NewBuffer(reqByte)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/_aliases", destHostName), postBody)
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
