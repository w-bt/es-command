package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"fmt"
	"io/ioutil"
	"net/http"
)

func RemoveAll() {
	aliases, err := getAllIndices()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v")
	}
	removeIndices(aliases)
}

func removeIndices(aliases []IndexAlias) {
	for _, item := range aliases {
		indexName := fmt.Sprintf("migration_%+v_%s", item.Alias, getTimeFormat())
		result, err := removeIndex(indexName)
		if err != nil {
			logger.Errorf("Error during get new config, new index name: %s, err: %+v", item.Index, err)
			continue
		}

		logger.Infof("Result for remove index (%s) creation, result (%+v)", indexName, string(result))
	}
}

func removeIndex(indexName string) ([]byte, error) {
	postBody := bytes.NewBuffer(nil)
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%+v", sourceHostName, indexName), postBody)
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
