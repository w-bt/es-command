package command

import (
	"crm-es/pkg/logger"
	"encoding/json"
	"io/ioutil"
	"os"
)

func Reindex(sourceIndex, sourceAlias, destIndex, updatedAt string) {
	threshold := checkThresholds([]IndexAlias{{Alias: sourceAlias}})
	resp, err := reindex(sourceIndex, sourceAlias, destIndex, updatedAt, threshold)
	if err != nil {
		logger.Errorf("Error during reindexing, index name: %s, err: %+v", sourceIndex, err)
		return
	}

	logger.Infof("Result for reindexing (%s) creation, result (%+v)", sourceIndex, resp)

	err = saveSingleResponse(resp)
	if err != nil {
		logger.Errorf("Error during saving response, index name: %s, err: %+v", sourceIndex, err)
		return
	}
}

func saveSingleResponse(response ReindexResp) error {
	file, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("last_reindex.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
