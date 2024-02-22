package command

import (
	"crm-es/pkg/logger"
	"encoding/json"
	"io/ioutil"
	"os"
)

func Merge(sourceIndex, destIndex string) {
	resp, err := merge(sourceIndex, destIndex)
	if err != nil {
		logger.Errorf("Error during reindexing, index name: %s, err: %+v", sourceIndex, err)
		return
	}

	logger.Infof("Result for reindexing (%s) creation, result (%+v)", sourceIndex, resp)

	err = saveSingleMergeResponse(resp)
	if err != nil {
		logger.Errorf("Error during saving response, index name: %s, err: %+v", sourceIndex, err)
		return
	}
}

func saveSingleMergeResponse(response ReindexResp) error {
	file, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("last_merge.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
