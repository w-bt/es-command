package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

func MergeAll() {
	aliases, err := getAllIndices()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}
	mergeIndices(aliases)
}

func mergeIndices(aliases []IndexAlias) {
	var responses []ReindexResp
	for _, item := range aliases {
		destIndex := fmt.Sprintf("migration_%+v_%s", item.Alias, getTimeFormat())
		resp, err := merge(destIndex, item.Index)
		if err != nil {
			logger.Errorf("Error during reindexing, index name: %s, err: %+v", destIndex, err)
			continue
		}

		responses = append(responses, resp)

		logger.Infof("Result for reindexing (%s) creation, result (%+v)", destIndex, resp)
	}

	err := saveMergeResponses(responses)
	if err != nil {
		logger.Errorf("Error during saving responses, responses: %+v, err: %+v", responses, err)
	}
}

func merge(sourceIndex, destIndex string) (ReindexResp, error) {
	reindexBody := getMergeRequestBody(sourceIndex, destIndex)
	bodyInBytes, err := json.Marshal(reindexBody)
	if err != nil {
		return ReindexResp{}, err
	}
	logger.Infof("merge %s to %s with req body %s", sourceIndex, destIndex, string(bodyInBytes))
	postBody := bytes.NewBuffer(bodyInBytes)
	// NOTE: please change wait_for_active_shards to all for production env
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/_reindex?wait_for_completion=false&wait_for_active_shards=1", destHostName), postBody)
	if err != nil {
		return ReindexResp{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return ReindexResp{}, err
	}
	defer resp.Body.Close()

	bodyResult, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ReindexResp{}, err
	}

	var reindexResp ReindexResp
	err = json.Unmarshal(bodyResult, &reindexResp)
	if err != nil {
		return ReindexResp{}, err
	}
	reindexResp.Index = destIndex

	return reindexResp, nil
}

func getMergeRequestBody(source, dest string) ReindexBody {
	return ReindexBody{
		Conflicts: "proceed",
		Source: ReindexSource{
			Index: source,
		},
		Dest: ReindexDest{
			Index: dest,
		},
	}
}

func saveMergeResponses(responses []ReindexResp) error {
	file, err := json.MarshalIndent(responses, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("merge_response.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
