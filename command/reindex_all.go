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

type ReindexBody struct {
	Conflicts string        `json:"conflicts,omitempty"`
	Source    ReindexSource `json:"source"`
	Dest      ReindexDest   `json:"dest"`
}

type ReindexSource struct {
	Index string      `json:"index"`
	Query SourceQuery `json:"query,omitempty"`
}

type SourceQuery struct {
	Range Range `json:"range,omitempty"`
}

type Range struct {
	UpdatedAt TimeGTE `json:"updated_at,omitempty"`
	CreatedAt TimeGTE `json:"created_at,omitempty"`
}

type TimeGTE struct {
	Gte string `json:"gte,omitempty"`
}

type ReindexDest struct {
	Index string `json:"index"`
}

type ReindexResp struct {
	Task  string `json:"task"`
	Index string `json:"index,omitempty"`
}

func ReindexAll(updatedAt string) {
	aliases, err := getAllIndices()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}
	threshold := checkThresholds(aliases)
	reindexIndices(aliases, updatedAt, threshold)
}

func reindexIndices(aliases []IndexAlias, updatedAt string, threshold Threshold) {
	var responses []ReindexResp
	for _, item := range aliases {
		destIndex := fmt.Sprintf("migration_%+v_%s", item.Alias, getTimeFormat())
		resp, err := reindex(item.Index, item.Alias, destIndex, updatedAt, threshold)
		if err != nil {
			logger.Errorf("Error during reindexing, index name: %s, err: %+v", item.Index, err)
			continue
		}

		responses = append(responses, resp)

		logger.Infof("Result for reindexing (%s) creation, result (%+v)", item.Index, resp)
	}

	err := saveResponses(responses)
	if err != nil {
		logger.Errorf("Error during saving responses, responses: %+v, err: %+v", responses, err)
	}
}

func reindex(sourceIndex, sourceAlias, destIndex, updatedAt string, threshold Threshold) (ReindexResp, error) {
	reindexBody := getReindexRequestBody(sourceIndex, sourceAlias, destIndex, updatedAt, threshold)
	bodyInBytes, err := json.Marshal(reindexBody)
	if err != nil {
		return ReindexResp{}, err
	}
	logger.Infof("reindex %s to %s with req body %s", sourceIndex, destIndex, string(bodyInBytes))
	postBody := bytes.NewBuffer(bodyInBytes)
	// NOTE: please change wait_for_active_shards to all for production env
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/_reindex?wait_for_completion=false&wait_for_active_shards=1", sourceHostName), postBody)
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

func getReindexRequestBody(source, alias, dest string, updatedAt string, threshold Threshold) ReindexBody {
	body := ReindexBody{
		Conflicts: "proceed",
		Source: ReindexSource{
			Index: source,
			Query: SourceQuery{
				Range: Range{
					UpdatedAt: TimeGTE{
						Gte: updatedAt,
					},
				},
			},
		},
		Dest: ReindexDest{
			Index: dest,
		},
	}

	if _, ok := threshold.UpdatedAtOnly[alias]; ok {
		body.Source.Query.Range.UpdatedAt.Gte = updatedAt
	} else if _, ok := threshold.CreatedAtOnly[alias]; ok {
		body.Source.Query.Range.CreatedAt.Gte = updatedAt
	}

	return body
}

func saveResponses(responses []ReindexResp) error {
	file, err := json.MarshalIndent(responses, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("reindex_response.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
