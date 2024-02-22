package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

type ResponseBodyCount struct {
	Count int64 `json:"count,omitempty"`
}

type RequestBodySearch struct {
	Query       *QueryBool      `json:"query,omitempty"`
	Size        int             `json:"size"`
	SearchAfter []interface{}   `json:"search_after,omitempty"`
	Sort        []RequestSortID `json:"sort"`
}

type RequestSortID struct {
	ID          string `json:"_id,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	CallTime    string `json:"call_time,omitempty"`
	DealWonAt   string `json:"deal_won_at,omitempty"`
	SubmittedAt string `json:"submitted_at,omitempty"`
}

type ResponseBodySearch struct {
	Hits ResponseBodyHits `json:"hits"`
}

type ResponseBodyHits struct {
	Docs []ResponseBodyDoc `json:"hits,omitempty"`
}

type ResponseBodyDoc struct {
	Index   string        `json:"_index"`
	Type    string        `json:"_type"`
	Id      string        `json:"_id"`
	Routing string        `json:"_routing,omitempty"`
	Source  interface{}   `json:"_source"`
	Sort    []interface{} `json:"sort"`
}

const (
	aliasFileName = "aliases.json"
)

func ManualReindexAll() {
	aliases, err := getAliasesFromFileCustom(aliasFileName)
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	manualReindexAll(aliases)
	logger.Infof("SUCCESS")
}

func getAliasesFromFileCustom(fileName string) (aliases []IndexAlias, err error) {
	body, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}
	err = json.Unmarshal(body, &aliases)
	if err != nil {
		return []IndexAlias{}, err
	}

	return aliases, nil
}

func manualReindexAll(aliases []IndexAlias) {
	for _, index := range aliases {
		isEqual, err := compareDocCount(index.Alias)
		if err != nil {
			logger.Errorf("Error during comparing doc count from index %s, err: %+v", index.Alias, err)
			continue
		}

		if isEqual {
			logger.Infof("Index %s is equal, skipped the indexing", index.Alias)
			continue
		}

		err = getAndCreateDocuments(index.Alias)
		if err != nil {
			logger.Fatalf("Error during comparing doc count from index %s", index.Alias)
			continue
		}
	}
}

func compareDocCount(indexName string) (bool, error) {
	aliCount, err := getCount(indexName, destHostName)
	if err != nil {
		return false, err
	}

	awsCount, err := getCount(indexName, sourceHostName)
	if err != nil {
		return false, err
	}

	isEqual := aliCount == awsCount
	if !isEqual {
		logger.Infof("Index %s count, alicloud %d, aws %d", indexName, aliCount, awsCount)
	}

	//if aliCount > awsCount {
	//	isEqual = true
	//}

	return isEqual, nil
}

func getCount(indexName, hostName string) (int64, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/_count", hostName, indexName), nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	bodyResult, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var countBody ResponseBodyCount
	err = json.Unmarshal(bodyResult, &countBody)
	if err != nil {
		return 0, err
	}
	return countBody.Count, nil
}

func getAndCreateDocuments(indexName string) error {
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
		for _, doc := range awsResult {
			counter = counter + 1
			docInBytes, err := json.Marshal(doc.Source)
			if err != nil {
				logger.Errorf("error during marshaling index %s doc id %+v", indexName, doc.Id)
				continue
			}
			lastSort = doc.Sort[0]

			err = indexing(indexName, doc.Type, doc.Id, doc.Routing, docInBytes)
			if err != nil {
				logger.Errorf("error during indexing %s doc id %+v, err %+v", indexName, doc.Id, err)
				continue
			}

			logger.Infof("Counter %d, Last Sort %+v", counter, lastSort)
		}
		awsResult, err = searchIndex(indexName, lastSort)
		if err != nil {
			logger.Fatalf("error during searching next page for index %s, last sort %+v, err %+v", indexName, lastSort, err)
			continue
		}
	}

	return nil
}

func checkSplitThreshold(indexName string, lastSort interface{}) bool {
	//if indexName == "crm_geolocations_staging" && len(lastSort) > 0 {
	//	first := string(lastSort[0])
	//	if first == "8" {
	//		return true
	//	}
	//}

	return false
}

func getInitialCounter(indexName string) int {
	//if indexName == "crm_product_logs_staging" {
	//	return 93351
	//} else if indexName == "audits_staging" {
	//	return 213918
	//} else if indexName == "crm_geolocations_staging" {
	//	return 265592
	//} else if indexName == "crm_log_apis_staging" {
	//	return 276885
	//}

	return 0
}

func getInitialSort(indexName string) interface{} {
	//if indexName == "crm_product_logs_staging" {
	//	return "60899"
	//} else if indexName == "audits_staging" {
	//	return "5185652"
	//} else if indexName == "crm_geolocations_staging" {
	//	return "711843"
	//} else if indexName == "crm_log_apis_staging" {
	//	return "63408"
	//}

	return nil
}

func searchIndex(indexName string, lastSort interface{}) (docs []ResponseBodyDoc, err error) {
	reqBody := RequestBodySearch{
		Size: 10000,
		Sort: []RequestSortID{{ID: "asc"}},
	}
	if lastSort != nil {
		reqBody.SearchAfter = []interface{}{lastSort}
	}
	reqByte, err := json.Marshal(reqBody)
	if err != nil {
		return
	}
	postBody := bytes.NewBuffer(reqByte)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", sourceHostName, indexName), postBody)
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	rawResp, err := client.Do(req)
	if err != nil {
		return
	}
	defer rawResp.Body.Close()

	bodyResult, err := ioutil.ReadAll(rawResp.Body)
	if err != nil {
		return
	}

	var resp ResponseBodySearch
	err = json.Unmarshal(bodyResult, &resp)
	if err != nil {
		return
	}

	return resp.Hits.Docs, nil
}

func indexing(indexName, docType, docID, routeKey string, body []byte) error {
	postBody := bytes.NewBuffer(body)
	routeParam := ""
	if routeKey != "" {
		routeParam = "?routing=" + routeKey
	}
	docType = url.QueryEscape(docType)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s/%s/%s%s", destHostName, indexName, docType, docID, routeParam), postBody)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	rawResp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer rawResp.Body.Close()

	bodyResult, err := ioutil.ReadAll(rawResp.Body)
	if err != nil {
		return err
	}

	logger.Infof("Successfully indexing %s with id %+v, resp: %+v", indexName, docID, string(bodyResult))

	return nil
}
