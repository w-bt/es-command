package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/zenthangplus/goccm"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	startTime time.Time
	duration  time.Duration
)

type QueryBool struct {
	Bool *BoolFilter `json:"bool,omitempty"`
}

type BoolFilter struct {
	Filter *FilterRange `json:"filter,omitempty"`
}

type FilterRange struct {
	Range *RangeThreshold `json:"range,omitempty"`
}

type RangeThreshold struct {
	CreatedAt   *RangeGTE `json:"created_at,omitempty"`
	UpdatedAt   *RangeGTE `json:"updated_at,omitempty"`
	CallTime    *RangeGTE `json:"call_time,omitempty"`
	DealWonAt   *RangeGTE `json:"deal_won_at,omitempty"`
	SubmittedAt *RangeGTE `json:"submitted_at,omitempty"`
}

type RangeGTE struct {
	Gte string `json:"gte,omitempty"`
}

type IndexBulk struct {
	Index IndexBulkInfo `json:"index"`
}

type IndexBulkInfo struct {
	Index   string `json:"_index"`
	Type    string `json:"_type,omitempty"`
	Id      string `json:"_id"`
	Routing string `json:"routing,omitempty"`
}

type IndexAliasWithThreshold struct {
	IndexAlias
	Threshold string `json:"threshold"`
}

func ManualReindexChunk(date, refresh string) {
	startTime = time.Now()
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	aliasThreshold := buildAliasThreshold(aliases)

	manualReindexChunk(aliasThreshold, date, refresh)
	duration = time.Since(startTime)
	logger.Infof("Finish, total execution time %f minutes", duration.Minutes())
}

func buildAliasThreshold(aliases []IndexAlias) (aliasThreshold []IndexAliasWithThreshold) {
	var createdAtOnlyList, updatedAtOnlyList, callTimeOnlyList, dealWonAtOnlyList, submittedAtOnlyList, otherList []IndexAliasWithThreshold
	for _, item := range aliases {
		resultBool, err := checkThreshold(item.Alias)
		if err != nil {
			logger.Errorf("Error during checking treshold, responses: err: %+v", err)
			continue
		}
		switch resultBool {
		case UpdatedAtOnly:
			updatedAtOnlyList = append(updatedAtOnlyList, IndexAliasWithThreshold{item, UpdatedAtOnly})
		case CreatedAtOnly:
			createdAtOnlyList = append(createdAtOnlyList, IndexAliasWithThreshold{item, CreatedAtOnly})
		case CallTimeOnly:
			callTimeOnlyList = append(callTimeOnlyList, IndexAliasWithThreshold{item, CallTimeOnly})
		case DealWonAtOnly:
			dealWonAtOnlyList = append(dealWonAtOnlyList, IndexAliasWithThreshold{item, DealWonAtOnly})
		case SubmittedAtOnly:
			submittedAtOnlyList = append(submittedAtOnlyList, IndexAliasWithThreshold{item, SubmittedAtOnly})
		case OtherThreshold:
			otherList = append(otherList, IndexAliasWithThreshold{item, OtherThreshold})
		}
	}

	aliasThreshold = append(aliasThreshold, updatedAtOnlyList...)
	aliasThreshold = append(aliasThreshold, createdAtOnlyList...)
	aliasThreshold = append(aliasThreshold, callTimeOnlyList...)
	aliasThreshold = append(aliasThreshold, dealWonAtOnlyList...)
	aliasThreshold = append(aliasThreshold, submittedAtOnlyList...)
	aliasThreshold = append(aliasThreshold, otherList...)

	return
}

func manualReindexChunk(aliases []IndexAliasWithThreshold, date, refresh string) {
	f, err := os.OpenFile("processed_index.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Fatalf("error get open file, err: %+v", err)
	}
	defer f.Close()

	var wg sync.WaitGroup

	c := goccm.New(3)
	wg.Add(len(aliases))
	for _, alias := range aliases {
		_, err := f.WriteString(alias.Alias + "\n")
		if err != nil {
			logger.Fatalf("Error write to doc")
		}
		c.Wait()
		go func(alias IndexAliasWithThreshold, date, refresh string, wgP *sync.WaitGroup) {
			err := getAndCreateBulkDocumentsWithThreshold(alias, date, refresh)
			if err != nil {
				logger.Fatalf("Error get and create document from index %s, err: %+v", alias.Alias, err)
			}
			c.Done()
			wgP.Done()
		}(alias, date, refresh, &wg)
	}
	wg.Wait()
}

func getAndCreateBulkDocumentsWithThreshold(alias IndexAliasWithThreshold, date, refresh string) error {
	indexName := alias.Alias
	initialSort := getInitialSort(alias.Alias)
	awsResult, err := searchIndexWithThreshold(alias, date, initialSort)
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
		logger.Infof("Starting indexing %s for %d documents", indexName, len(awsResult))
		counterBulk := 0
		var bulkBody []string
		ci := goccm.New(5)
		for _, doc := range awsResult {
			counter = counter + 1

			for index := range doc.Sort {
				lastSort = doc.Sort[index]
				break
			}

			if counterBulk%5 == 0 {
				ci.Wait()
				go func(bulkBody []string, indexName, refresh string) {
					retry := true
					for retry {
						err := bulkIndexing(bulkBody, indexName, refresh)
						if err != nil {
							logger.Errorf("error during bulk indexing %s, last sort %+v, err %+v", indexName, lastSort, err)
						} else {
							retry = false
						}
					}
					ci.Done()
				}(bulkBody, indexName, refresh)

				bulkBody = []string{}
			}

			counterBulk += 1
			partBody, err := buildBodyPart(indexName, doc)
			if err != nil {
				logger.Errorf("error during building body part index %s, last sort %+v, err %+v", indexName, lastSort, err)
				continue
			}
			bulkBody = append(bulkBody, partBody...)

			if alias.Threshold == OtherThreshold {
				logger.Infof("[OPTIONAL][%s] Counter %d, Last Sort %+v", indexName, counter, lastSort)
			} else {
				logger.Infof("[MANDATORY][%s] Counter %d, Last Sort %+v", indexName, counter, lastSort)
			}
		}
		if counterBulk > 0 {
			// bulk indexing
			err = bulkIndexing(bulkBody, indexName, refresh)
			if err != nil {
				logger.Errorf("error during bulk indexing %s, last sort %+v, err %+v", indexName, lastSort, err)
				continue
			}
		}
		awsResult, err = searchIndexWithThreshold(alias, date, lastSort)
		if err != nil {
			logger.Errorf("error during searching next page for index %s, last sort %+v, err %+v", indexName, lastSort, err)
			continue
		}
	}
	logger.Infof("Finish indexing %s", indexName)

	return nil
}

func buildBodyPart(indexName string, doc ResponseBodyDoc) ([]string, error) {
	var result []string
	indexHeader := IndexBulk{IndexBulkInfo{
		Index: indexName,
		Type:  doc.Type,
		Id:    doc.Id,
	}}

	if doc.Routing != "" {
		indexHeader.Index.Routing = doc.Routing
	}

	headerInBytes, err := json.Marshal(indexHeader)
	if err != nil {
		logger.Errorf("error during marshaling index %s doc id %+v", indexName, doc.Id)
		return []string{}, err
	}

	sourceInBytes, err := json.Marshal(doc.Source)
	if err != nil {
		logger.Errorf("error during marshaling index %s doc id %+v", indexName, doc.Id)
		return []string{}, err
	}

	result = append(result, string(headerInBytes), string(sourceInBytes))

	return result, nil
}

func searchIndexWithThreshold(alias IndexAliasWithThreshold, date string, lastSort interface{}) (docs []ResponseBodyDoc, err error) {
	reqBody := RequestBodySearch{
		Size: 10000,
	}

	switch alias.Threshold {
	case UpdatedAtOnly:
		query := &QueryBool{Bool: &BoolFilter{Filter: &FilterRange{Range: &RangeThreshold{
			UpdatedAt: &RangeGTE{Gte: date},
		}}}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{UpdatedAt: "asc"}}
	case CreatedAtOnly:
		query := &QueryBool{Bool: &BoolFilter{Filter: &FilterRange{Range: &RangeThreshold{
			CreatedAt: &RangeGTE{Gte: date},
		}}}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{CreatedAt: "asc"}}
	case CallTimeOnly:
		query := &QueryBool{Bool: &BoolFilter{Filter: &FilterRange{Range: &RangeThreshold{
			CallTime: &RangeGTE{Gte: date},
		}}}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{CallTime: "asc"}}
	case DealWonAtOnly:
		query := &QueryBool{Bool: &BoolFilter{Filter: &FilterRange{Range: &RangeThreshold{
			DealWonAt: &RangeGTE{Gte: date},
		}}}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{DealWonAt: "asc"}}
	case SubmittedAtOnly:
		query := &QueryBool{Bool: &BoolFilter{Filter: &FilterRange{Range: &RangeThreshold{
			SubmittedAt: &RangeGTE{Gte: date},
		}}}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{SubmittedAt: "asc"}}
	default:
		reqBody.Sort = []RequestSortID{{ID: "asc"}}
	}

	if lastSort != nil {
		reqBody.SearchAfter = []interface{}{lastSort}
	}

	reqByte, err := json.Marshal(reqBody)
	if err != nil {
		return
	}

	postBody := bytes.NewBuffer(reqByte)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", sourceHostName, alias.Alias), postBody)
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

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

func bulkIndexing(body []string, indexName, refresh string) error {
	bodyString := strings.Join(body, "\n") + "\n"
	postBody := bytes.NewBuffer([]byte(bodyString))
	additionalParams := ""
	if refresh != "false" {
		additionalParams += "?refresh=" + refresh
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/_bulk%s", destHostName, additionalParams), postBody)
	if err != nil {
		logger.Debugf("error create new request %s/_bulk%s with body %s", destHostName, additionalParams, bodyString)
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	rawResp, err := client.Do(req)
	if err != nil {
		logger.Debugf("error during indexing %s/_bulk%s with body %s", destHostName, additionalParams, bodyString)
		return err
	}
	defer rawResp.Body.Close()

	_, err = ioutil.ReadAll(rawResp.Body)
	if err != nil {
		logger.Debugf("error read all bulk indexing %s/_bulk%s with body %s", destHostName, additionalParams, bodyString)
		return err
	}

	return nil
}
