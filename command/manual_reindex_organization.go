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
	"sync"
	"time"
)

type RequestBodySearchOrganization struct {
	Query       *QueryBoolOrganization `json:"query,omitempty"`
	Size        int                    `json:"size"`
	SearchAfter []interface{}          `json:"search_after,omitempty"`
	Sort        []RequestSortID        `json:"sort"`
}

type QueryBoolOrganization struct {
	Bool *BoolFilterOrganization `json:"bool,omitempty"`
}

type BoolFilterOrganization struct {
	Filter []interface{} `json:"filter,omitempty"`
}

type FilterTermsOrganization struct {
	Terms *TermsOrganization `json:"terms,omitempty"`
}

type TermsOrganization struct {
	OrganizationID *[]string `json:"organization_id,omitempty"`
}

type FilterRangeOrganization struct {
	Range *RangeThreshold `json:"range,omitempty"`
}

type IndexBulkOrganization struct {
	Index IndexBulkInfo `json:"index"`
}

type IndexBulkInfoOrganization struct {
	Index   string `json:"_index"`
	Type    string `json:"_type,omitempty"`
	Id      string `json:"_id"`
	Routing string `json:"routing,omitempty"`
}

type IndexAliasWithThresholdAndOrganization struct {
	IndexAlias
	Threshold         string `json:"threshold"`
	HasOrganizationID bool   `json:"has_organization_id,omitempty"`
}

func ManualReindexOrganization(date, organizationID string) {
	startTime = time.Now()
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	aliasThreshold := buildAliasThresholdAndOrganization(aliases)
	logger.Infof("Threshold %+v", aliasThreshold)

	manualReindexOrganization(aliasThreshold, date, organizationID)
	duration = time.Since(startTime)
	logger.Infof("Finish, total execution time %f minutes", duration.Minutes())
}

func buildAliasThresholdAndOrganization(aliases []IndexAlias) (aliasThreshold []IndexAliasWithThresholdAndOrganization) {
	var createdAtOnlyList, updatedAtOnlyList, callTimeOnlyList, dealWonAtOnlyList, submittedAtOnlyList, otherList []IndexAliasWithThresholdAndOrganization
	for _, item := range aliases {
		thresholdResult, hasOrganizationID, err := checkThresholdAndOrganization(item.Alias)
		if err != nil {
			logger.Errorf("Error during checking treshold, responses: err: %+v", err)
			continue
		}
		switch thresholdResult {
		case UpdatedAtOnly:
			updatedAtOnlyList = append(updatedAtOnlyList, IndexAliasWithThresholdAndOrganization{item, UpdatedAtOnly, hasOrganizationID})
		case CreatedAtOnly:
			createdAtOnlyList = append(createdAtOnlyList, IndexAliasWithThresholdAndOrganization{item, CreatedAtOnly, hasOrganizationID})
		case CallTimeOnly:
			callTimeOnlyList = append(callTimeOnlyList, IndexAliasWithThresholdAndOrganization{item, CallTimeOnly, hasOrganizationID})
		case DealWonAtOnly:
			dealWonAtOnlyList = append(dealWonAtOnlyList, IndexAliasWithThresholdAndOrganization{item, DealWonAtOnly, hasOrganizationID})
		case SubmittedAtOnly:
			submittedAtOnlyList = append(submittedAtOnlyList, IndexAliasWithThresholdAndOrganization{item, SubmittedAtOnly, hasOrganizationID})
		case OtherThreshold:
			otherList = append(otherList, IndexAliasWithThresholdAndOrganization{item, OtherThreshold, hasOrganizationID})
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

func manualReindexOrganization(aliases []IndexAliasWithThresholdAndOrganization, date, refresh string) {
	f, err := os.OpenFile("processed_index_organization.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
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
		go func(alias IndexAliasWithThresholdAndOrganization, date, refresh string, wgP *sync.WaitGroup) {
			err := getAndCreateBulkDocumentsWithThresholdAndOrganization(alias, date, refresh)
			if err != nil {
				logger.Fatalf("Error get and create document from index %s, err: %+v", alias.Alias, err)
			}
			c.Done()
			wgP.Done()
		}(alias, date, refresh, &wg)
	}
	wg.Wait()
}

func getAndCreateBulkDocumentsWithThresholdAndOrganization(alias IndexAliasWithThresholdAndOrganization, date, organizationID string) error {
	indexName := alias.Alias
	initialSort := getInitialSort(alias.Alias)
	awsResult, err := searchIndexWithThresholdAndOrganization(alias, date, organizationID, initialSort)
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
				}(bulkBody, indexName, "false")

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
			err = bulkIndexing(bulkBody, indexName, "false")
			if err != nil {
				logger.Errorf("error during bulk indexing %s, last sort %+v, err %+v", indexName, lastSort, err)
				continue
			}
		}
		awsResult, err = searchIndexWithThresholdAndOrganization(alias, date, organizationID, lastSort)
		if err != nil {
			logger.Errorf("error during searching next page for index %s, last sort %+v, err %+v", indexName, lastSort, err)
			continue
		}
	}
	logger.Infof("Finish indexing %s", indexName)

	return nil
}

func searchIndexWithThresholdAndOrganization(alias IndexAliasWithThresholdAndOrganization, date, organizationID string, lastSort interface{}) (docs []ResponseBodyDoc, err error) {
	reqBody := RequestBodySearchOrganization{
		Size: 10000,
	}

	var filters []interface{}
	if alias.HasOrganizationID {
		filters = append(filters, &FilterTermsOrganization{Terms: &TermsOrganization{OrganizationID: &[]string{organizationID}}})
	}

	switch alias.Threshold {
	case UpdatedAtOnly:
		filters = append(filters, &FilterRangeOrganization{Range: &RangeThreshold{
			UpdatedAt: &RangeGTE{Gte: date},
		}})
		query := &QueryBoolOrganization{Bool: &BoolFilterOrganization{Filter: filters}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{UpdatedAt: "desc"}}
	case CreatedAtOnly:
		filters = append(filters, &FilterRange{Range: &RangeThreshold{
			CreatedAt: &RangeGTE{Gte: date},
		}})
		query := &QueryBoolOrganization{Bool: &BoolFilterOrganization{Filter: filters}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{CreatedAt: "desc"}}
	case CallTimeOnly:
		filters = append(filters, &FilterRange{Range: &RangeThreshold{
			CallTime: &RangeGTE{Gte: date},
		}})
		query := &QueryBoolOrganization{Bool: &BoolFilterOrganization{Filter: filters}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{CallTime: "desc"}}
	case DealWonAtOnly:
		filters = append(filters, &FilterRange{Range: &RangeThreshold{
			DealWonAt: &RangeGTE{Gte: date},
		}})
		query := &QueryBoolOrganization{Bool: &BoolFilterOrganization{Filter: filters}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{DealWonAt: "desc"}}
	case SubmittedAtOnly:
		filters = append(filters, &FilterRange{Range: &RangeThreshold{
			SubmittedAt: &RangeGTE{Gte: date},
		}})
		query := &QueryBoolOrganization{Bool: &BoolFilterOrganization{Filter: filters}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{SubmittedAt: "desc"}}
	default:
		query := &QueryBoolOrganization{Bool: &BoolFilterOrganization{Filter: filters}}
		reqBody.Query = query
		reqBody.Sort = []RequestSortID{{ID: "desc"}}
	}

	if lastSort != nil {
		reqBody.SearchAfter = []interface{}{lastSort}
	}

	reqByte, err := json.Marshal(reqBody)
	if err != nil {
		return
	}
	logger.Infof("req body %+v %+v", string(reqByte), alias.Alias)

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
