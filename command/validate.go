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
	"reflect"
	"sync"
	"sync/atomic"
)

var total int64 = 0

func Validate(indexName string, threshold int, forceRewrite bool) {
	records, err := getDocByIndex(indexName, sourceHostName, threshold)

	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	var wg sync.WaitGroup
	chanTotal := make(chan int64)
	chanBool := make(chan bool)

	go func() {
		for {
			select {
			case t := <-chanTotal:
				atomic.AddInt64(&total, t)
			case <-chanBool:
				break
			}
		}
	}()

	for _, doc := range records {
		wg.Add(1)
		go func(wg *sync.WaitGroup, doc ResponseBodyDoc, chanTotal chan int64) {
			logger.Infof("Starting...")
			defer wg.Done()
			destDoc, err := getDocByID(indexName, doc.Type, doc.Id, doc.Routing)
			logger.Infof("doc: %+v", doc)
			logger.Infof("destDoc: %+v", destDoc)
			if err != nil {
				logger.Infof("Doc is missing, err: %+v", err)
				docInBytes, err := json.Marshal(doc.Source)
				if err != nil {
					logger.Errorf("error during marshaling index %s doc id %+v", indexName, doc.Id)
				}

				err = indexing(indexName, doc.Type, doc.Id, doc.Routing, docInBytes)
				if err != nil {
					logger.Errorf("error during indexing %s doc id %+v, err %+v", indexName, doc.Id, err)
				} else {
					chanTotal <- 1
				}
			} else {
				docInBytes, err := json.Marshal(doc.Source)
				if err != nil {
					logger.Errorf("error during marshaling index %s doc id %+v", indexName, doc.Id)
				}

				destDocInBytes, err := json.Marshal(destDoc.Source)
				if err != nil {
					logger.Errorf("error during marshaling index %s doc id %+v", indexName, destDoc.Id)
				}

				eq, err := JsonBytesEqual(docInBytes, destDocInBytes)
				if err != nil {
					logger.Infof("Error while compare doc, err: %+v", err)
				}

				if !eq || destDoc.Source == nil {
					err = indexing(indexName, doc.Type, doc.Id, doc.Routing, docInBytes)
					if err != nil {
						logger.Errorf("error during indexing %s doc id %+v, err %+v", indexName, doc.Id, err)
					} else {
						chanTotal <- 1
					}
				}
			}
		}(&wg, doc, chanTotal)
	}

	chanBool <- true
	wg.Wait()
	close(chanBool)
	log.Printf("total executed keys %d\n", total)
}

func getDocByIndex(indexName, hostName string, threshold int) (docs []ResponseBodyDoc, err error) {
	var responses []ResponseBodyDoc

	if threshold > 10000 {
		threshold = 10000
	}

	awsResult, err := searchAllIndex(indexName, hostName, threshold, 0)

	if err != nil {
		return nil, err
	}

	responses = append(responses, awsResult...)

	if len(responses) >= threshold {
		return responses, nil
	}

	var lastSort interface{}

	for len(awsResult) > 0 {
		lastSort = awsResult[len(awsResult)-1].Sort[0]

		if len(responses) >= threshold {
			return responses, nil
		}

		awsResult, err = searchAllIndex(indexName, hostName, threshold, lastSort)

		if err != nil {
			logger.Fatalf("error during searching next page for index %s, last sort %+v, err %+v", indexName, lastSort, err)
			continue
		}

		responses = append(responses, awsResult...)
	}

	return responses, nil
}

func searchAllIndex(indexName string, hostName string, threshold int, lastSort interface{}) (docs []ResponseBodyDoc, err error) {
	reqBody := RequestBodySearch{
		Size: threshold,
		Sort: []RequestSortID{{CreatedAt: "asc"}},
	}
	if lastSort != nil {
		reqBody.SearchAfter = []interface{}{lastSort}
	}
	reqByte, err := json.Marshal(reqBody)
	if err != nil {
		return
	}
	postBody := bytes.NewBuffer(reqByte)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", hostName, indexName), postBody)
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

func getDocByID(indexName, docType, docID, routeKey string) (doc ResponseBodyDoc, err error) {
	routeParam := ""
	if routeKey != "" {
		routeParam = "?routing=" + routeKey
	}
	docType = url.QueryEscape(docType)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/%s/%s%s", destHostName, indexName, docType, docID, routeParam), nil)
	if err != nil {
		return doc, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	rawResp, err := client.Do(req)
	if err != nil {
		return doc, err
	}
	defer rawResp.Body.Close()

	bodyResult, err := ioutil.ReadAll(rawResp.Body)

	if err != nil {
		return doc, err
	}

	err = json.Unmarshal(bodyResult, &doc)
	if err != nil {
		return
	}

	logger.Infof("Successfully get document from index %s with id %+v, resp: %+v", indexName, docID, string(bodyResult))

	return doc, nil
}

func JsonBytesEqual(a, b []byte) (bool, error) {
	var j, j2 interface{}
	if err := json.Unmarshal(a, &j); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j), nil
}
