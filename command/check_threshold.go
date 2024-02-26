package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

const (
	CreatedAtOnly  = "created_at"
	UpdatedAtOnly  = "updated_at"
	OtherThreshold = "other"
)

type Threshold struct {
	CreatedAtOnly map[string]bool
	UpdatedAtOnly map[string]bool
	Other         map[string]bool
}

func CheckThreshold() {
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	result := checkThresholds(aliases)
	err = saveThresholdResponses(result)
	if err != nil {
		logger.Errorf("Error during saving responses, responses: %+v, err: %+v", aliases, err)
	}
}

func checkThresholds(aliases []IndexAlias) (threshold Threshold) {
	threshold.CreatedAtOnly = make(map[string]bool)
	threshold.UpdatedAtOnly = make(map[string]bool)
	threshold.Other = make(map[string]bool)
	for _, item := range aliases {
		resultBool, err := checkThreshold(item.Alias)
		if err != nil {
			logger.Errorf("Error during checking treshold, responses: err: %+v", err)
			continue
		}
		switch resultBool {
		case UpdatedAtOnly:
			threshold.UpdatedAtOnly[item.Alias] = true
		case CreatedAtOnly:
			threshold.CreatedAtOnly[item.Alias] = true
		case OtherThreshold:
			threshold.Other[item.Alias] = true
		}
	}

	return
}

func checkThreshold(index string) (string, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	postBody := bytes.NewBuffer(nil)
	logger.Infof(fmt.Sprintf("%s/%+v", sourceHostName, index))
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%+v", sourceHostName, index), postBody)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	jsonString := string(body)
	boolResult := strings.Contains(jsonString, "updated_at")
	if boolResult {
		return UpdatedAtOnly, nil
	}

	boolResult = strings.Contains(jsonString, "created_at")
	if boolResult {
		return CreatedAtOnly, nil
	}

	return OtherThreshold, nil
}

func checkThresholdAndOrganization(index string) (string, bool, error) {
	hasOrganizationID := false

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	postBody := bytes.NewBuffer(nil)
	logger.Infof(fmt.Sprintf("%s/%+v", sourceHostName, index))
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%+v", sourceHostName, index), postBody)
	if err != nil {
		return "", hasOrganizationID, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return "", hasOrganizationID, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", hasOrganizationID, err
	}

	jsonString := string(body)
	boolResult := strings.Contains(jsonString, "organization_id")
	if boolResult {
		hasOrganizationID = true
	}

	boolResult = strings.Contains(jsonString, "updated_at")
	if boolResult {
		return UpdatedAtOnly, hasOrganizationID, nil
	}

	boolResult = strings.Contains(jsonString, "created_at")
	if boolResult {
		return CreatedAtOnly, hasOrganizationID, nil
	}

	return OtherThreshold, hasOrganizationID, nil
}

func saveThresholdResponses(threshold Threshold) error {
	file, err := json.MarshalIndent(threshold, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("threshold.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
