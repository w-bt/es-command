package command

import (
	"crm-es/pkg/logger"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type Progress struct {
	Index              string `json:"index"`
	Shard              string `json:"shard"`
	Snapshot           string `json:"snapshot"`
	FilesPercent       string `json:"files_percent"`
	TranslogOpsPercent string `json:"translog_ops_percent"`
}

func Recovery(name string) {
	progresses, err := getRecovery()
	if err != nil {
		logger.Fatalf("error get recovery progress, err: %+v", err)
	}

	done, inProgress := filterProgress(progresses, name)
	total := len(done) + len(inProgress)
	logger.Infof("total done: %d of %d, in_progress: %d of %d", len(done), total, len(inProgress), total)

	err = saveRecoveryResponses(done, inProgress)
	if err != nil {
		logger.Errorf("Error during saving recovery, responses: %+v, err: %+v", done, inProgress, err)
	}
}

func getRecovery() (progresses []Progress, err error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/_cat/recovery?pretty&format=json", destHostName), nil)
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(body, &progresses)
	if err != nil {
		logger.Infof("here %+v", string(body))
		return
	}

	return progresses, nil
}

func filterProgress(progresses []Progress, name string) (done []Progress, inProgress []Progress) {
	for _, item := range progresses {
		if item.Snapshot == name {
			if item.FilesPercent == "100.0%" && item.TranslogOpsPercent == "100.0%" {
				done = append(done, item)
			} else {
				inProgress = append(inProgress, item)
			}
		}
	}

	return
}

func saveRecoveryResponses(done, inProgress []Progress) error {
	file, err := json.MarshalIndent(done, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("recovery_done.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	fileInProgress, err := json.MarshalIndent(inProgress, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("recovery_in_progress.json", fileInProgress, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
