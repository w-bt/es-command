package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type SnapShotReqBody struct {
	IgnoreUnavailable  bool   `json:"ignore_unavailable"`
	IncludeGlobalState bool   `json:"include_global_state"`
	Indices            string `json:"indices,omitempty"`
}

func InitialSnapshotAll() {
	result, err := snapshotIndices()
	if err != nil {
		logger.Fatalf("error snapshot all indices, err: %+v", err)
	}
	logger.Infof("success snapshot all indices, result: %s", string(result))
}

func snapshotIndices() ([]byte, error) {
	reqRaw := SnapShotReqBody{
		IgnoreUnavailable:  true,
		IncludeGlobalState: false,
	}
	reqByte, err := json.Marshal(reqRaw)
	if err != nil {
		return []byte{}, err
	}
	postBody := bytes.NewBuffer(reqByte)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s/%s", sourceHostName, sourceBucketName, sourceSnapshotName), postBody)
	if err != nil {
		return []byte{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()

	bodyResult, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}

	return bodyResult, nil
}
