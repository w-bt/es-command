package command

import (
	"bytes"
	"crm-es/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func SpecificSnapshot() {
	aliases, err := getAllIndices()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}

	aliasesString := getAliasesString(aliases)

	result, err := snapshotDuplicates(aliasesString)
	if err != nil {
		logger.Fatalf("error snapshot all indices, err: %+v", err)
	}
	logger.Infof("success snapshot all indices, result: %s", string(result))
}

func getAliasesString(aliases []IndexAlias) string {
	var indices []string
	for _, item := range aliases {
		destIndex := fmt.Sprintf("migration_%+v_%s", item.Alias, getTimeFormat())
		indices = append(indices, destIndex)
	}

	return strings.Join(indices, ",")
}

func snapshotDuplicates(aliasesString string) ([]byte, error) {
	reqRaw := SnapShotReqBody{
		IgnoreUnavailable:  true,
		IncludeGlobalState: false,
		Indices:            aliasesString,
	}
	reqByte, err := json.Marshal(reqRaw)
	if err != nil {
		return []byte{}, err
	}
	postBody := bytes.NewBuffer(reqByte)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s/%s_%s", sourceHostName, sourceBucketName, sourceSnapshotName, getTimeFormat()), postBody)
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
