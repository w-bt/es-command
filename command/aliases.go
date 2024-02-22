package command

import (
	"crm-es/pkg/logger"
	"encoding/json"
	"io/ioutil"
	"os"
)

func GetAliases() {
	aliases, err := getAllIndices()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}
	err = saveAliasesResponses(aliases)
	if err != nil {
		logger.Errorf("Error during saving responses, responses: %+v, err: %+v", aliases, err)
	}
}

func saveAliasesResponses(aliases []IndexAlias) error {
	file, err := json.MarshalIndent(aliases, "", " ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("aliases.json", file, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}
