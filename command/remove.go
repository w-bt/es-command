package command

import (
	"crm-es/pkg/logger"
)

func Remove(indexName string) {
	result, err := removeIndex(indexName)
	if err != nil {
		logger.Fatalf("error remove index :%s, err: %+v", indexName, err)
	}

	logger.Infof("Result for new index (%s) deletion, result (%+v)", indexName, string(result))
}
