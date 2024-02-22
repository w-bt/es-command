package command

import (
	"crm-es/pkg/logger"
	"strings"
)

func Print() {
	aliases, err := getAliasesFromFile()
	if err != nil {
		logger.Fatalf("error get all indices, err: %+v", err)
	}
	items := []string{}
	for _, alias := range aliases {
		items = append(items, alias.Alias)
	}
	itemStr := strings.Join(items, ",")
	logger.Infof(itemStr)
}
