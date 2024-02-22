package command

import (
	"crm-es/pkg/logger"
)

func GetTask(taskID string) {
	result, err := getTaskStatus(taskID)
	if err != nil {
		logger.Fatalf("error get task statuses, err: %+v", err)
	}

	logger.Infof("Result for task (%s), result (%+v)", taskID, result)
}
