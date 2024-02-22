package command

import (
	"crm-es/pkg/logger"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

func GetTaskAll() {
	tasks, err := getTaskIDs()
	if err != nil {
		logger.Fatalf("error get all task ID, err: %+v", err)
	}
	err = getTaskStatuses(tasks)
	if err != nil {
		logger.Fatalf("error get task statuses, err: %+v", err)
	}
}

func getTaskIDs() (tasks []ReindexResp, err error) {
	fileBytes, err := os.ReadFile("reindex_response.json")
	if err != nil {
		return
	}
	err = json.Unmarshal(fileBytes, &tasks)
	if err != nil {
		return
	}

	return
}

func getTaskStatuses(tasks []ReindexResp) error {
	for _, task := range tasks {
		result, err := getTaskStatus(task.Task)
		if err != nil {
			logger.Fatalf("error get task status for index %s task_id %s, err: %+v", task.Index, task.Task, err)
			continue
		}

		logger.Infof("task status for index %s with task id %s:\n%s", task.Index, task.Task, result)
	}

	return nil
}

func getTaskStatus(taskID string) (result string, err error) {
	resp, err := http.Get(fmt.Sprintf("%s/_tasks/%s", sourceHostName, taskID))
	if err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	return string(body), nil
}
