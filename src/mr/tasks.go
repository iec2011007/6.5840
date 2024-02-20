package mr

import (
	"fmt"
	"log"
	"time"
)

type TaskScheduler struct {
	Tasks []Task
}

const TASK_TIMEOUT time.Duration = 10 * time.Second

type TaskStatus int

const (
	PENDING = iota
	IN_PROGRESS
	COMPLETED
)

type Task struct {
	name         string
	status       TaskStatus
	assignedTime time.Time
}

func (task Task) String() string {
	return fmt.Sprintf("{name:%v, status: %v, assignedTime: %v}", task.name, task.status, task.assignedTime)
}

func NewTask(taskName string) Task {
	return Task{name: taskName, status: PENDING, assignedTime: time.Now()}
}

func NewTaskScheduler(files []string) *TaskScheduler {
	tasksArr := make([]Task, len(files))
	ts := TaskScheduler{Tasks: tasksArr}
	for idx, fName := range files {
		task := NewTask(fName)
		tasksArr[idx] = task
	}
	return &ts
}

/*
*
AssignAvailableTask returns any pending tasks or any task running for more than 10 second (TIMEOUT).
If no such task is available then it returns empty path.
Usage check if task is available then use the response of fileName
*/
func (ts TaskScheduler) AssignAvailableTask() (fileName string, available bool) {
	for _, task := range ts.Tasks {
		if task.status == PENDING || isTaskStuck(task) {
			// This task can be reassigned
			task.status = IN_PROGRESS
			task.assignedTime = time.Now()
			log.Printf("assigning task: %v", task)
			return task.name, true
		}
	}
	return "", false
}

func isTaskStuck(task Task) bool {
	durationCheck := time.Duration(time.Now().Sub(task.assignedTime).Seconds()) > TASK_TIMEOUT
	statusCheck := task.status == IN_PROGRESS
	return durationCheck && statusCheck
}
