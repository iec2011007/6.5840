package mr

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type TaskTracker struct {
	MapTasks []Task
}

const TASK_TIMEOUT time.Duration = 10 * time.Second

type TaskStatus int

const (
	PENDING = iota
	IN_PROGRESS
	COMPLETED
)

type Task struct {
	id           int
	name         string
	status       TaskStatus
	assignedTime time.Time
}

func (task Task) String() string {
	return fmt.Sprintf("{id: %v, name:%v, status: %v, assignedTime: %v}", task.id, task.name, task.status, task.assignedTime)
}

// Optimization: we can short circuit. Kept it this way for readability
func (tracker TaskTracker) AllMapTasksCompleted() bool {
	allCompleted := true
	for _, val := range tracker.MapTasks {
		allCompleted = allCompleted && val.status == COMPLETED
		log.Printf("from AllMapTasksCompleted %v, allCompleted: %v, check: %v", val, allCompleted, val.status == COMPLETED)
	}
	log.Println()
	return allCompleted
}

func (tracker TaskTracker) AllReduceTasksCompleted() bool {
	return true
}

func NewTask(taskName string, id int) Task {
	return Task{id: id, name: taskName, status: PENDING, assignedTime: time.Unix(0, 0)}
}

func NewTaskScheduler(files []string) *TaskTracker {
	tasksArr := make([]Task, len(files))
	ts := TaskTracker{MapTasks: tasksArr}
	for idx, fName := range files {
		task := NewTask(fName, idx)
		tasksArr[idx] = task
	}
	return &ts
}

func (tracker *TaskTracker) UpdateMapTaskCompletion(taskId int) bool {
	for i := 0; i < len(tracker.MapTasks); i++ {
		task := tracker.MapTasks[i]
		if task.id == taskId {
			task.status = COMPLETED
			tracker.MapTasks[i] = task
			return true
		}
	}
	return false
}

/*
*
AssignAvailableTask returns any pending tasks or any task running for more than 10 second (TIMEOUT).
If no such task is available then it returns empty path.
Usage check if task is available then use the response of fileName
*/
func (ts *TaskTracker) AssignAvailableTask() (Task, error) {
	log.Println(ts.MapTasks)
	// https://www.calhoun.io/does-range-copy-the-slice-in-go/
	for i := 0; i < len(ts.MapTasks); i++ {
		task := ts.MapTasks[i]
		if task.status == PENDING || isTaskStuck(task) {
			// This task can be reassigned
			task.status = IN_PROGRESS
			task.assignedTime = time.Now()
			log.Printf("assigning task: %v", task)
			ts.MapTasks[i] = task
			return task, nil
		}
	}
	return NewTask("", 0), errors.New("no available task")
}

func isTaskStuck(task Task) bool {
	durationCheck := task.assignedTime != time.Unix(0, 0) && time.Duration(time.Since(task.assignedTime).Seconds()) > TASK_TIMEOUT
	statusCheck := task.status == IN_PROGRESS
	// log.Printf("durationCheck: %v statusCheck: %v", durationCheck, statusCheck)
	return durationCheck && statusCheck
}
