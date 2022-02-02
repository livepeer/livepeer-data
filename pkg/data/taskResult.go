package data

const EventTypeTaskResult EventType = "task"

func NewTaskResultEvent(info TaskInfo, err *ErrorInfo, output *TaskOutput) *TaskResultEvent {
	return &TaskResultEvent{
		Base:   newEventBase(EventTypeTaskResult, ""),
		Task:   info,
		Error:  err,
		Output: output,
	}
}

type TaskResultEvent struct {
	Base
	Task   TaskInfo    `json:"task"`
	Error  *ErrorInfo  `json:"error,omitempty"`
	Output *TaskOutput `json:"output,omitempty"`
}

type ErrorInfo struct {
	Message     string `json:"message"`
	Unretriable bool   `json:"unretriable"`
}

type TaskOutput struct {
	Import interface{} `json:"import,omitempty"`
}
