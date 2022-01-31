package data

import "encoding/json"

const EventTypeTask EventType = "task"

func NewTaskEvent(info TaskInfo) *TaskEvent {
	return &TaskEvent{
		Base: newEventBase(EventTypeTranscode, ""),
		Task: info,
	}
}

type TaskEvent struct {
	Base
	Task TaskInfo `json:"task"`
}

type TaskInfo struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Snapshot json.RawMessage `json:"snapshot"`
}
