package data

import "encoding/json"

const EventTypeTaskTrigger EventType = "task_trigger"

func NewTaskTriggerEvent(info TaskInfo) *TaskTriggerEvent {
	return &TaskTriggerEvent{
		Base: newEventBase(EventTypeTaskTrigger, ""),
		Task: info,
	}
}

type TaskTriggerEvent struct {
	Base
	Task TaskInfo `json:"task"`
}

type TaskInfo struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Snapshot json.RawMessage `json:"snapshot"`
}
