package data

const EventTypeTaskResultPartial EventType = "task_result_partial"

func NewTaskResultPartialEvent(info TaskInfo, output *TaskPartialOutput) *TaskResultPartialEvent {
	return &TaskResultPartialEvent{
		Base:   newEventBase(EventTypeTaskResultPartial, ""),
		Task:   info,
		Output: output,
	}
}

type TaskResultPartialEvent struct {
	Base
	Task   TaskInfo           `json:"task"`
	Output *TaskPartialOutput `json:"output,omitempty"`
}

type TaskPartialOutput struct {
	Upload *UploadTaskOutput `json:"upload,omitempty"`
}
