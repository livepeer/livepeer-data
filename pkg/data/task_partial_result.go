package data

const EventTypeTaskResultPartial EventType = "task_result_partial"

func NewTaskResultPartialEvent(info TaskInfo, err *ErrorInfo, output *TaskOutput) *TaskResultPartialEvent {
	return &TaskResultPartialEvent{
		Base:   newEventBase(EventTypeTaskResultPartial, ""),
		Task:   info,
		Error:  err,
		Output: output,
	}
}

type TaskResultPartialEvent TaskResultEvent
