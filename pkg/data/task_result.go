package data

const EventTypeTaskResult EventType = "task_result"

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
	Upload        *UploadTaskOutput        `json:"upload,omitempty"`
	Import        *UploadTaskOutput        `json:"import,omitempty"`
	Export        *ExportTaskOutput        `json:"export,omitempty"`
	Transcode     *TranscodeTaskOutput     `json:"transcode,omitempty"`
	TranscodeFile *TranscodeFileTaskOutput `json:"transcodeFile,omitempty"`
}

type TranscodeTaskOutput struct {
	Asset UploadTaskOutput `json:"asset,omitempty"`
}

type TranscodeFileTaskOutputPath struct {
	Path string `json:"path,omitempty"`
}

type TranscodeFileTaskOutput struct {
	BaseUrl string                        `json:"baseUrl,omitempty"`
	Hls     *TranscodeFileTaskOutputPath  `json:"hls,omitempty"`
	Mp4     []TranscodeFileTaskOutputPath `json:"mp4,omitempty"`
}

type UploadTaskOutput struct {
	VideoFilePath    string `json:"videoFilePath"`
	MetadataFilePath string `json:"metadataFilePath"`
	// This is livepeerAPI.AssetSpec but we don't want to depend on the whole pkg
	AssetSpec interface{} `json:"assetSpec"`
}

type ExportTaskOutput struct {
	IPFS     *IPFSExportInfo `json:"ipfs,omitempty"`
	Internal interface{}     `json:"internal,omitempty"`
}

type IPFSExportInfo struct {
	VideoFileCID   string `json:"videoFileCid"`
	NFTMetadataCID string `json:"nftMetadataCid,omitempty"`
}
