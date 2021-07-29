package event

import (
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var ByteCapacity = stream.ByteCapacity{}
var OffsetSpec = stream.OffsetSpecification{}

type RawStreamOptions struct {
	MaxLengthBytes      string
	MaxSegmentSizeBytes string
	MaxAge              time.Duration
}

func ParseStreamOptions(raw RawStreamOptions) (*stream.StreamOptions, error) {
	return &stream.StreamOptions{
		MaxLengthBytes:      ByteCapacity.From(raw.MaxLengthBytes),
		MaxSegmentSizeBytes: ByteCapacity.From(raw.MaxSegmentSizeBytes),
		MaxAge:              raw.MaxAge,
	}, nil
}

func NewConsumerOptions(name string, offset stream.OffsetSpecification) *stream.ConsumerOptions {
	return stream.NewConsumerOptions().SetConsumerName(name).SetOffset(offset)
}

func TimestampOffset(t time.Time) stream.OffsetSpecification {
	return OffsetSpec.Timestamp(t.UnixNano() / 1e6)
}
