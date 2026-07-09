package daemon

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/control/job"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// BuildJobHandler returns the shared plain/fair-ready job consumer callback.
func BuildJobHandler(cfg config.Daemon, prod *kafkaclient.Client, jobProc *job.Processor) func(*kgo.Record) error {
	return func(rec *kgo.Record) error {
		src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
		out, err := jobProc.Process(context.Background(), rec.Value, src)
		if err != nil {
			return err
		}
		return applyJobOutcome(context.Background(), cfg, prod, out)
	}
}
