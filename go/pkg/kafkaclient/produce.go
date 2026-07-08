package kafkaclient

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducePartition sends to an explicit partition (partition < 0 lets the broker assign).
func (c *Client) ProducePartition(ctx context.Context, topic, key string, payload []byte, partition int32) error {
	r := kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	if partition >= 0 {
		r.Partition = partition
	}
	results := c.inner.ProduceSync(ctx, &r)
	for _, res := range results {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}
