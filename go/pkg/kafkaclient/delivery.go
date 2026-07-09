package kafkaclient

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Delivery holds broker-assigned coordinates after a sync produce.
type Delivery struct {
	Partition int32
	Offset    int64
}

// ProduceSync produces one record and returns delivery coordinates.
func (c *Client) ProduceSync(ctx context.Context, topic, key string, payload []byte, partition *int32) (Delivery, error) {
	r := kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	if partition != nil && *partition >= 0 {
		r.Partition = *partition
	}
	results := c.inner.ProduceSync(ctx, &r)
	for _, res := range results {
		if res.Err != nil {
			return Delivery{}, res.Err
		}
		if res.Record != nil {
			return Delivery{Partition: res.Record.Partition, Offset: res.Record.Offset}, nil
		}
	}
	return Delivery{}, nil
}

// ProduceManySync pipelines multiple records; returns one delivery per input.
func (c *Client) ProduceManySync(ctx context.Context, records []ProduceRecord) ([]Delivery, error) {
	if len(records) == 0 {
		return nil, nil
	}
	krecs := make([]*kgo.Record, len(records))
	for i, pr := range records {
		r := &kgo.Record{Topic: pr.Topic, Key: []byte(pr.Key), Value: pr.Payload}
		if pr.Partition != nil && *pr.Partition >= 0 {
			r.Partition = *pr.Partition
		}
		krecs[i] = r
	}
	results := c.inner.ProduceSync(ctx, krecs...)
	out := make([]Delivery, 0, len(results))
	for _, res := range results {
		if res.Err != nil {
			return out, res.Err
		}
		d := Delivery{}
		if res.Record != nil {
			d.Partition = res.Record.Partition
			d.Offset = res.Record.Offset
		}
		out = append(out, d)
	}
	return out, nil
}

// ProduceRecord is one message for bulk produce.
type ProduceRecord struct {
	Topic     string
	Key       string
	Payload   []byte
	Partition *int32
}
