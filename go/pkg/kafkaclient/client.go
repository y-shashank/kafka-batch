package kafkaclient

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client wraps franz-go for produce + consume.
type Client struct {
	inner *kgo.Client
}

func New(brokers []string, opts ...kgo.Opt) (*Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	}
	inner, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, err
	}
	return &Client{inner: inner}, nil
}

func (c *Client) Close() {
	c.inner.Close()
}

func (c *Client) Produce(ctx context.Context, topic, key string, payload []byte) error {
	r := kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	results := c.inner.ProduceSync(ctx, &r)
	for _, res := range results {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

func (c *Client) Poll(ctx context.Context, topics []string, group string, handler func(*kgo.Record) error) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	// re-use seed brokers from existing client is tricky; daemon passes full opts
	_ = opts
	return fmt.Errorf("use daemon.Run with integrated consumer loops")
}

// Inner exposes the raw client for the daemon.
func (c *Client) Inner() *kgo.Client { return c.inner }
