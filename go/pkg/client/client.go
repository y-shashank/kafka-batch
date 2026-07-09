package client

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/schedule"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
	"github.com/y-shashank/kafka-batch/go/pkg/uniq"
)

// Client is the Go produce API (Ruby KafkaBatch::Batch).
type Client struct {
	cfg      Config
	manifest config.Manifest
	store    *store.RedisStore
	sched    *schedule.RedisStore
	prod     *kafkaclient.Client
	uniq     *uniq.Locker
	rdb      *redis.Client
}

// New connects to Kafka and Redis and loads the handler manifest.
func New(cfg Config) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ConfigurationError{Message: "brokers are required"}
	}
	if cfg.RedisURL == "" {
		return nil, ConfigurationError{Message: "redis_url is required"}
	}
	manifest, err := config.LoadManifest(cfg.ManifestPath, cfg.TopicPrefix)
	if err != nil {
		return nil, err
	}
	rOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(rOpts)
	c := &Client{
		cfg:      cfg,
		manifest: manifest,
		store:    store.NewRedisStore(rdb, cfg.BatchTTL),
		sched:    schedule.NewRedisStore(rdb, 500),
		uniq:     uniq.NewLocker(rdb, cfg.UniqLockTTL),
		rdb:      rdb,
	}
	if err := c.validateManifest(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	if err := pingRedis(context.Background(), rdb); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	prod, err := kafkaclient.New(cfg.Brokers)
	if err != nil {
		_ = rdb.Close()
		return nil, err
	}
	c.prod = prod
	return c, nil
}

// Close releases Redis and Kafka connections.
func (c *Client) Close() {
	if c.prod != nil {
		c.prod.Close()
		c.prod = nil
	}
	if c.rdb != nil {
		_ = c.rdb.Close()
		c.rdb = nil
	}
}

// CreateBatch persists a new batch. When populate is non-nil the batch stays
// unsealed until populate returns (block form, Ruby Batch.create with block).
func (c *Client) CreateBatch(ctx context.Context, opts BatchOptions, populate func(*Batch) error) (*Batch, error) {
	id := opts.ID
	if id == "" {
		id = uuid.NewString()
	}
	sealed := populate == nil
	created, err := c.store.CreateBatch(ctx, store.CreateBatchParams{
		ID: id, TotalJobs: 0,
		OnSuccess: opts.OnSuccess, OnComplete: opts.OnComplete,
		Meta: opts.Meta, Description: opts.Description,
		TenantID: opts.TenantID, Sealed: sealed,
	})
	if err != nil {
		return nil, err
	}
	if !created {
		return nil, BatchExistsError{BatchID: id}
	}
	instrument.BatchCreated(id, opts.Description, opts.TenantID, opts.OnSuccess, opts.OnComplete)

	b := &Batch{
		client: c, id: id,
		onSuccess: opts.OnSuccess, onComplete: opts.OnComplete,
		meta: opts.Meta, description: opts.Description, tenantID: opts.TenantID,
	}
	if populate != nil {
		popErr := populate(b)
		_, sealErr := b.Seal(ctx)
		if popErr != nil {
			return b, popErr
		}
		if sealErr != nil {
			return b, sealErr
		}
	}
	return b, nil
}

// OpenBatch re-attaches to an existing batch ledger row.
func (c *Client) OpenBatch(ctx context.Context, id string) (*Batch, error) {
	row, err := c.store.FindBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, BatchNotFoundError{BatchID: id}
	}
	meta := map[string]interface{}{}
	if row.Meta != "" {
		_ = json.Unmarshal([]byte(row.Meta), &meta)
	}
	return &Batch{
		client: c, id: id,
		onSuccess: row.OnSuccess, onComplete: row.OnComplete,
		meta: meta, description: row.Description, tenantID: row.TenantID,
	}, nil
}

// EnqueueJob enqueues a standalone manifest job immediately.
func (c *Client) EnqueueJob(ctx context.Context, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	entry, err := c.lookupHandler(jobType)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := c.claimUniq(ctx, entry, jobType, payload, jobID, ""); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	msg, err := c.buildMessage(entry, jobType, payload, jobID, nil, opts, nil)
	if err != nil {
		c.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	route := c.routeFor(entry, jobID, opts.tenantID(""), nil)
	if err := c.produceJob(ctx, route, msg); err != nil {
		c.releaseUniq(entry, jobType, payload, jobID, msg.UniqFP)
		return "", err
	}
	return jobID, nil
}

// EnqueueJobAt schedules a standalone manifest job.
func (c *Client) EnqueueJobAt(ctx context.Context, runAt interface{}, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	entry, err := c.lookupHandler(jobType)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := c.claimUniq(ctx, entry, jobType, payload, jobID, ""); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	msg, err := c.buildMessage(entry, jobType, payload, jobID, nil, opts, nil)
	if err != nil {
		c.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	if err := c.scheduleMessage(ctx, msg, clampRunAt(runAt, c.cfg.MaxScheduleHorizon), ""); err != nil {
		c.releaseUniq(entry, jobType, payload, jobID, msg.UniqFP)
		return "", err
	}
	return jobID, nil
}

func (c *Client) lookupHandler(jobType string) (config.HandlerEntry, error) {
	entry, ok := c.manifest.Handlers[jobType]
	if !ok {
		return entry, UnknownHandlerError{JobType: jobType}
	}
	return entry, nil
}

func (c *Client) produceCallback(ctx context.Context, batch *store.Batch, outcome string) error {
	meta := map[string]interface{}{}
	if batch.Meta != "" {
		_ = json.Unmarshal([]byte(batch.Meta), &meta)
	}
	cb := protocol.CallbackMessage{
		BatchID: batch.ID, Outcome: outcome,
		TotalJobs: batch.TotalJobs, CompletedCount: batch.CompletedCount,
		FailedCount: batch.FailedCount,
		OnSuccess: batch.OnSuccess, OnComplete: batch.OnComplete,
		FinishedAt: batch.FinishedAt,
		Meta: meta,
	}
	if cb.FinishedAt == "" {
		cb.FinishedAt = protocol.NowISO()
	}
	raw, _ := json.Marshal(cb)
	topic := c.cfg.resolveTopic(c.cfg.CallbacksTopic)
	return c.prod.Produce(ctx, topic, batch.ID, raw)
}
