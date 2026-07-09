package client

import (
	"context"

	"github.com/y-shashank/kafka-batch/go/pkg/schedule"
)

// scheduleIndex writes delayed-job pointers (Redis or MySQL).
type scheduleIndex interface {
	scheduleOne(ctx context.Context, e schedule.ScheduleEntry) error
	scheduleMany(ctx context.Context, entries []schedule.ScheduleEntry) error
}

type redisScheduleIndex struct {
	inner *schedule.RedisStore
}

func (r redisScheduleIndex) scheduleOne(ctx context.Context, e schedule.ScheduleEntry) error {
	return r.inner.Schedule(ctx, e.JobID, e.RunAt, e.Partition, e.Offset)
}

func (r redisScheduleIndex) scheduleMany(ctx context.Context, entries []schedule.ScheduleEntry) error {
	return r.inner.ScheduleMany(ctx, entries)
}

type mysqlScheduleIndex struct {
	inner *schedule.MysqlStore
}

func (m mysqlScheduleIndex) scheduleOne(ctx context.Context, e schedule.ScheduleEntry) error {
	return m.inner.Schedule(ctx, e.JobID, e.RunAt, e.Partition, e.Offset, e.BatchID)
}

func (m mysqlScheduleIndex) scheduleMany(ctx context.Context, entries []schedule.ScheduleEntry) error {
	return m.inner.ScheduleMany(ctx, entries)
}

func openScheduleIndex(cfg Config) (scheduleIndex, *schedule.MysqlStore, error) {
	switch cfg.ScheduleStore {
	case "", "redis":
		return nil, nil, nil
	case "mysql":
		if cfg.ScheduleMySQLDSN == "" {
			return nil, nil, ConfigurationError{Message: "schedule_mysql_dsn required when schedule_store is mysql"}
		}
		ms, err := schedule.NewMysqlStore(cfg.ScheduleMySQLDSN, 500)
		if err != nil {
			return nil, nil, err
		}
		return mysqlScheduleIndex{inner: ms}, ms, nil
	default:
		return nil, nil, ConfigurationError{Message: "schedule_store must be redis or mysql"}
	}
}
