package schedule

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MysqlStore implements the delayed-job index on kafka_batch_scheduled_jobs.
type MysqlStore struct {
	db    *sql.DB
	limit int
}

func NewMysqlStore(dsn string, readMissLimit int) (*MysqlStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql schedule store ping: %w", err)
	}
	if readMissLimit <= 0 {
		readMissLimit = 500
	}
	return &MysqlStore{db: db, limit: readMissLimit}, nil
}

func (s *MysqlStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *MysqlStore) Schedule(ctx context.Context, jobID string, runAt time.Time, partition int32, offset int64, batchID string) error {
	var bid interface{}
	if batchID != "" {
		bid = batchID
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO kafka_batch_scheduled_jobs
  (job_id, run_at, partition_id, kafka_offset, batch_id, lease_until, created_at)
VALUES (?, ?, ?, ?, ?, NULL, ?)
ON DUPLICATE KEY UPDATE
  run_at = VALUES(run_at),
  partition_id = VALUES(partition_id),
  kafka_offset = VALUES(kafka_offset),
  batch_id = VALUES(batch_id),
  lease_until = NULL`,
		jobID, runAt.UTC(), partition, offset, bid, time.Now().UTC(),
	)
	return err
}

// ScheduleMany bulk-inserts delayed-job rows (Ruby schedule_many).
func (s *MysqlStore) ScheduleMany(ctx context.Context, entries []ScheduleEntry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	stmt := `
INSERT INTO kafka_batch_scheduled_jobs
  (job_id, run_at, partition_id, kafka_offset, batch_id, lease_until, created_at)
VALUES (?, ?, ?, ?, ?, NULL, ?)
ON DUPLICATE KEY UPDATE
  run_at = VALUES(run_at),
  partition_id = VALUES(partition_id),
  kafka_offset = VALUES(kafka_offset),
  batch_id = VALUES(batch_id),
  lease_until = NULL`
	for _, e := range entries {
		var bid interface{}
		if e.BatchID != "" {
			bid = e.BatchID
		}
		if _, err := tx.ExecContext(ctx, stmt, e.JobID, e.RunAt.UTC(), e.Partition, e.Offset, bid, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MysqlStore) ClaimDue(ctx context.Context, now time.Time, leaseSeconds, limit int) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
SELECT job_id, partition_id, kafka_offset
FROM kafka_batch_scheduled_jobs
WHERE run_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
ORDER BY run_at
LIMIT ?
FOR UPDATE SKIP LOCKED`, now.UTC(), now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	type row struct {
		jobID     string
		partition int32
		offset    int64
	}
	var claimed []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.jobID, &r.partition, &r.offset); err != nil {
			rows.Close()
			return nil, err
		}
		claimed = append(claimed, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		return nil, tx.Commit()
	}

	leaseUntil := now.Add(time.Duration(leaseSeconds) * time.Second).UTC()
	for _, r := range claimed {
		if _, err := tx.ExecContext(ctx,
			`UPDATE kafka_batch_scheduled_jobs SET lease_until = ? WHERE job_id = ?`,
			leaseUntil, r.jobID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(claimed))
	for _, r := range claimed {
		out = append(out, BuildMember(r.jobID, r.partition, r.offset))
	}
	return out, nil
}

func (s *MysqlStore) Ack(ctx context.Context, members []string) error {
	if len(members) == 0 {
		return nil
	}
	for _, m := range members {
		pm, ok := ParseMember(m)
		if !ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM kafka_batch_scheduled_jobs WHERE job_id = ?`, pm.JobID); err != nil {
			return err
		}
	}
	return nil
}

func (s *MysqlStore) Reclaim(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE kafka_batch_scheduled_jobs SET lease_until = NULL WHERE lease_until IS NOT NULL AND lease_until <= ?`,
		now.UTC())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *MysqlStore) RecordReadMiss(ctx context.Context, member string) (int64, error) {
	// MySQL store has no read-miss counter; poller drops via retention path only.
	return 1, nil
}

func (s *MysqlStore) ClearReadMiss(ctx context.Context, member string) error {
	return nil
}
