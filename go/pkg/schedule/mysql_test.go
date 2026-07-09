package schedule

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestMysqlScheduleManyWithBatchID(t *testing.T) {
	dsn := os.Getenv("KAFKA_BATCH_SCHEDULE_MYSQL_DSN")
	if dsn == "" {
		t.Skip("KAFKA_BATCH_SCHEDULE_MYSQL_DSN not set")
	}
	st, err := NewMysqlStore(dsn, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	runAt := time.Now().Add(time.Hour)
	entries := []ScheduleEntry{
		{JobID: "j1", RunAt: runAt, Partition: 0, Offset: 10, BatchID: "batch-1"},
		{JobID: "j2", RunAt: runAt, Partition: 1, Offset: 11},
	}
	if err := st.ScheduleMany(ctx, entries); err != nil {
		t.Fatal(err)
	}
	if err := st.Schedule(ctx, "j3", runAt, 2, 12, "batch-2"); err != nil {
		t.Fatal(err)
	}
}
