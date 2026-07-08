package schedule

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Reader fetches payloads from the scheduled topic by partition/offset.
type Reader struct {
	topic  string
	client *kgo.Client
	adm    *kadm.Client
}

func NewReader(brokers []string, topic string) (*Reader, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("kbatch-schedule-reader"),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, err
	}
	return &Reader{topic: topic, client: cl, adm: kadm.NewClient(cl)}, nil
}

type ReadResult struct {
	Found map[string][]byte
	Lost  []string
}

// Read loads payloads for partition→offsets map.
func (r *Reader) Read(ctx context.Context, byPartition map[int32][]int64) (ReadResult, error) {
	out := ReadResult{Found: map[string][]byte{}}
	if len(byPartition) == 0 {
		return out, nil
	}

	startOffs, err := r.adm.ListStartOffsets(ctx, r.topic)
	if err != nil {
		return out, fmt.Errorf("list start offsets: %w", err)
	}
	endOffs, err := r.adm.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return out, fmt.Errorf("list end offsets: %w", err)
	}

	for partition, offsets := range byPartition {
		if len(offsets) == 0 {
			continue
		}
		sorted := append([]int64(nil), offsets...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		low, _ := startOffs.Lookup(r.topic, partition)
		high, _ := endOffs.Lookup(r.topic, partition)

		want := make(map[int64]struct{})
		minOff := int64(-1)
		for _, off := range sorted {
			loc := BuildKey(partition, off)
			if off < low.Offset {
				out.Lost = append(out.Lost, loc)
				continue
			}
			if off >= high.Offset {
				continue
			}
			want[off] = struct{}{}
			if minOff < 0 || off < minOff {
				minOff = off
			}
		}
		if len(want) == 0 {
			continue
		}

		assign := map[string]map[int32]kgo.Offset{
			r.topic: {partition: kgo.NewOffset().At(minOff)},
		}
		r.client.AddConsumePartitions(assign)

		deadline := time.Now().Add(5 * time.Second)
		scanned := 0
		const scanSlack = 1000
		for time.Now().Before(deadline) && len(want) > 0 && scanned < scanSlack {
			fetches := r.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				return out, fmt.Errorf("poll scheduled topic: %v", errs[0].Err)
			}
			fetches.EachRecord(func(rec *kgo.Record) {
				if rec.Topic != r.topic || rec.Partition != partition {
					return
				}
				scanned++
				if _, ok := want[rec.Offset]; ok {
					key := BuildKey(partition, rec.Offset)
					out.Found[key] = append([]byte(nil), rec.Value...)
					delete(want, rec.Offset)
				}
			})
		}
	}

	return out, nil
}

func (r *Reader) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// ParseOffsets converts string map keys from tests.
func ParseOffsets(m map[string][]int64) map[int32][]int64 {
	out := map[int32][]int64{}
	for k, offs := range m {
		p, _ := strconv.Atoi(k)
		out[int32(p)] = offs
	}
	return out
}
