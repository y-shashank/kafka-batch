package priority

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// LagChecker reports whether a consumer group has backlog on topics.
type LagChecker interface {
	GroupHasLag(ctx context.Context, group string, topics []string) (bool, error)
}

// LagReader checks consumer-group lag for topics.
type LagReader struct {
	adm *kadm.Client
}

func NewLagReader(cl *kgo.Client) *LagReader {
	return &LagReader{adm: kadm.NewClient(cl)}
}

// GroupHasLag returns true when any listed topic has positive lag for the group.
func (l *LagReader) GroupHasLag(ctx context.Context, group string, topics []string) (bool, error) {
	if len(topics) == 0 {
		return false, nil
	}
	lags, err := l.adm.Lag(ctx, group)
	if err != nil {
		return false, err
	}
	gl, ok := lags[group]
	if !ok {
		return false, nil
	}
	if err := gl.Error(); err != nil {
		return false, err
	}
	for _, topic := range topics {
		for _, part := range gl.Lag[topic] {
			if part.Lag > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

// Gate caches lag checks (mirrors Ruby PriorityGate).
type Gate struct {
	Reader        LagChecker
	Interval      time.Duration
	Consumption   TopicPauseFilter
	ConsumerGroup string

	mu         sync.Mutex
	lastCheck  time.Time
	lastResult bool
}

// TopicPauseFilter excludes topic-level-paused topics from lag gating.
type TopicPauseFilter interface {
	ActiveHigherTopics(ctx context.Context, group string, higher []string) []string
}

func NewGate(reader LagChecker, interval time.Duration) *Gate {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Gate{Reader: reader, Interval: interval}
}

// HigherTopicsHaveLag returns true when higher-ranked topics still have backlog.
// On error, returns the last successful result (fail open when never checked).
func (g *Gate) HigherTopicsHaveLag(ctx context.Context, group string, topics []string, force bool) bool {
	topics = g.filterHigher(ctx, group, topics)
	if len(topics) == 0 {
		return false
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if !force && !g.lastCheck.IsZero() && now.Sub(g.lastCheck) < g.Interval {
		return g.lastResult
	}
	hasLag, err := g.Reader.GroupHasLag(ctx, group, topics)
	if err != nil {
		if g.lastCheck.IsZero() {
			return false
		}
		return g.lastResult
	}
	g.lastCheck = now
	g.lastResult = hasLag
	return hasLag
}

func (g *Gate) filterHigher(ctx context.Context, group string, topics []string) []string {
	if g.Consumption != nil {
		return g.Consumption.ActiveHigherTopics(ctx, group, topics)
	}
	return topics
}

// ShouldYield decides whether a lower-ranked message should pause (Ruby PriorityJobConsumer).
func ShouldYield(spec TopicSpec, gate *Gate, weightedTick *int, ctx context.Context) (bool, error) {
	if spec.Rank == 0 || len(spec.HigherTopics) == 0 {
		return false, nil
	}
	if !gate.HigherTopicsHaveLag(ctx, spec.ConsumerGroup, spec.HigherTopics, false) {
		return false, nil
	}
	switch spec.Mode {
	case ModeStrict:
		return true, nil
	case ModeWeighted:
		every := spec.WeightedInterleave
		if every < 1 {
			every = 4
		}
		*weightedTick++
		if (*weightedTick % every) != 0 {
			return true, nil
		}
		return false, nil
	default:
		return true, nil
	}
}

// YieldError is returned to skip committing a message while yielding to higher priority.
type YieldError struct {
	Spec TopicSpec
}

func (e YieldError) Error() string {
	return fmt.Sprintf("priority yield rank=%d group=%s", e.Spec.Rank, e.Spec.ConsumerGroup)
}
