package job

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/fairness"
)

var errFairSkipped = errors.New("fair slot already executed")

type fairMeta struct {
	slot     bool
	slotID   string
	tenantID string
	lane     fairness.Lane
}

func parseFairMeta(raw []byte) fairMeta {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return fairMeta{}
	}
	fm := fairMeta{}
	if v, ok := m["_fair_slot"].(bool); ok {
		fm.slot = v
	}
	if s, ok := m["_fair_slot_id"].(string); ok {
		fm.slotID = s
	}
	if t, ok := m["tenant_id"].(string); ok {
		fm.tenantID = t
	}
	if ty, ok := m["_fair_type"].(string); ok && ty != "" {
		fm.lane = fairness.Lane(ty)
	} else {
		fm.lane = fairness.LaneTime
	}
	return fm
}

func (p *Processor) withFairSlot(ctx context.Context, raw []byte, perform func() error) error {
	sched := p.fairScheduler(raw)
	if sched == nil {
		return perform()
	}
	fm := parseFairMeta(raw)
	if !fm.slot || fm.slotID == "" {
		return perform()
	}
	tenant := fm.tenantID
	if tenant == "" {
		tenant = "default"
	}
	claimed, err := sched.ClaimSlotExecution(ctx, fm.slotID)
	if err != nil {
		return err
	}
	if !claimed {
		return errFairSkipped
	}
	start := p.now()
	renewStop := make(chan struct{})
	go p.renewFairLease(renewStop, tenant, fm.slotID, sched)
	defer close(renewStop)

	performErr := perform()
	dur := p.now().Sub(start).Seconds()
	_ = sched.Complete(ctx, tenant, fm.slotID, dur)
	return performErr
}

func (p *Processor) fairScheduler(raw []byte) *fairness.Scheduler {
	fm := parseFairMeta(raw)
	if fm.lane == fairness.LaneThroughput {
		return p.FairThroughput
	}
	return p.FairTime
}

func (p *Processor) releaseFairSlotIfHeld(ctx context.Context, raw []byte) {
	fm := parseFairMeta(raw)
	if !fm.slot || fm.slotID == "" {
		return
	}
	sched := p.fairScheduler(raw)
	if sched == nil {
		return
	}
	tenant := fm.tenantID
	if tenant == "" {
		tenant = "default"
	}
	_ = sched.Complete(ctx, tenant, fm.slotID, 0)
}

func (p *Processor) renewFairLease(stop <-chan struct{}, tenantID, slotID string, sched *fairness.Scheduler) {
	if sched == nil {
		return
	}
	ttl := sched.Settings.EffectiveLeaseTTL()
	interval := ttl / 3
	if interval < 10 {
		interval = 10
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = sched.RenewLease(context.Background(), tenantID, slotID)
		}
	}
}
