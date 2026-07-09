# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Instrumentation do
  let(:error) { StandardError.new("boom") }

  it "exposes all lifecycle instrument methods without error" do
    expect { described_class.job_processed(job_id: "j", batch_id: "b", worker_class: "W", duration: 1.0) }.not_to raise_error
    expect { described_class.job_retried(job_id: "j", batch_id: "b", worker_class: "W", attempt: 0, next_attempt: 1) }.not_to raise_error
    expect { described_class.job_failed(job_id: "j", batch_id: "b", worker_class: "W", attempt: 1, error: error) }.not_to raise_error
    expect { described_class.job_cancelled(job_id: "j", batch_id: "b", worker_class: "W") }.not_to raise_error
    expect { described_class.job_uniq_skipped(worker_class: "W", payload: {}) }.not_to raise_error
    expect { described_class.job_expired(job_id: "j", batch_id: "b", worker_class: "W", valid_till: Time.now.iso8601) }.not_to raise_error
    expect { described_class.job_emit_retried(job_id: "j", batch_id: "b", attempt: 1, error: error) }.not_to raise_error
    expect { described_class.scheduled_enqueued(job_id: "j", batch_id: "b", worker_class: "W", run_at: Time.now) }.not_to raise_error
    expect { described_class.scheduled_enqueued_bulk(count: 2, batch_id: "b", worker_class: "W", run_at: Time.now) }.not_to raise_error
    expect { described_class.scheduled_dispatched(job_id: "j", batch_id: "b", worker_class: "W", topic: "t") }.not_to raise_error
    expect { described_class.batch_created(batch_id: "b") }.not_to raise_error
    expect { described_class.batch_sealed(batch_id: "b", total_jobs: 1) }.not_to raise_error
    expect { described_class.batch_completed(batch_id: "b", outcome: "success", total_jobs: 1, completed_count: 1, failed_count: 0) }.not_to raise_error
    expect { described_class.callback_invoked(batch_id: "b", callback_class: "C", callback_method: "on_success") }.not_to raise_error
    expect { described_class.callback_failed(batch_id: "b", callback_class: "C", callback_method: "on_success", error: error) }.not_to raise_error
    expect { described_class.dlt_published(dlt_type: "job", source_topic: "t") }.not_to raise_error
    expect {
      described_class.consumer_priority_yielded(
        consumer_class: "KafkaBatch::Consumers::PriorityJobConsumer",
        p0_topic:       "p0",
        consumer_group:   "cg",
        pause_ms:       2000,
        mode:           "strict",
        rank:           1,
        higher_topics:  %w[p0]
      )
    }.not_to raise_error
    expect { described_class.reconciler_ran(stale_count: 0, lost_count: 0, duration: 0.1) }.not_to raise_error
    expect { described_class.scheduled_index_failed(count: 2, batch_id: "b", attempts: 3, error: error) }.not_to raise_error
    expect {
      described_class.web_action(action: "batches.cancel", path: "/batches/a/cancel", status: "ok", actor: "ops")
    }.not_to raise_error
  end
end
