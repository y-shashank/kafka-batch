require "karafka"
require "oj"

module KafkaBatch
  module Fairness
    # Karafka consumer for the fairness INGEST topic. It is the *intake* half of
    # the Redis-backed WFQ fairness path: it drains the durable ingest backlog
    # into the Scheduler's bounded per-tenant ready window, applying backpressure
    # when a tenant's window is full.
    #
    #   ingest topic → Dispatcher (Scheduler#enqueue → Redis window)   ← this file
    #               → Forwarder  (Scheduler#checkout → ready topic)
    #               → ready topic → JobConsumer → perform → Scheduler#complete
    #
    # Why this shape:
    #   * The ingest topic is the durable, unbounded backlog (Kafka).
    #   * The Redis window (fairness_ready_window per tenant) is the WFQ staging
    #     area the Scheduler orders across tenants. When it is full the Dispatcher
    #     pauses that ingest partition, so the overflow stays in Kafka rather than
    #     ballooning Redis.
    #   * The Forwarder (a per-process background thread, started lazily here)
    #     pulls the fairest job and forwards it to the ready topic under the
    #     global + per-tenant in-flight limits. That is what makes both
    #     :time_fairness and :job_count_fairness real.
    #
    # Fairness itself is decided by the Scheduler (virtual-time ring), not by
    # Kafka fetch balance — so it is exact/weighted, not approximate.
    class Dispatcher < Karafka::BaseConsumer
      prepend KafkaBatch::Consumers::ConsumptionGate

      # How long to pause an ingest partition when a tenant's Redis ready window
      # is full, before retrying the enqueue (backpressure).
      BACKPRESSURE_PAUSE_MS = 250

      def consume
        # Which lane is this? Both ingest topics route to this consumer class; the
        # topic name tells us whether we're the :time or :throughput dispatcher.
        type = fairness_type

        # Ensure this process runs exactly one forwarder thread for THIS lane.
        # Lazy-started here so only processes assigned ingest partitions forward.
        KafkaBatch::Fairness::Forwarder.ensure_running!(type)

        sched = KafkaBatch.scheduler(type)
        unless sched
          # Redis is a hard dependency; if the scheduler is genuinely unavailable
          # we cannot enqueue. Leave offsets uncommitted so Karafka redelivers
          # once Redis recovers, rather than dropping jobs.
          KafkaBatch.logger.error(
            "[KafkaBatch][Fairness::Dispatcher] scheduler unavailable — leaving " \
            "offsets uncommitted for redelivery"
          )
          pause(messages.first.offset, BACKPRESSURE_PAUSE_MS)
          return
        end

        last_committed = nil
        messages.each do |message|
          tenant = tenant_key(message.raw_payload)
          result = sched.enqueue(tenant, message.raw_payload)

          if result == :full
            # Tenant's Redis window is full. Commit everything enqueued so far,
            # then pause at THIS message so it (and the rest of the batch) is
            # redelivered once the forwarder drains the window. Backpressure keeps
            # the durable backlog in Kafka.
            mark_as_consumed!(last_committed) if last_committed
            pause(message.offset, BACKPRESSURE_PAUSE_MS)
            return
          end

          last_committed = message
        end

        mark_as_consumed!(last_committed) if last_committed
      end

      private

      # Which fairness lane this consumer instance is draining, derived from the
      # ingest topic of the messages it received (works both under real Karafka and
      # in unit tests where the consumer's own #topic isn't set). Defaults to :time.
      def fairness_type
        tname = messages.first&.topic
        tname == KafkaBatch.config.fair_throughput_ingest_topic ? :throughput : :time
      rescue StandardError
        :time
      end

      # The fairness unit. Prefer tenant_id; fall back to batch_id then job_id so
      # jobs without an explicit tenant still flow (grouped per batch/job) — but
      # multi-tenant fairness only works when tenant_id is set on the job.
      def tenant_key(raw)
        data = Oj.load(raw)
        t = data["tenant_id"]
        return t if t.is_a?(String) && !t.empty?
        b = data["batch_id"]
        return b if b.is_a?(String) && !b.empty?
        data["job_id"].to_s
      rescue StandardError
        # Unparseable payload: use a stable bucket so it is not silently dropped.
        "_unparsable"
      end
    end
  end
end
