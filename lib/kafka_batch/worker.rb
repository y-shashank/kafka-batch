module KafkaBatch
  # Include this module in any class that should act as a Kafka-batch worker.
  #
  # Example:
  #
  #   class ProcessOrderWorker
  #     include KafkaBatch::Worker
  #
  #     kafka_topic "orders.process"
  #     max_retries 5      # optional, overrides global default
  #
  #     def perform(payload)
  #       Order.find(payload["order_id"]).process!
  #       ChildWorker.perform_async("parent_job_id" => job_id) if batch
  #     end
  #
  # While #perform runs, JobConsumer binds job metadata on the worker instance:
  #   job_id, batch_id, batch, retry_count, uniq_hex (when `uniq true`).
  #   end
  #
  # The worker is registered automatically on include and will be picked up
  # by KafkaBatch.draw_routes when you set up Karafka routing.
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
      KafkaBatch.register_worker(base)
    end

    module ClassMethods
      # Kafka topic this worker consumes from. When a worker doesn't declare one,
      # it falls back to the shared default queue (config.jobs_topic) — multiple
      # such workers share that topic, and JobConsumer dispatches each message to
      # the right worker via its embedded worker_class.
      def kafka_topic(name = nil)
        if name
          @kafka_topic = name.to_s
        else
          @kafka_topic || KafkaBatch.config.jobs_topic
        end
      end

      # Maximum retry attempts before a job is sent to the DLT.
      # Defaults to KafkaBatch.config.max_retries.
      def max_retries(n = nil)
        if n
          @max_retries = n.to_i
        else
          @max_retries || KafkaBatch.config.max_retries
        end
      end

      # After this many retries a still-failing job counts toward its batch's
      # on_complete (the job keeps retrying up to max_retries in the background).
      # Defaults to KafkaBatch.config.complete_after_retries.
      def complete_after_retries(n = nil)
        if n
          @complete_after_retries = n.to_i
        else
          @complete_after_retries || KafkaBatch.config.complete_after_retries
        end
      end

      # Whether this worker's jobs flow through the shared multi-tenant FAIR lane
      # (ingest topic → Dispatcher → ready topic → JobConsumer swarm) instead of
      # the worker's own/default topic. Fairness is a per-worker choice — there is
      # no global switch — so fair and plain workers run side by side in the same
      # process. Tenant isolation comes from the `tenant_id` set on the batch/push.
      #
      #   fairness true    # this worker shares the fair lane across tenants
      #
      # @return [Boolean]
      def fairness(enabled = :__unset__)
        if enabled == :__unset__
          @fairness.nil? ? false : @fairness
        else
          @fairness = enabled ? true : false
        end
      end

      # Predicate form of #fairness.
      def fairness?
        fairness
      end

      # Which fairness LANE this worker's jobs flow through when `fairness true`:
      #   :time       – weighted wall-clock-time fairness (default). Best for
      #                 uneven runtimes (e.g. 20-60s jobs).
      #   :throughput – weighted job-count fairness. Best when runtimes are similar.
      # Both lanes run simultaneously, so a single batch may contain jobs of both
      # types. Ignored unless the worker also sets `fairness true`.
      #
      #   fairness true
      #   fairness_type :throughput
      #
      # @return [Symbol] :time (default) | :throughput
      def fairness_type(type = :__unset__)
        if type == :__unset__
          @fairness_type || :time
        else
          t = type&.to_sym
          unless KafkaBatch::Configuration::FAIRNESS_TYPES.include?(t)
            raise ArgumentError, "fairness_type must be :time or :throughput (got #{type.inspect})"
          end
          @fairness_type = t
        end
      end

      # ── Sidekiq-compatible enqueue API ────────────────────────────────────
      # Convenience wrappers so a worker reads like a Sidekiq job:
      #
      #   MyWorker.perform_async("id" => 1)          # run now
      #   MyWorker.perform_async({ "id" => 1 }, valid_till: 1.hour.from_now)
      #   MyWorker.perform_in(5.minutes, "id" => 1)  # run in 5 minutes (schedule poller)
      #   MyWorker.perform_at(time, "id" => 1)       # run at an absolute time
      #
      # perform_in/perform_at persist the job to the delayed-job index and are
      # dispatched by the SchedulePoller when due. @return [String] job_id

      def perform_async(payload = {}, valid_till: nil, job_id: nil, tenant_id: nil)
        KafkaBatch::Batch.enqueue(
          self, payload,
          valid_till: valid_till, job_id: job_id, tenant_id: tenant_id
        )
      end

      def perform_in(interval, payload = {})
        KafkaBatch::Batch.enqueue_in(interval, self, payload)
      end

      def perform_at(time, payload = {})
        KafkaBatch::Batch.enqueue_at(time, self, payload)
      end

      # Bulk-schedule many jobs (same worker) to run at one time (Sidekiq
      # perform_bulk, delayed). One broker round-trip + one index write.
      #   MyWorker.perform_bulk_in(300, [{"id"=>1}, {"id"=>2}, …])
      # @return [Array<String>] job ids
      def perform_bulk_in(interval, payloads)
        KafkaBatch::Batch.enqueue_many_in(interval, self, payloads)
      end

      def perform_bulk_at(time, payloads)
        KafkaBatch::Batch.enqueue_many_at(time, self, payloads)
      end

      # Reject duplicate enqueues while an identical job (same worker + payload)
      # is queued in Kafka or in progress. Uses a compact Redis lock (64-bit
      # XXHash64 digest stored as 8 raw bytes, not hex).
      #
      #   uniq true
      #
      # @return [Boolean]
      def uniq(enabled = :__unset__)
        if enabled == :__unset__
          @uniq.nil? ? false : @uniq
        else
          @uniq = enabled ? true : false
        end
      end

      def uniq?
        uniq
      end

      # Pin every retry of this worker to a single delay tier (e.g. :short,
      # :medium, :large) instead of walking the default progression. Pass nil
      # (default) to use config.retry_tier_progression.
      #
      #   retry_tier :medium   # all retries wait ~7 min
      #
      # @return [Symbol, nil]
      def retry_tier(tier = :__unset__)
        if tier == :__unset__
          @retry_tier
        else
          @retry_tier = tier&.to_sym
        end
      end
    end

    # Job metadata set by JobConsumer (and tests) immediately before #perform.
    attr_reader :job_id, :batch_id, :retry_count, :uniq_hex

    # @deprecated Prefer +batch_id+ — kept for backward compatibility.
    def kafka_batch_id
      batch_id
    end

    def kafka_batch_id=(id)
      @batch_id = id
      @kafka_batch = nil
    end

    # Bind metadata from a decoded Kafka job message. Called by JobConsumer; safe
    # to call manually in tests.
    def bind_job_context!(data, worker_class: self.class)
      @job_id      = data["job_id"]
      @batch_id    = data["batch_id"]
      @retry_count = data["attempt"].to_i
      @kafka_batch = nil

      wc = worker_class.is_a?(Class) ? worker_class : self.class
      @uniq_hex =
        if wc.uniq? && KafkaBatch.config.uniq_enabled
          KafkaBatch::Uniqueness.digest_hex(wc, data["payload"] || {})
        end

      self
    end

    # The batch this job belongs to — a pushable Batch handle, or nil for a
    # standalone job. Uses Batch.open so tenant_id and callbacks are restored
    # from the store (no Batch.open call needed in application code):
    #
    #   def perform(payload)
    #     batch.push(ChildWorker, "parent_id" => payload["id"])
    #   end
    #
    # @return [KafkaBatch::Batch, nil]
    def batch
      bid = batch_id
      return nil if bid.nil? || bid.to_s.empty?

      @kafka_batch ||= KafkaBatch::Batch.open(bid)
    end

    # Override this in your worker class.
    # @param payload [Hash] deserialized message payload
    def perform(payload)
      raise NotImplementedError, "#{self.class}#perform must be implemented"
    end
  end
end
