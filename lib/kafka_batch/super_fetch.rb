# frozen_string_literal: true

require "oj"
require "set"

module KafkaBatch
  # Sidekiq SuperFetch-style concurrency for Karafka job consumers.
  #
  # Listener thread: Claim (Redis) → mark_as_consumed! → enqueue perform.
  # Pool threads: run the existing JobConsumer pipeline without marking; Complete
  # the workset on success, or leave it for control-plane reclaim on apply failure.
  #
  # Two limits (Go parity):
  #   claim_window — max Claimed∨Queued∨Performing (gates Claim+Mark)
  #   concurrency  — max concurrent #perform
  # Renew starts at Claim so leases stay alive while waiting for a perform slot.
  module SuperFetch
    class Executor
      def initialize(store: nil)
        @store          = store
        @mutex          = Mutex.new
        @in_flight      = Set.new
        @accepting      = true
        @claim_window   = nil # SizedQueue
        @perform_sem    = nil # SizedQueue
        @active_threads = 0
        @shutdown       = false
      end

      def dispatch(consumer, messages)
        messages.each { |message| dispatch_one(consumer, message) }
      end

      # Claim → mark → pool. Blocks on claim_window when full (not perform pool).
      def dispatch_one(consumer, message)
        acquire_claim_window!
        job_id = extract_job_id(message)

        if job_id.empty?
          begin
            consumer.send(:process_message, message)
          ensure
            release_claim_window!
          end
          return
        end

        unless track_in_flight(job_id)
          # Already claimed/performing in this process (Kafka redelivery).
          consumer.mark_as_consumed!(message)
          release_claim_window!
          return
        end

        begin
          claim = work.claim(
            job_id:        job_id,
            payload:       message.raw_payload,
            topic:         message.topic,
            partition:     message.partition,
            offset:        message.offset,
            consumer_id:   consumer_id,
            lease_ttl:     KafkaBatch.config.super_fetch_lease_ttl,
            heartbeat_ttl: KafkaBatch.config.liveness_ttl,
            steal_grace:   KafkaBatch.config.super_fetch_orphan_grace
          )
        rescue StandardError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][SuperFetch] claim error job_id=#{job_id}: #{e.class}: #{e.message} — leaving unacked"
          )
          untrack_in_flight(job_id)
          release_claim_window!
          return
        end

        unless claim.won
          KafkaBatch.logger.info(
            "[KafkaBatch][SuperFetch] claim lost job_id=#{job_id} — acking duplicate"
          )
          consumer.mark_as_consumed!(message)
          untrack_in_flight(job_id)
          release_claim_window!
          return
        end

        # Durability: Redis owns the job before Kafka forgets it.
        consumer.mark_as_consumed!(message)

        # Renew from claim time so lease cannot expire while waiting for perform.
        stop_renew = start_renew(job_id, claim.fence)

        Thread.new do
          Thread.current.name = "kafka-batch-superfetch-#{job_id[0, 8]}" if Thread.current.respond_to?(:name=)
          perform(consumer, message, job_id, claim.fence, stop_renew)
        end
      end

      def drain(timeout: 30)
        deadline = monotonic_now + timeout.to_f
        @mutex.synchronize { @accepting = false }
        loop do
          idle = @mutex.synchronize { @in_flight.empty? && @active_threads.zero? }
          break if idle
          break if monotonic_now >= deadline

          sleep 0.05
        end
        remaining = @mutex.synchronize { @in_flight.size }
        if remaining.positive?
          KafkaBatch.logger.warn(
            "[KafkaBatch][SuperFetch] drain timed out with #{remaining} in-flight job(s)"
          )
        end
      end

      def reset!
        drain(timeout: 5)
        @mutex.synchronize do
          @in_flight.clear
          @accepting = true
          @shutdown  = false
          @claim_window = nil
          @perform_sem  = nil
        end
      end

      private

      def work
        @store || Workset.store
      end

      def consumer_id
        Liveness.consumer_id
      end

      def concurrency
        n = KafkaBatch.config.super_fetch_concurrency.to_i
        n.positive? ? n : 1
      end

      def claim_window_size
        n = KafkaBatch.config.super_fetch_claim_window.to_i
        return n if n >= concurrency

        concurrency * 2
      end

      def acquire_claim_window!
        claim_window_queue.pop
      end

      def release_claim_window!
        claim_window_queue << true
      end

      def acquire_perform_slot!
        perform_sem_queue.pop
        @mutex.synchronize { @active_threads += 1 }
      end

      def release_perform_slot!
        @mutex.synchronize { @active_threads -= 1 if @active_threads.positive? }
        perform_sem_queue << true
      end

      def claim_window_queue
        @mutex.synchronize do
          return @claim_window if @claim_window

          n = claim_window_size
          @claim_window = SizedQueue.new(n)
          n.times { @claim_window << true }
          @claim_window
        end
      end

      def perform_sem_queue
        @mutex.synchronize do
          return @perform_sem if @perform_sem

          n = concurrency
          @perform_sem = SizedQueue.new(n)
          n.times { @perform_sem << true }
          @perform_sem
        end
      end

      def track_in_flight(job_id)
        @mutex.synchronize do
          return false if @in_flight.include?(job_id)

          @in_flight.add(job_id)
          true
        end
      end

      def untrack_in_flight(job_id)
        @mutex.synchronize { @in_flight.delete(job_id) }
      end

      def perform(consumer, message, job_id, fence, stop_renew)
        acquire_perform_slot!
        begin
          Thread.current[:kafka_batch_sf_acked] = true
          consumer.send(:process_message, message)

          unless work.still_owned?(job_id, consumer_id, fence)
            KafkaBatch.logger.warn(
              "[KafkaBatch][SuperFetch] lost fence job_id=#{job_id} — skip complete"
            )
            return
          end

          complete_with_retry(job_id, fence)
        rescue StandardError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][SuperFetch] perform/apply error job_id=#{job_id}: " \
            "#{e.class}: #{e.message} — leaving in workset for reclaim"
          )
        ensure
          Thread.current[:kafka_batch_sf_acked] = nil
          stop_renew.call if stop_renew
          untrack_in_flight(job_id)
          release_perform_slot!
          release_claim_window!
        end
      end

      def complete_with_retry(job_id, fence)
        5.times do |i|
          begin
            work.complete(job_id, consumer_id, fence)
            return
          rescue StandardError => e
            KafkaBatch.logger.warn(
              "[KafkaBatch][SuperFetch] complete error job_id=#{job_id} " \
              "attempt=#{i + 1}: #{e.message}"
            )
            sleep((i + 1) * 0.05)
          end
        end
      end

      def start_renew(job_id, fence)
        stop = false
        lease = KafkaBatch.config.super_fetch_lease_ttl.to_i
        lease = Workset::DEFAULT_LEASE_TTL if lease <= 0
        interval = [lease / 3.0, 5.0].max

        thread = Thread.new do
          Thread.current.name = "kafka-batch-sf-renew-#{job_id[0, 8]}" if Thread.current.respond_to?(:name=)
          loop do
            sleep(interval)
            break if stop

            begin
              ok = work.renew(job_id, consumer_id, fence, ttl: lease)
              unless ok
                KafkaBatch.logger.warn(
                  "[KafkaBatch][SuperFetch] renew lost fence job_id=#{job_id} — stop renew"
                )
                break
              end
            rescue StandardError => e
              # Transient Redis errors must not stop renew — lease expiry after
              # Kafka ack would drop the job with no reclaim path.
              KafkaBatch.logger.warn(
                "[KafkaBatch][SuperFetch] renew error job_id=#{job_id}: #{e.message} — will retry"
              )
            end
          end
        end

        -> {
          stop = true
          begin
            thread.join(0.5)
          rescue StandardError
            nil
          end
        }
      end

      def extract_job_id(message)
        raw = message.raw_payload
        return "" if raw.nil?

        data = Oj.load(raw)
        return "" unless data.is_a?(Hash)

        data["job_id"].to_s
      rescue StandardError
        ""
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end

    class << self
      def executor
        @mutex ||= Mutex.new
        @mutex.synchronize { @executor ||= Executor.new }
      end

      def reset!
        @mutex ||= Mutex.new
        @mutex.synchronize do
          @executor&.reset!
          @executor = nil
        end
      end

      def drain(timeout: 30)
        executor.drain(timeout: timeout)
      end
    end
  end
end
