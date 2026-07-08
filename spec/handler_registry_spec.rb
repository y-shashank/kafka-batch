# frozen_string_literal: true

RSpec.describe KafkaBatch::HandlerRegistry do
  after { described_class.reset! }

  describe ".register_ruby" do
    it "indexes handlers by job_type and worker_class name" do
      klass = Class.new do
        def self.name
          "CustomJobWorker"
        end

        include KafkaBatch::Worker
        job_type "custom.job"
      end

      described_class.register_ruby(klass)

      handler = described_class.resolve!({ "job_type" => "custom.job" })
      expect(handler.worker_class).to eq(klass)
      expect(handler.runtime).to eq(:ruby)
      expect(handler.executor).to be_a(KafkaBatch::Executors::Ruby)
    end

    it "raises when the same job_type is registered to two classes" do
      Class.new do
        def self.name
          "DupAWorker"
        end

        include KafkaBatch::Worker
        job_type "dup.type"
      end

      expect {
        Class.new do
          def self.name
            "DupBWorker"
          end

          include KafkaBatch::Worker
          job_type "dup.type"
        end
      }.to raise_error(ArgumentError, /already registered/)
    end
  end

  describe ".resolve!" do
    it "resolves by job_type when present" do
      klass = Class.new do
        def self.name
          "OrdersProcessWorker"
        end

        include KafkaBatch::Worker
        job_type "orders.process"
      end
      described_class.register_ruby(klass)

      handler = described_class.resolve!(
        "job_type" => "orders.process", "worker_class" => "OtherWorker"
      )
      expect(handler.worker_class).to eq(klass)
    end

    it "falls back to worker_class for legacy messages" do
      handler = described_class.resolve!(
        "worker_class" => "SuccessfulWorker", "job_type" => nil
      )
      expect(handler.worker_class).to eq(SuccessfulWorker)
      expect(handler.job_type).to eq("successful")
    end

    it "raises UnknownHandler for an unknown job_type" do
      expect {
        described_class.resolve!("job_type" => "missing.handler")
      }.to raise_error(described_class::UnknownHandler, /Missing job_type and worker_class|Unknown job_type/)
    end

    it "falls back to worker_class when job_type is on the wire but the registry was cleared" do
      described_class.reset!
      handler = described_class.resolve!(
        "job_type" => "successful", "worker_class" => "SuccessfulWorker"
      )
      expect(handler.worker_class).to eq(SuccessfulWorker)
      expect(handler.job_type).to eq("successful")
    end

    it "raises when job_type disagrees with the resolved worker" do
      described_class.reset!
      expect {
        described_class.resolve!(
          "job_type" => "wrong.type", "worker_class" => "SuccessfulWorker"
        )
      }.to raise_error(described_class::UnknownHandler, /does not match/)
    end

    it "raises UnknownHandler for an unknown worker_class" do
      expect {
        described_class.resolve!("worker_class" => "NoSuchWorker")
      }.to raise_error(described_class::UnknownHandler, /Unknown worker class/)
    end
  end
end

RSpec.describe KafkaBatch::Executors::Ruby do
  it "binds job context and calls #perform" do
    executor = described_class.new(SuccessfulWorker)
    handler  = KafkaBatch::HandlerRegistry::Handler.new(
      job_type: "successful", runtime: :ruby,
      worker_class: SuccessfulWorker, executor: executor
    )
    data = {
      "job_id" => "j1", "batch_id" => nil, "attempt" => 0,
      "payload" => { "x" => 1 }
    }
    message = FakeMessage.new(topic: "test.context_probe", payload: data)

    context = KafkaBatch::ExecutionContext.new(data: data, message: message, handler: handler)
    executor.call(context)

    run = KafkaBatchSpec::WorkerRuns.runs.last
    expect(run[:name]).to eq(:success)
    expect(run[:payload]).to eq({ "x" => 1 })
  end
end
