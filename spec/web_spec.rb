RSpec.describe KafkaBatch::Web do
  def get(path, query: "")
    KafkaBatch::Web.call(
      "REQUEST_METHOD" => "GET", "PATH_INFO" => path,
      "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => query
    )
  end

  def post(path, query: "", body: nil, csrf: true)
    token = SecureRandom.hex(16)
    qs = query.to_s
    if csrf
      qs = qs.empty? ? "_csrf=#{token}" : "#{qs}&_csrf=#{token}" unless qs.include?("_csrf=")
    end
    env = {
      "REQUEST_METHOD" => "POST", "PATH_INFO" => path,
      "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => qs
    }
    env["HTTP_COOKIE"] = "#{KafkaBatch::Web::CSRF_COOKIE}=#{token}" if csrf
    if body
      env["rack.input"] = StringIO.new(body)
      env["CONTENT_TYPE"] = "application/x-www-form-urlencoded"
    end
    KafkaBatch::Web.call(env)
  end

  def seed(total: 3, **opts)
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  # By default keep the dashboard off the real Kafka cluster (lag uses
  # Karafka::Admin). Individual lag tests opt back in with their own stubs.
  before { allow(KafkaBatch::Lag).to receive(:available?).and_return(false) }

  describe "GET /" do
    it "marks dashboard responses no-store so a refresh always shows current data" do
      _s, headers, = get("/")
      expect(headers["cache-control"]).to eq("no-store")
    end

    it "reflects live store counts on each request" do
      seed(total: 5)
      first = get("/").last.join
      seed(total: 3)
      second = get("/").last.join
      # the Total metric increases between requests (no caching, fresh read)
      expect(first).not_to eq(second)
    end

    it "renders the batch list with metrics" do
      id = seed(on_complete: "RecordingCallback")
      status, headers, body = get("/")
      html = body.join

      expect(status).to eq(200)
      expect(headers["content-type"]).to match(%r{text/html})
      expect(html).to include("KafkaBatch")
      expect(html).to include("metric-value")          # summary cards
      expect(html).to include(id[0, 8])                # batch row
      expect(html).to include("Pending")
    end

    it "has a Live toggle (localStorage-persisted 5s auto-reload)" do
      html = get("/").last.join
      expect(html).to include('id="kb-live-toggle"')
      expect(html).to include("kafka_batch_live")  # localStorage key
      expect(html).to include("location.reload()")
    end

    it "embeds an inline SVG favicon and header logo mark (works at any mount path)" do
      html = get("/").last.join
      expect(html).to include('<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,')
      expect(html).to include('rel="apple-touch-icon"')
      expect(html).to include('class="logo-mark"')
      # The data URI decodes back to the fan-out SVG mark.
      require "base64"
      expect(Base64.strict_decode64(KafkaBatch::Web::FAVICON_DATA_URI.split(",", 2).last))
        .to include("<svg").and include("</svg>")
    end

    it "has bulk select checkboxes and cancel/delete actions" do
      seed
      html = get("/").last.join
      expect(html).to include('id="kb-select-all"')
      expect(html).to include('id="kb-bulk-form"')
      expect(html).to include("Cancel selected")
      expect(html).to include("Delete selected")
      expect(html).to include("Cancel all")
      expect(html).to include("Delete all")
      expect(html).to include('class="kb-batch-check"')
    end

    it "shows the batch description on the list and detail pages" do
      id = seed(description: "Important nightly batch")
      expect(get("/").last.join).to include("Important nightly batch")
      expect(get("/batches/#{id}").last.join).to include("Important nightly batch")
    end

    it "has a search box and filters by id or description" do
      a = seed(description: "Findable Alpha")
      b = seed(description: "Other Beta")

      expect(get("/").last.join).to include('name="q"')  # search box present

      html = get("/", query: "q=Findable").last.join
      expect(html).to include(a)
      expect(html).not_to include(b)
    end

    it "shows which consumer ran the callback on the detail page" do
      id = seed(total: 1)
      KafkaBatch.store.claim_callback(id, "pod-xyz#99")
      html = get("/batches/#{id}").last.join
      expect(html).to include("Callback ran on")
      expect(html).to include("pod-xyz#99")
    end

    it "shows the total pending-jobs counter" do
      seed(total: 7)  # 7 pending (running, none completed)
      html = get("/").last.join
      expect(html).to include("Pending jobs")
      expect(html).to include(">7<")
    end

    it "shows consumer and running-job counts when liveness is available" do
      allow(KafkaBatch::Liveness).to receive(:available?).and_return(true)
      allow(KafkaBatch::Liveness).to receive(:consumers).and_return([{"consumer_id" => "a"}, {"consumer_id" => "b"}])
      allow(KafkaBatch::Liveness).to receive(:running_jobs).and_return([{"job_id" => "j1"}])

      html = get("/").last.join
      expect(html).to include("Consumers")
      expect(html).to include("Running jobs")
      expect(html).to include('href="/kafka_batch/live"')
      expect(html).to include("color:#0ea5e9'>2</div>")
      expect(html).to include("color:#6366f1'>1</div>")
    end

    it "omits liveness metrics when Redis liveness is unavailable" do
      allow(KafkaBatch::Liveness).to receive(:available?).and_return(false)
      html = get("/").last.join
      expect(html).not_to include("Running jobs")
    end

    it "filters by status" do
      running = seed
      cancelled = seed
      KafkaBatch::Batch.cancel(cancelled)

      html = get("/", query: "status=cancelled").last.join
      expect(html).to include(cancelled[0, 8])
      expect(html).not_to include(running[0, 8])
    end
  end

  describe "GET /lag" do
    it "links to the lag page from the dashboard" do
      expect(get("/").last.join).to include("/kafka_batch/lag")
    end

    it "shows a graceful message when the admin API is unavailable" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(false)
      status, _headers, body = get("/lag")
      html = body.join
      expect(status).to eq(200)
      expect(html).to include("Topic lag")
      expect(html).to include("admin API")
    end

    it "renders per-topic and per-partition pending counts" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch::Lag).to receive(:partitions).and_return(
        [
          { group: "g-jobs", topic: "demo", partition: 0, committed: 10, end_offset: 17, lag: 7, never_consumed: false },
          { group: "g-jobs", topic: "demo", partition: 1, committed: 5,  end_offset: 5,  lag: 0, never_consumed: false }
        ]
      )

      html = get("/lag").last.join
      expect(html).to include("Total pending")
      expect(html).to include("Pending by topic")
      expect(html).to include("Pending by partition")
      expect(html).to include("Ingest partition lookup")
      expect(html).to include("demo")
      expect(html).to include("7") # the lag value
    end

    it "resolves ingest partition for a tenant_id query param" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch::Lag).to receive(:partitions).and_return([])
      allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(12)
      allow(KafkaBatch).to receive(:tenant_ingest_partition).with("acme", :time).and_return(7)

      html = get("/lag", query: "tenant_id=acme").last.join
      expect(html).to include("partition 7")
      expect(html).to include("acme")
      expect(html).to include(KafkaBatch.config.fairness_ingest_topic(:time))
    end

    it "shows pause controls when Redis consumption control is available" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch::Lag).to receive(:partitions).and_return(
        [{ group: "g-jobs", topic: "demo", partition: 0, committed: 0, end_offset: 1, lag: 1, never_consumed: false }]
      )
      allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(true)
      allow(KafkaBatch::ConsumptionControl).to receive(:snapshot).and_return(topics: Set.new, partitions: Set.new)

      html = get("/lag").last.join
      expect(html).to include("Pause")
      expect(html).to include("Status")
    end

    it "pauses a topic via POST and redirects back to /lag" do
      allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(true)
      allow(KafkaBatch::ConsumptionControl).to receive(:pause_topic).with(group: "g", topic: "demo").and_return(true)

      status, headers, = post("/lag/pause", query: "scope=topic&group=g&topic=demo")
      expect(status).to eq(302)
      expect(headers["location"]).to eq("/kafka_batch/lag")
    end
  end

  describe "GET /fairness/time" do
    it "shows an inactive notice but still renders lag when no worker uses the lane" do
      allow(KafkaBatch).to receive(:active_fairness_types).and_return([])
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      ingest_group = KafkaBatch.dispatch_consumer_group(:time)
      fair_group   = KafkaBatch.jobs_fair_consumer_group(:time)
      go_group     = KafkaBatch.go_worker_fair_ready_consumer_group(:time)
      ingest_t     = KafkaBatch.config.fairness_ingest_topic(:time)
      ruby_ready   = KafkaBatch.config.fairness_ready_topic(:time, :ruby)
      go_ready     = KafkaBatch.config.fairness_ready_topic(:time, :go)
      allow(KafkaBatch::Lag).to receive(:read_group).with(ingest_group, [ingest_t])
        .and_return(ingest_group => { ingest_t => { 0 => { offset: 0, lag: 3 } } })
      allow(KafkaBatch::Lag).to receive(:read_group).with(fair_group, [ruby_ready])
        .and_return(fair_group => { ruby_ready => { 0 => { offset: 0, lag: 0 } } })
      allow(KafkaBatch::Lag).to receive(:read_group).with(go_group, [go_ready])
        .and_return(go_group => { go_ready => {} })

      html = get("/fairness/time").last.join
      expect(html).to include("No registered workers use the")
      expect(html).to include("Active lanes")
      expect(html).to include(">3<")
    end

    it "renders lanes, buffer depth and dispatcher status when a worker is fair" do
      allow(KafkaBatch).to receive(:active_fairness_types).and_return([:time])
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      ingest_group = KafkaBatch.dispatch_consumer_group(:time)
      fair_group   = KafkaBatch.jobs_fair_consumer_group(:time)
      go_group     = KafkaBatch.go_worker_fair_ready_consumer_group(:time)
      ingest_t     = KafkaBatch.config.fairness_ingest_topic(:time)
      ruby_ready   = KafkaBatch.config.fairness_ready_topic(:time, :ruby)
      go_ready     = KafkaBatch.config.fairness_ready_topic(:time, :go)
      allow(KafkaBatch::Lag).to receive(:read_group).with(ingest_group, [ingest_t])
        .and_return(ingest_group => { ingest_t => { 8 => { offset: 0, lag: 40 }, 9 => { offset: 0, lag: 25 } } })
      allow(KafkaBatch::Lag).to receive(:read_group).with(fair_group, [ruby_ready])
        .and_return(fair_group => { ruby_ready => { 0 => { offset: 0, lag: 12 } } })
      allow(KafkaBatch::Lag).to receive(:read_group).with(go_group, [go_ready])
        .and_return(go_group => { go_ready => { 1 => { offset: 0, lag: 5 } } })

      html = get("/fairness/time").last.join
      expect(html).to include("Active lanes")
      expect(html).to include(">2<")    # 2 active lanes
      expect(html).to include(">65<")   # un-dispatched total 40+25
      expect(html).to include(">17<")   # ready buffer total 12+5 across split topics
      expect(html).to include("fair_time_ready.ruby")
      expect(html).to include("fair_time_ready.go")
      expect(html).to include("Flowing")
    end

    it "reads the legacy single ready topic when split topics are not configured" do
      allow(KafkaBatch).to receive(:active_fairness_types).and_return([:time])
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch.config).to receive(:runtime_split_fair_ready?).with(:time).and_return(false)
      ingest_group = KafkaBatch.dispatch_consumer_group(:time)
      fair_group   = KafkaBatch.jobs_fair_consumer_group(:time)
      ingest_t     = KafkaBatch.config.fairness_ingest_topic(:time)
      legacy_ready = KafkaBatch.config.fairness_ready_topic(:time)
      allow(KafkaBatch::Lag).to receive(:read_group).with(ingest_group, [ingest_t])
        .and_return(ingest_group => { ingest_t => {} })
      allow(KafkaBatch::Lag).to receive(:read_group).with(fair_group, [legacy_ready])
        .and_return(fair_group => { legacy_ready => { 2 => { offset: 0, lag: 7 } } })

      html = get("/fairness/time").last.join
      expect(html).to include(">7<")
      expect(html).not_to include("fair_time_ready.ruby")
    end

    it "links to the fairness pages from the dashboard nav" do
      html = get("/").last.join
      expect(html).to include("/kafka_batch/fairness/time")
      expect(html).to include("/kafka_batch/fairness/throughput")
    end
  end

  describe "GET /failures (all batches)" do
    it "includes the Live toggle in the header" do
      expect(get("/failures").last.join).to include('id="kb-live-toggle"')
    end

    it "lists failures across batches and the dashboard links to it" do
      a = seed(total: 1)
      b = seed(total: 1)
      KafkaBatch.store.record_failure(batch_id: a, job_id: "ja", worker_class: "W", error_class: "Boom", error_message: "msg-a", status: "failed")
      KafkaBatch.store.record_failure(batch_id: b, job_id: "jb", worker_class: "W", error_class: "Nope", error_message: "msg-b", status: "retrying")

      # dashboard has a link to the global failures page
      expect(get("/").last.join).to include("/kafka_batch/failures")

      html = get("/failures").last.join
      expect(html).to include("Failures across all batches")
      expect(html).to include("Boom")
      expect(html).to include("Nope")
      expect(html).to include(a[0, 8])
      expect(html).to include(b[0, 8])
    end

    it "filters by status" do
      a = seed(total: 1)
      KafkaBatch.store.record_failure(batch_id: a, job_id: "ja", worker_class: "W", error_class: "PermanentBoom", error_message: "m", status: "failed")
      KafkaBatch.store.record_failure(batch_id: a, job_id: "jr", worker_class: "W", error_class: "FlakyTimeout", error_message: "m", status: "retrying")

      html = get("/failures", query: "status=failed").last.join
      expect(html).to include("PermanentBoom")
      expect(html).not_to include("FlakyTimeout")
    end

    it "offers only Retrying and Failed filters (no All)" do
      html = get("/failures").last.join
      expect(html).to include(">Retrying<")
      expect(html).to include(">Failed<")
      expect(html).not_to include(">All<")
    end

    it "shows the total and per-tier pending retries when available" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch::Lag).to receive(:partitions).and_return(
        [
          { group: "g-control", topic: KafkaBatch.config.retry_topic_for(:short),  partition: 0, lag: 3 },
          { group: "g-control", topic: KafkaBatch.config.retry_topic_for(:short),  partition: 1, lag: 2 },
          { group: "g-control", topic: KafkaBatch.config.retry_topic_for(:medium), partition: 0, lag: 1 },
          { group: "g-jobs",    topic: "something.else",                           partition: 0, lag: 9 }
        ]
      )

      html = get("/failures").last.join
      expect(html).to include("Pending retries (all tiers)")
      expect(html).to include("short tier", "medium tier", "large tier")
      expect(html).to include(">6<") # total across tier topics (3+2+1), not 15
      expect(html).to include(">5<") # short tier (3+2)
    end
  end

  describe "GET /weights" do
    it "shows capacity share percentages that sum to 100%" do
      sched = instance_double(KafkaBatch::Fairness::Scheduler, default_weight: 1.0)
      allow(KafkaBatch).to receive(:scheduler).and_return(sched)
      allow(sched).to receive(:all_tenants).and_return([
        { tenant_id: "a", weight: 3.0, has_custom_weight: true, inflight: 0, queued: false, vtime: 0.0 },
        { tenant_id: "b", weight: 1.0, has_custom_weight: false, inflight: 0, queued: false, vtime: 0.0 }
      ])

      html = get("/weights").last.join
      expect(html).to include("Capacity distribution")
      expect(html).to include("weight-share-track")
      expect(html).to include("75.0%")
      expect(html).to include("25.0%")
      expect(html).to include("Capacity share")
    end

    it "warns that weights only affect ordering when weighted concurrency is off" do
      sched = instance_double(KafkaBatch::Fairness::Scheduler, default_weight: 1.0, all_tenants: [])
      allow(KafkaBatch).to receive(:scheduler).and_return(sched)

      KafkaBatch.config.fairness_weighted_concurrency = false
      html = get("/weights").last.join
      expect(html).to include("Weights only affect ordering")
      expect(html).to include("fairness_weighted_concurrency")

      KafkaBatch.config.fairness_weighted_concurrency = true
      html = get("/weights").last.join
      expect(html).not_to include("Weights only affect ordering")
    end
  end

  describe "GET /system" do
    it "renders configuration cards with masked secrets" do
      KafkaBatch.config.redis_url = "redis://user:secret@localhost:6379/0"
      html = get("/system").last.join

      expect(html).to include("System")
      expect(html).to include("sys-grid")
      expect(html).to include("sys-card")
      expect(html).to include("Overview")
      expect(html).to include("Kafka")
      expect(html).to include("Redis")
      expect(html).to include("Fairness")
      expect(html).to include("localhost:9092")
      expect(html).not_to include("user:secret@")
      expect(html).to include("***")
    end

    it "links System from the header nav" do
      expect(get("/").last.join).to include('href="/kafka_batch/system"')
      expect(get("/").last.join).to include("⚙ System")
    end

    it "highlights System in the nav when on /system" do
      html = get("/system").last.join
      expect(html).to include('class="btn nav-active" href="/kafka_batch/system"')
    end
  end

  describe "GET /live" do
    it "shows running jobs and consumers when Redis is available" do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url          = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.track_running_jobs = true
      KafkaBatch::Liveness.reset!
      KafkaBatchSpec::RedisHelper.flush!

      KafkaBatch::Liveness.heartbeat(topic: "test.success")
      KafkaBatch::Liveness.job_started(job_id: "live-1", batch_id: "b1", worker_class: "ProcessUserWorker", topic: "test.success", partition: 0)

      html = get("/live").last.join
      expect(html).to include("Active consumers")
      expect(html).to include("Running jobs")
      expect(html).to include("RAM")
      expect(html).to include("CPU")
      expect(html).to include("ProcessUserWorker")
      expect(html).to include("live-1"[0, 8])
      # dashboard links to it
      expect(get("/").last.join).to include("/kafka_batch/live")
    end

    it "shows an unavailable message when Redis liveness is down" do
      allow(KafkaBatch::Liveness).to receive(:available?).and_return(false)
      html = get("/live").last.join
      expect(html).to include("requires Redis")
    end

    it "/live, /lag and /fairness include the Live toggle" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(false)
      %w[/live /lag /fairness /system].each do |path|
        expect(get(path).last.join).to include('id="kb-live-toggle"')
      end
    end
  end

  describe "GET /batches/:id" do
    it "renders the batch detail" do
      id = seed(total: 5)
      status, _h, body = get("/batches/#{id}")
      expect(status).to eq(200)
      expect(body.join).to include(id[0, 8])
    end

    it "404s for an unknown batch" do
      status, = get("/batches/does-not-exist")
      expect(status).to eq(404)
    end

    it "shows recorded failures for the batch" do
      id = seed(total: 2)
      KafkaBatch.store.record_failure(
        batch_id: id, job_id: "jx", worker_class: "ProcessUserWorker",
        error_class: "RuntimeError", error_message: "kaboom"
      )

      html = get("/batches/#{id}").last.join
      expect(html).to include("Job failures")
      expect(html).to include("RuntimeError")
      expect(html).to include("kaboom")
    end
  end

  describe "CSRF protection" do
    it "rejects a forged POST without the CSRF cookie" do
      id = seed
      status, = post("/batches/#{id}/delete", csrf: false)
      expect(status).to eq(403)
      expect(KafkaBatch.store.find_batch(id)).not_to be_nil
    end

    it "rejects a POST when the submitted token does not match the cookie" do
      id = seed
      token = SecureRandom.hex(16)
      env = {
        "REQUEST_METHOD" => "POST", "PATH_INFO" => "/batches/#{id}/delete",
        "SCRIPT_NAME" => "/kafka_batch",
        "QUERY_STRING" => "_csrf=wrong",
        "HTTP_COOKIE" => "#{KafkaBatch::Web::CSRF_COOKIE}=#{token}"
      }
      status, = KafkaBatch::Web.call(env)
      expect(status).to eq(403)
      expect(KafkaBatch.store.find_batch(id)).not_to be_nil
    end

    it "rejects delete_all without a CSRF cookie (cross-site forgery vector)" do
      a = seed
      b = seed
      status, = post("/batches/bulk", body: "bulk_action=delete_all", csrf: false)
      expect(status).to eq(403)
      expect(KafkaBatch.store.find_batch(a)).not_to be_nil
      expect(KafkaBatch.store.find_batch(b)).not_to be_nil
    end

    it "does not execute XSS in bulk confirm labels from the search param" do
      html = get("/", query: "q=%22%3E%3Cimg%20src=x%20onerror=alert(1)%3E").last.join
      expect(html).not_to match(/onsubmit="return confirm\('[^']*"><img/)
      expect(html).to include("&quot;&gt;&lt;img")
    end

    it "accepts the CSRF token from the POST body (not just the query string)" do
      id = seed
      token = SecureRandom.hex(16)
      env = {
        "REQUEST_METHOD" => "POST", "PATH_INFO" => "/batches/#{id}/delete",
        "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => "",
        "HTTP_COOKIE" => "#{KafkaBatch::Web::CSRF_COOKIE}=#{token}",
        "rack.input" => StringIO.new("_csrf=#{token}"),
        "CONTENT_TYPE" => "application/x-www-form-urlencoded"
      }
      status, = KafkaBatch::Web.call(env)
      expect(status).to eq(302)
      expect(KafkaBatch.store.find_batch(id)).to be_nil
    end

    it "does not embed the CSRF token in form action URLs (no Referer/log leak)" do
      id = seed
      html = get("/batches/#{id}").last.join
      expect(html).to include(%(name="#{KafkaBatch::Web::CSRF_FIELD}"))
      expect(html).not_to match(/action=['"][^'"]*#{KafkaBatch::Web::CSRF_FIELD}=/)
    end
  end

  describe "CSRF cookie hardening" do
    it "sets SameSite=Strict and HttpOnly on the CSRF cookie" do
      _s, headers, = get("/")
      cookie = Array(headers["set-cookie"]).join("\n")
      expect(cookie).to include("SameSite=Strict")
      expect(cookie).to include("HttpOnly")
    end

    it "adds Secure only on HTTPS requests" do
      _s, headers, = KafkaBatch::Web.call(
        "REQUEST_METHOD" => "GET", "PATH_INFO" => "/",
        "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => "",
        "rack.url_scheme" => "https"
      )
      expect(Array(headers["set-cookie"]).join).to include("Secure")

      _s2, h2, = get("/")
      expect(Array(h2["set-cookie"]).join).not_to include("Secure")
    end
  end

  describe "optional web_authenticator" do
    it "returns 401 when the authenticator rejects the request" do
      KafkaBatch.config.web_authenticator = ->(_env) { false }
      status, = get("/")
      expect(status).to eq(401)
    end

    it "serves normally when the authenticator allows the request" do
      KafkaBatch.config.web_authenticator = ->(_env) { true }
      status, = get("/")
      expect(status).to eq(200)
    end

    it "denies (401) when the authenticator raises" do
      KafkaBatch.config.web_authenticator = ->(_env) { raise "boom" }
      status, = get("/")
      expect(status).to eq(401)
    end
  end

  describe "status filter validation" do
    it "ignores an unknown status value instead of forwarding it to the store" do
      expect(KafkaBatch.store).to receive(:list_batches)
        .with(hash_including(status: nil)).and_return([])
      get("/", query: "status=%27%20OR%201=1")
    end
  end

  describe "POST /batches/bulk" do
    it "cancels multiple batches and redirects back with filters preserved" do
      a = seed
      b = seed
      body = "batch_ids=#{CGI.escape(a)}&batch_ids=#{CGI.escape(b)}&bulk_action=cancel&return_status=running"
      status, headers, = post("/batches/bulk", body: body)

      expect(status).to eq(302)
      expect(headers["location"]).to eq("/kafka_batch/?status=running")
      expect(KafkaBatch.store.find_batch(a)[:status]).to eq("cancelled")
      expect(KafkaBatch.store.find_batch(b)[:status]).to eq("cancelled")
    end

    it "deletes multiple batches" do
      a = seed
      b = seed
      body = "batch_ids=#{CGI.escape(a)}&batch_ids=#{CGI.escape(b)}&bulk_action=delete"
      post("/batches/bulk", body: body)

      expect(KafkaBatch.store.find_batch(a)).to be_nil
      expect(KafkaBatch.store.find_batch(b)).to be_nil
    end

    it "cancels all running batches matching the index scope" do
      running = seed
      done    = seed(total: 1)
      KafkaBatch.store.record_completion_by_offset(
        batch_id: done, source_topic: "t", source_partition: 0,
        job_id: "j1", batch_seq: 1, source_offset: 1, status: "success"
      )
      KafkaBatch.store.seal_batch(done)

      body = "bulk_action=cancel_all&scope_status=running"
      post("/batches/bulk", body: body)

      expect(KafkaBatch.store.find_batch(running)[:status]).to eq("cancelled")
      expect(KafkaBatch.store.find_batch(done)[:status]).to eq("success")
    end

    it "deletes all batches matching the index scope" do
      a = seed
      b = seed
      body = "bulk_action=delete_all"
      post("/batches/bulk", body: body)

      expect(KafkaBatch.store.find_batch(a)).to be_nil
      expect(KafkaBatch.store.find_batch(b)).to be_nil
    end
  end

  describe "POST /batches/:id/cancel" do
    it "cancels the batch and redirects" do
      id = seed
      status, headers, = post("/batches/#{id}/cancel")

      expect(status).to eq(302)
      expect(headers["location"]).to eq("/kafka_batch/")
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("cancelled")
    end

    it "does not flip a non-running (e.g. succeeded) batch to cancelled" do
      id = seed
      KafkaBatch.store.update_batch_status(id, "success")
      status, = post("/batches/#{id}/cancel")

      expect(status).to eq(302)
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("success")
    end

    it "404s when the batch does not exist" do
      status, = post("/batches/does-not-exist/cancel")
      expect(status).to eq(404)
    end
  end

  describe "POST /batches/:id/delete" do
    it "deletes the batch and redirects" do
      id = seed
      status, _h, = post("/batches/#{id}/delete")

      expect(status).to eq(302)
      expect(KafkaBatch.store.find_batch(id)).to be_nil
    end

    it "404s when the batch does not exist" do
      status, = post("/batches/does-not-exist/delete")
      expect(status).to eq(404)
    end
  end

  it "404s unknown routes" do
    status, = get("/nope")
    expect(status).to eq(404)
  end

  describe "GET /audit" do
    it "shows a disabled notice and no nav link when audit is off" do
      status, _h, body = get("/audit")
      html = body.join
      expect(status).to eq(200)
      expect(html).to include("audit log is disabled")
      expect(html).not_to include(">📝 Audit<")
    end

    context "when enabled" do
      before do
        KafkaBatchSpec::ActiveRecordSupport.establish!
        KafkaBatchSpec::ActiveRecordSupport.truncate!
        KafkaBatch::AuditLog.reset!
        KafkaBatch.config.audit_enabled = true
      end

      after do
        KafkaBatch::AuditLog.reset!
        KafkaBatch.config.audit_enabled = false
      end

      it "renders recorded actions newest-first with a nav link and filter chips" do
        KafkaBatch::AuditLog.record(action: "batches.cancel", path: "/batches/a/cancel", status: "ok", metadata: { "n" => 1 })
        KafkaBatch::AuditLog.record(action: "lag.pause", path: "/lag/pause", status: "error", metadata: { "n" => 2 })

        status, _h, body = get("/audit")
        html = body.join
        expect(status).to eq(200)
        expect(html).to include(">📝 Audit<")                 # nav link present
        expect(html).to include("chip")                        # action filter chips
        # Assert on the request paths, which appear only in table rows (not chips).
        expect(html).to include("/lag/pause").and include("/batches/a/cancel")
        expect(html.index("/lag/pause")).to be < html.index("/batches/a/cancel")  # newest first
      end

      it "filters by action" do
        KafkaBatch::AuditLog.record(action: "batches.delete", path: "/batches/a/delete", status: "ok")
        KafkaBatch::AuditLog.record(action: "lag.resume", path: "/lag/resume", status: "ok")

        html = get("/audit", query: "action=lag.resume").last.join
        expect(html).to include("/lag/resume")           # kept row
        expect(html).not_to include("/batches/a/delete") # filtered-out row (path only in rows)
      end
    end
  end

  describe "time formatting" do
    let(:web) { described_class.new }

    it "renders times as UTC in 24-hour format with an explicit suffix" do
      expect(web.send(:fmt_time, Time.utc(2026, 6, 27, 20, 19, 44))).to eq("2026-06-27 20:19:44 UTC")
      expect(web.send(:fmt_time, Time.new(2026, 6, 27, 22, 19, 44, "+02:00"))).to eq("2026-06-27 20:19:44 UTC")
      expect(web.send(:fmt_time, "2026-06-27T20:19:44Z")).to eq("2026-06-27 20:19:44 UTC")
      expect(web.send(:fmt_time, nil)).to eq("—")
      expect(web.send(:fmt_time, "")).to eq("—")
    end

    it "renders UTC timestamps on the batch detail page" do
      id = seed(total: 1)
      html = get("/batches/#{id}").last.join
      expect(html).to match(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC/)
    end

    it "formats next-retry ETA as a human duration" do
      expect(web.send(:fmt_eta, Time.now + 90)).to match(/\Ain 1m \d+s\z/)
      expect(web.send(:fmt_eta, Time.now + 3 * 3600 + 120)).to match(/\Ain 3h \d+m\z/)
      expect(web.send(:fmt_eta, Time.now - 5)).to eq("due now")
      expect(web.send(:fmt_eta, nil)).to eq("—")
    end
  end

  it "shows the next-retry ETA for a retrying failure" do
    id = seed(total: 1)
    KafkaBatch.store.record_failure(
      batch_id: id, job_id: "jr", worker_class: "W", error_class: "Flaky",
      error_message: "x", attempt: 0, status: "retrying", next_retry_at: Time.now + 7200
    )
    html = get("/batches/#{id}").last.join
    expect(html).to include("Next retry")
    expect(html).to match(/in \d+h/)
  end

  describe "GET /scheduled" do
    let(:sched) do
      instance_double(KafkaBatch::Schedule::RedisStore,
                      size: 2,
                      list: [
                        { job_id: "aaaa1111", partition: 3, offset: 42, run_at: Time.now + 300, batch_id: nil },
                        { job_id: "bbbb2222", partition: 1, offset: 7,  run_at: Time.now + 600, batch_id: "bx" }
                      ])
    end

    before { allow(KafkaBatch).to receive(:schedule_store).and_return(sched) }

    it "shows the pending counter and the next job_id:partition:offset pointers" do
      status, _headers, body = get("/scheduled")
      html = body.join

      expect(status).to eq(200)
      expect(html).to include("Pending scheduled")
      expect(html).to include("2")                    # counter
      expect(html).to include("aaaa1111:3:42")        # pointer member
      expect(html).to include("bbbb2222:1:7")
    end

    it "searches by job_id" do
      allow(sched).to receive(:find).with("aaaa1111").and_return(
        { job_id: "aaaa1111", partition: 3, offset: 42, run_at: Time.now + 300, batch_id: nil, state: :pending }
      )
      html = get("/scheduled", query: "q=aaaa1111").last.join

      expect(sched).to have_received(:find).with("aaaa1111")
      expect(html).to include("aaaa1111:3:42")
      expect(html).to include("Search result")
    end

    it "reports no match cleanly" do
      allow(sched).to receive(:find).and_return(nil)
      html = get("/scheduled", query: "q=missing").last.join
      expect(html).to include("No scheduled job matches")
    end
  end

  describe "GET /reconciler" do
    it "renders when no run has been recorded" do
      allow(KafkaBatch::Reconciler::RunSummary).to receive(:load_last).and_return(nil)
      allow(KafkaBatch::Reconciler::RunSummary).to receive(:load_skip).and_return(nil)
      html = get("/reconciler").last.join
      expect(html).to include("No reconciler run recorded")
    end

    it "shows last run metrics when present" do
      allow(KafkaBatch::Reconciler::RunSummary).to receive(:load_last).and_return(
        ran_at: Time.now.utc.iso8601(3),
        triggered_by: "consumer",
        duration: 0.42,
        recovered_stale: 2,
        refired_lost: 1,
        produce_failed: 0,
        found_stale: 2,
        processed_stale: 2,
        found_lost: 1,
        processed_lost: 1,
        capped_stale: "0",
        capped_lost: "0",
        skipped_stale: 0,
        details: []
      )
      allow(KafkaBatch::Reconciler::RunSummary).to receive(:load_skip).and_return(nil)
      html = get("/reconciler").last.join
      expect(html).to include("Stuck batches recovered")
      expect(html).to include("consumer")
    end
  end

  describe "GET /dead_letter" do
    it "renders unavailable state when stats cannot be loaded" do
      allow(KafkaBatch::Dlt::Stats).to receive(:fetch).and_return(nil)
      html = get("/dead_letter").last.join
      expect(html).to include("Could not read")
    end

    it "renders totals and messages when stats and page are available" do
      allow(KafkaBatch::Dlt::Stats).to receive(:fetch).and_return(
        topic: "kafka_batch.dead_letter",
        partitions: 3,
        total: 42,
        by_type: { "job" => 40 },
        sample_size: 10,
        sample_limited: true
      )
      allow(KafkaBatch::Dlt::Reader).to receive(:new).and_return(
        instance_double(
          KafkaBatch::Dlt::Reader,
          fetch_page: {
            messages: [{
              partition: 0, offset: 9, dlt_at: Time.now.iso8601, dlt_type: "job",
              worker_class: "MyWorker", batch_id: "b1", job_id: "j1",
              source_topic: "t", error_class: "RuntimeError", error_message: "boom"
            }],
            has_older: false,
            cursor_older: nil
          },
          close: nil
        )
      )
      html = get("/dead_letter").last.join
      expect(html).to include("Messages in DLT")
      expect(html).to include("42")
      expect(html).to include("MyWorker")
    end
  end
end
