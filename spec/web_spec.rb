# frozen_string_literal: true

require "json"
require "base64"
require "set"

RSpec.describe KafkaBatch::Web do
  def get(path, query: "")
    KafkaBatch::Web.call(
      "REQUEST_METHOD" => "GET", "PATH_INFO" => path,
      "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => query
    )
  end

  def post(path, query: "", body: nil, csrf: true, json: nil, headers: {})
    mutate("POST", path, query: query, body: body, csrf: csrf, json: json, headers: headers)
  end

  def delete_req(path, query: "", csrf: true, json: nil)
    mutate("DELETE", path, query: query, csrf: csrf, json: json)
  end

  def put(path, query: "", csrf: true, json: nil)
    mutate("PUT", path, query: query, csrf: csrf, json: json)
  end

  def mutate(method, path, query: "", body: nil, csrf: true, json: nil, headers: {})
    token = SecureRandom.hex(16)
    qs = query.to_s
    if csrf && json.nil? && body.nil?
      qs = qs.empty? ? "_csrf=#{token}" : "#{qs}&_csrf=#{token}" unless qs.include?("_csrf=")
    end
    env = {
      "REQUEST_METHOD" => method, "PATH_INFO" => path,
      "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => qs
    }
    env["HTTP_COOKIE"] = "#{KafkaBatch::Web::CSRF_COOKIE}=#{token}" if csrf
    headers.each { |k, v| env[k] = v }
    if json
      env["HTTP_X_CSRF_TOKEN"] = token if csrf
      env["rack.input"] = StringIO.new(JSON.generate(json))
      env["CONTENT_TYPE"] = "application/json"
    elsif body
      env["rack.input"] = StringIO.new(body)
      env["CONTENT_TYPE"] = "application/x-www-form-urlencoded"
    end
    KafkaBatch::Web.call(env)
  end

  def json_body(response)
    JSON.parse(response.last.join)
  end

  def seed(total: 3, **opts)
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  before { allow(KafkaBatch::Lag).to receive(:available?).and_return(false) }

  describe "GET / (SPA shell)" do
    it "marks dashboard responses no-store so a refresh always shows current data" do
      _s, headers, = get("/")
      expect(headers["cache-control"]).to eq("no-store")
    end

    it "serves the React SPA shell with mount + CSRF bootstrap" do
      status, headers, body = get("/")
      html = body.join

      expect(status).to eq(200)
      expect(headers["content-type"]).to match(%r{text/html})
      expect(html).to include("window.__KB_MOUNT__")
      expect(html).to include("/kafka_batch")
      expect(html).to include("window.__KB_CSRF__")
      expect(html).to include('rel="icon" type="image/svg+xml"')
      expect(html).to include("assets/")
    end

    it "embeds an inline SVG favicon data URI" do
      expect(Base64.strict_decode64(KafkaBatch::Web::FAVICON_DATA_URI.split(",", 2).last))
        .to include("<svg").and include("</svg>")
    end
  end

  describe "GET /api/bootstrap" do
    it "returns csrf token and nav flags" do
      status, headers, body = get("/api/bootstrap")
      payload = JSON.parse(body.join)

      expect(status).to eq(200)
      expect(headers["cache-control"]).to eq("no-store")
      expect(headers["content-type"]).to match(%r{application/json})
      expect(payload["ok"]).to eq(true)
      expect(payload["csrf_token"]).to match(/\A[a-f0-9]{32}\z/)
      expect(payload["mount"]).to eq("/kafka_batch")
      expect(payload).to have_key("audit_enabled")
      expect(payload["version"]).to eq(KafkaBatch::VERSION)
    end
  end

  describe "GET /api/dashboard + /api/batches" do
    it "reflects live store counts on each request" do
      seed(total: 5)
      first = json_body(get("/api/dashboard"))["total"]
      seed(total: 3)
      second = json_body(get("/api/dashboard"))["total"]
      expect(second).to be > first
    end

    it "lists batches with metrics fields" do
      id = seed(on_complete: "RecordingCallback", description: "Important nightly batch")
      status, _h, body = get("/api/batches")
      payload = JSON.parse(body.join)

      expect(status).to eq(200)
      expect(payload["batches"].map { |b| b["id"] }).to include(id)
      row = payload["batches"].find { |b| b["id"] == id }
      expect(row["description"]).to eq("Important nightly batch")
      expect(row["short_id"]).to eq(id[0, 8])
    end

    it "filters by status whitelist" do
      running = seed
      KafkaBatch.store.update_batch_status(running, "success")
      other = seed
      payload = json_body(get("/api/batches", query: "status=running"))
      ids = payload["batches"].map { |b| b["id"] }
      expect(ids).to include(other)
      expect(ids).not_to include(running)
    end

    it "ignores unknown status filters" do
      id = seed
      payload = json_body(get("/api/batches", query: "status=not_a_status';DROP TABLE"))
      expect(payload["status"]).to be_nil
      expect(payload["batches"].map { |b| b["id"] }).to include(id)
    end
  end

  describe "GET /api/batches/:id" do
    it "returns batch detail" do
      id = seed(description: "Detail me")
      payload = json_body(get("/api/batches/#{id}"))
      expect(payload["batch"]["id"]).to eq(id)
      expect(payload["batch"]["description"]).to eq("Detail me")
    end

    it "returns 404 for unknown batch" do
      status, = get("/api/batches/#{SecureRandom.uuid}")
      expect(status).to eq(404)
    end
  end

  describe "mutations CSRF" do
    it "rejects a forged POST without the CSRF cookie" do
      id = seed
      status, = post("/api/batches/#{id}/cancel", csrf: false, json: {})
      expect(status).to eq(403)
    end

    it "rejects a POST when the submitted token does not match the cookie" do
      id = seed
      token = SecureRandom.hex(16)
      env = {
        "REQUEST_METHOD" => "POST", "PATH_INFO" => "/api/batches/#{id}/cancel",
        "SCRIPT_NAME" => "/kafka_batch", "QUERY_STRING" => "",
        "HTTP_COOKIE" => "#{KafkaBatch::Web::CSRF_COOKIE}=#{token}",
        "HTTP_X_CSRF_TOKEN" => "deadbeef",
        "CONTENT_TYPE" => "application/json",
        "rack.input" => StringIO.new("{}")
      }
      status, = KafkaBatch::Web.call(env)
      expect(status).to eq(403)
    end

    it "accepts CSRF from X-CSRF-Token header with JSON body" do
      id = seed
      status, = post("/api/batches/#{id}/cancel", json: {})
      expect(status).to eq(200)
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("cancelled")
    end
  end

  describe "POST /api/batches/:id/cancel" do
    it "cancels only running batches" do
      id = seed
      KafkaBatch.store.update_batch_status(id, "success")
      post("/api/batches/#{id}/cancel", json: {})
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("success")
    end

    it "cancels a running batch" do
      id = seed
      status, = post("/api/batches/#{id}/cancel", json: {})
      expect(status).to eq(200)
      expect(KafkaBatch.store.find_batch(id)[:status]).to eq("cancelled")
    end
  end

  describe "DELETE /api/batches/:id" do
    it "deletes a batch" do
      id = seed
      status, = delete_req("/api/batches/#{id}", json: {})
      expect(status).to eq(200)
      expect(KafkaBatch.store.find_batch(id)).to be_nil
    end

    it "returns 404 for unknown batch" do
      status, = delete_req("/api/batches/#{SecureRandom.uuid}", json: {})
      expect(status).to eq(404)
    end
  end

  describe "POST /api/batches/bulk" do
    it "cancels selected batch ids" do
      a = seed
      b = seed
      status, = post("/api/batches/bulk", json: { bulk_action: "cancel", batch_ids: [a, b] })
      expect(status).to eq(200)
      expect(KafkaBatch.store.find_batch(a)[:status]).to eq("cancelled")
      expect(KafkaBatch.store.find_batch(b)[:status]).to eq("cancelled")
    end

    it "delete_all respects BULK_ALL_MAX note when many match" do
      allow(KafkaBatch::Web).to receive(:const_get).and_call_original
      stub_const("KafkaBatch::Web::BULK_ALL_MAX", 2)
      stub_const("KafkaBatch::Web::FILTER_SCAN_MAX", 3)
      3.times { seed }
      payload = json_body(post("/api/batches/bulk", json: { bulk_action: "delete_all" }))
      expect(payload["ok"]).to eq(true)
      expect(payload["bulk_note"]).to include("Processed first")
    end
  end

  describe "GET /api/lag" do
    it "degrades gracefully when Lag is unavailable" do
      payload = json_body(get("/api/lag"))
      expect(payload["ok"]).to eq(true)
      expect(payload["available"]).to eq(false)
      expect(payload["message"]).to include("Karafka")
    end

    it "pauses a topic via POST JSON" do
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch::Lag).to receive(:partitions).and_return([])
      allow(KafkaBatch::Lag).to receive(:topics).and_return([])
      allow(KafkaBatch::Lag).to receive(:total).and_return(0)
      allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(true)
      allow(KafkaBatch::ConsumptionControl).to receive(:snapshot).and_return({ topics: Set.new, partitions: Set.new })
      expect(KafkaBatch::ConsumptionControl).to receive(:pause_topic).with(group: "g1", topic: "t1").and_return(true)

      status, = post("/api/lag/pause", json: { scope: "topic", group: "g1", topic: "t1" })
      expect(status).to eq(200)
    end
  end

  describe "GET /api/fairness/time" do
    it "returns unavailable payload when Lag is off" do
      payload = json_body(get("/api/fairness/time"))
      expect(payload["ok"]).to eq(true)
      expect(payload["available"]).to eq(false)
    end
  end

  describe "GET /api/weights" do
    it "returns JSON for time weights (scheduler may be unavailable)" do
      payload = json_body(get("/api/weights/time"))
      expect(payload["ok"]).to eq(true)
      expect(payload).to have_key("available")
    end
  end

  describe "GET /api/system" do
    it "masks redis credentials when present in sections" do
      allow(KafkaBatch.config).to receive(:redis_url).and_return("redis://user:secret@localhost:6379/0")
      payload = json_body(get("/api/system"))
      blob = JSON.generate(payload)
      expect(blob).not_to include("user:secret@")
      expect(payload["ok"]).to eq(true)
      # SystemInfo masks password segments as *** whenever a redis URL is shown.
      redis_values = payload["sections"].flat_map { |s| s["rows"].map { |r| r["value"].to_s } }
      expect(redis_values.any? { |v| v.include?("***") || v.include?("redis://") }).to eq(true)
    end
  end


  describe "GET /api/live" do
    it "returns a structured payload" do
      payload = json_body(get("/api/live"))
      expect(payload["ok"]).to eq(true)
      expect(payload).to have_key("consumers")
      expect(payload).to have_key("running_jobs")
    end
  end

  describe "GET /api/failures" do
    it "lists failures" do
      payload = json_body(get("/api/failures", query: "status=retrying"))
      expect(payload["ok"]).to eq(true)
      expect(payload["failures"]).to be_a(Array)
    end
  end

  describe "GET /api/scheduled" do
    it "returns scheduled payload" do
      payload = json_body(get("/api/scheduled"))
      expect(payload["ok"]).to eq(true)
    end
  end

  describe "GET /api/reconciler" do
    it "returns reconciler summary" do
      payload = json_body(get("/api/reconciler"))
      expect(payload["ok"]).to eq(true)
    end
  end

  describe "GET /api/dead_letter" do
    it "returns DLT payload without raising" do
      payload = json_body(get("/api/dead_letter"))
      expect(payload["ok"]).to eq(true)
    end
  end

  describe "GET /api/audit" do
    it "reports disabled when audit is off" do
      allow(KafkaBatch.config).to receive(:audit_enabled).and_return(false)
      payload = json_body(get("/api/audit"))
      expect(payload["enabled"]).to eq(false)
    end
  end

  describe "authentication" do
    it "returns 401 when web_authenticator rejects" do
      allow(KafkaBatch.config).to receive(:web_authenticator).and_return(->(_env) { false })
      status, headers, = get("/api/bootstrap")
      expect(status).to eq(401)
      expect(headers["www-authenticate"]).to include("Basic")
    end
  end

  describe "CSRF cookie flags" do
    it "sets SameSite=Strict HttpOnly cookie" do
      _s, headers, = get("/api/bootstrap")
      cookie = headers["set-cookie"].to_s
      expect(cookie).to include("#{KafkaBatch::Web::CSRF_COOKIE}=")
      expect(cookie).to include("SameSite=Strict")
      expect(cookie).to include("HttpOnly")
      expect(cookie).to include("Path=/kafka_batch/")
    end
  end
end
