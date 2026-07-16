# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Workset::Store do
  before do
    skip "Redis not available" unless KafkaBatchSpec::RedisHelper.available?
  end

  let(:store) { described_class.new }

  def claim!(job_id:, consumer_id:, steal_grace: -1, payload: nil)
    store.claim(
      job_id:      job_id,
      payload:     payload || %({"job_id":"#{job_id}"}),
      topic:       "jobs",
      partition:   0,
      offset:      1,
      consumer_id: consumer_id,
      lease_ttl:   60,
      steal_grace: steal_grace
    )
  end

  def kill_consumer!(consumer_id)
    Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
         .del("#{KafkaBatch::Workset::LIVE_CONSUMER_PREFIX}#{consumer_id}")
  end

  def age_claim!(job_id, age_sec)
    entry = store.get_entry(job_id)
    entry.claimed_at_unix = Time.now.to_i - age_sec
    entry.claimed_at = Time.at(entry.claimed_at_unix).utc.iso8601(9)
    raw = store.send(:dump_entry, entry)
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.set("#{KafkaBatch::Workset::JOB_KEY_PREFIX}#{job_id}", raw)
    r.zadd(KafkaBatch::Workset::INDEX_KEY, entry.claimed_at_unix, job_id)
  end

  it "claims, still_owned?, and completes with a fence" do
    res = claim!(job_id: "j1", consumer_id: "c1")
    expect(res.won).to eq(true)
    expect(res.fence).not_to be_empty
    expect(store.still_owned?("j1", "c1", res.fence)).to eq(true)

    store.complete("j1", "c1", res.fence)
    expect(store.still_owned?("j1", "c1", res.fence)).to eq(false)
  end

  it "sets live:consumer after claim" do
    claim!(job_id: "j-live", consumer_id: "c-live")
    n = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
             .exists("#{KafkaBatch::Workset::LIVE_CONSUMER_PREFIX}c-live")
    expect(n.to_i).to be >= 1
  end

  it "loses claim when another consumer is still live" do
    claim!(job_id: "j2", consumer_id: "alive")
    res = claim!(job_id: "j2", consumer_id: "other")
    expect(res.won).to eq(false)
  end

  it "steals from a dead consumer when grace is disabled" do
    claim!(job_id: "j3", consumer_id: "dead")
    kill_consumer!("dead")
    res = claim!(job_id: "j3", consumer_id: "alive", steal_grace: -1)
    expect(res.won).to eq(true)
  end

  it "does not steal inside orphan grace" do
    claim!(job_id: "j3g", consumer_id: "dead")
    kill_consumer!("dead")
    res = claim!(job_id: "j3g", consumer_id: "alive", steal_grace: 40)
    expect(res.won).to eq(false)
  end

  it "steals after orphan grace when heartbeat is missing" do
    claim!(job_id: "j3old", consumer_id: "dead")
    kill_consumer!("dead")
    age_claim!("j3old", 60)
    res = claim!(job_id: "j3old", consumer_id: "alive", steal_grace: 40)
    expect(res.won).to eq(true)
  end

  it "resumes the same consumer with the prior fence" do
    first = claim!(job_id: "j4", consumer_id: "c1")
    second = claim!(job_id: "j4", consumer_id: "c1")
    expect(second.won).to eq(true)
    expect(second.fence).to eq(first.fence)
  end

  it "stores runtime ruby and base64 payload compatible with Go" do
    payload = %({"job_id":"j5","x":1})
    res = claim!(job_id: "j5", consumer_id: "c1", payload: payload)
    entry = store.get_entry("j5")
    expect(entry.runtime).to eq("ruby")
    expect(entry.payload).to eq(payload.b)
    expect(res.entry.runtime).to eq("ruby")
  end

  it "renew extends ownership and fails after complete" do
    res = claim!(job_id: "j6", consumer_id: "c1")
    expect(store.renew("j6", "c1", res.fence, ttl: 60)).to eq(true)
    store.complete("j6", "c1", res.fence)
    expect(store.renew("j6", "c1", res.fence, ttl: 60)).to eq(false)
  end

  describe "orphan reclaim" do
    it "list_orphans respects grace then returns aged dead owners" do
      claim!(job_id: "j5g", consumer_id: "gone")
      kill_consumer!("gone")
      expect(store.list_orphans(limit: 10, grace: 40)).to be_empty

      age_claim!("j5g", 60)
      orphans = store.list_orphans(limit: 10, grace: 40)
      expect(orphans.map(&:job_id)).to eq(["j5g"])
    end

    it "reclaim_orphans re-produces with _reclaim and drops ownership" do
      claim!(job_id: "j5", consumer_id: "gone", payload: %({"job_id":"j5","worker_class":"W"}))
      kill_consumer!("gone")
      age_claim!("j5", 60)

      produced = []
      producer = ->(topic, key, body) { produced << { topic: topic, key: key, body: body } }
      res = store.reclaim_orphans(producer: producer, limit: 10, grace: -1)
      expect(res.reclaimed).to eq(1)
      expect(produced.size).to eq(1)
      expect(produced.first[:topic]).to eq("jobs")
      expect(Oj.load(produced.first[:body])["_reclaim"]).to eq(true)
      expect(store.get_entry("j5")).to be_nil
    end

    it "gzip-compresses large workset payloads and reclaim expands them" do
      big = ({ "job_id" => "j-gz", "worker_class" => "W", "pad" => ("x" * 300) }).to_json
      claim!(job_id: "j-gz", consumer_id: "gone", payload: big)
      entry = store.get_entry("j-gz")
      expect(entry.encoding).to eq("gzip")
      expect(entry.payload.bytesize).to be < big.bytesize

      kill_consumer!("gone")
      age_claim!("j-gz", 60)
      produced = []
      producer = ->(topic, key, body) { produced << body }
      res = store.reclaim_orphans(producer: producer, limit: 10, grace: -1)
      expect(res.reclaimed).to eq(1)
      parsed = Oj.load(produced.first)
      expect(parsed["_reclaim"]).to eq(true)
      expect(parsed["job_id"]).to eq("j-gz")
    end

    it "finish-only path does not double-produce after MarkProduced" do
      claim_res = claim!(job_id: "j-idem", consumer_id: "gone")
      kill_consumer!("gone")
      age_claim!("j-idem", 60)

      produces = 0
      producer = ->(_t, _k, _b) { produces += 1 }
      store.mark_produced("j-idem", claim_res.fence)
      store.abort_reclaim("j-idem")

      res = store.reclaim_orphans(producer: producer, limit: 10, grace: -1)
      expect(produces).to eq(0)
      expect(res.reclaimed).to eq(1)
      expect(store.get_entry("j-idem")).to be_nil
    end

    it "aborts reclaim lock when produce fails" do
      claim!(job_id: "j-fail", consumer_id: "gone")
      kill_consumer!("gone")
      age_claim!("j-fail", 60)

      producer = ->(_t, _k, _b) { raise "broker down" }
      res = store.reclaim_orphans(producer: producer, limit: 10, grace: -1)
      expect(res.failed).to eq(1)
      expect(store.get_entry("j-fail")).not_to be_nil
      # Lock released so a later sweep can retry.
      expect(store.begin_reclaim("j-fail")).to eq(true)
    end
  end
end
