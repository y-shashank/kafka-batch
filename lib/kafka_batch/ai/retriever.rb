# frozen_string_literal: true

require "set"

module KafkaBatch
  module Ai
    # Lexical retrieval over packaged knowledge chunks already in Redis.
    # Docs-only — never touches operational ledger/fairness/workset keys.
    module Retriever
      STOP = Set.new(%w[
        a an the and or of to in on for is are was were be been being
        this that these those it its with from as by at if how what why
        when who whom which can could should would will just not no yes
      ]).freeze

      class << self
        def search(query, limit: nil)
          limit ||= KafkaBatch.config.ai_chat_context_chunks.to_i
          limit = 6 if limit <= 0
          tokens = tokenize(query)
          return [] if tokens.empty?

          ids = KnowledgeIndex.chunk_ids
          scored = []
          ids.each do |id|
            next if id == KnowledgeIndex::LIVE_CONFIG_CHUNK_ID

            chunk = KnowledgeIndex.fetch_chunk(id)
            next unless chunk.is_a?(Hash)

            text = chunk["text"].to_s
            next if text.empty?

            score = score_text(tokens, text)
            next if score <= 0

            scored << {
              "id" => id,
              "title" => chunk["title"],
              "source" => chunk["source"],
              "section" => chunk["section"],
              "score" => score,
              "text" => text
            }
          end

          # Always include live config first — authoritative for this cluster
          # (broker partition counts, knobs). Docs alone often cite create defaults.
          live = KnowledgeIndex.fetch_chunk(KnowledgeIndex::LIVE_CONFIG_CHUNK_ID)
          scored.sort_by! { |c| -c["score"] }
          top = scored.first(limit)
          if live.is_a?(Hash) && !live["text"].to_s.empty?
            top = top.reject { |c| c["id"] == KnowledgeIndex::LIVE_CONFIG_CHUNK_ID }
            top = top.first([limit - 1, 0].max)
            top.unshift(
              "id" => KnowledgeIndex::LIVE_CONFIG_CHUNK_ID,
              "title" => live["title"],
              "source" => "config",
              "section" => "Live configuration",
              "score" => 1_000_000,
              "text" => live["text"]
            )
          end
          top
        end

        private

        def tokenize(text)
          text.to_s.downcase.scan(/[a-z0-9_]{2,}/).reject { |t| STOP.include?(t) }.uniq
        end

        def score_text(tokens, text)
          hay = text.downcase
          score = 0.0
          tokens.each do |t|
            count = hay.scan(t).size
            next if count.zero?

            score += Math.log2(1 + count) * (t.length >= 6 ? 1.5 : 1.0)
          end
          score
        end
      end
    end
  end
end
