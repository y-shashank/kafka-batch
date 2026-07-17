# frozen_string_literal: true

require_relative "settings"
require_relative "chat_history"
require_relative "retriever"
require_relative "open_router"
require_relative "knowledge_index"

module KafkaBatch
  module Ai
    # RAG chat over the packaged knowledge corpus + live config snapshot.
    # Never reads/writes operational kafka-batch Redis keys.
    module Chat
      SYSTEM_PROMPT = <<~TXT.freeze
        You are the kafka-batch admin dashboard assistant.
        Answer using the provided knowledge context about kafka-batch (Ruby + Go).
        The Live configuration snapshot (source: config) is AUTHORITATIVE for THIS cluster.
        For partition counts: use live_broker_partitions / broker_partitions from that
        snapshot only. create_default_partitions / configured_partitions and any docs
        mentioning DEFAULT_PARTITIONS (e.g. 768) are create_topics defaults — not live
        cluster size. If live_broker_partitions is n/a or topic_inventory_available is
        false, say broker metadata is unavailable; do not invent a count from docs.
        If the context is insufficient for other questions, say you do not know from the
        docs — do not invent Redis keys or live metrics.
        Prefer concise, operator-focused answers. Mention relevant config knobs when useful.
        When citing, refer to section titles from the context.
      TXT

      class << self
        # @return [Hash] ok, reply, citations, history_size
        def ask(message)
          message = message.to_s.strip
          raise ArgumentError, "message is blank" if message.empty?

          api_key = Settings.api_key
          raise ArgumentError, "OpenRouter API key is not configured (AI Settings)" if api_key.nil? || api_key.empty?

          unless KnowledgeIndex.meta["corpus_version"]
            KnowledgeIndex.sync!
          end

          contexts = Retriever.search(message)
          citations = contexts.map do |c|
            { "id" => c["id"], "title" => c["title"], "source" => c["source"], "section" => c["section"] }
          end

          context_block =
            if contexts.empty?
              "(No matching knowledge chunks found. Answer only if the question is trivial; otherwise say you need docs.)"
            else
              contexts.map.with_index(1) do |c, i|
                "### Context #{i}: #{c['title']} (#{c['source']})\n#{c['text']}"
              end.join("\n\n")
            end

          recent = ChatHistory.list(limit: 12).reverse
          history_msgs = recent.map { |m| { "role" => m["role"], "content" => m["content"].to_s[0, 2000] } }

          messages = [
            { "role" => "system", "content" => SYSTEM_PROMPT },
            { "role" => "system", "content" => "Knowledge context:\n\n#{context_block}" }
          ] + history_msgs + [
            { "role" => "user", "content" => message }
          ]

          client = OpenRouter.new(
            api_key: api_key,
            model: Settings.model,
            base_url: Settings.base_url
          )
          reply = client.chat(messages: messages)

          ChatHistory.append!(role: "user", content: message)
          ChatHistory.append!(role: "assistant", content: reply, citations: citations)

          {
            "ok" => true,
            "reply" => reply,
            "citations" => citations,
            "model" => Settings.model,
            "history_size" => ChatHistory.size
          }
        end
      end
    end
  end
end
