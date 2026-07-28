# frozen_string_literal: true

module KafkaBatch
  module Alerts
    module Rules
      Finding = Struct.new(
        :rule_id, :fingerprint, :title, :summary, :severity, :link, :sample,
        keyword_init: true
      )

      class Base
        class << self
          attr_accessor :id, :title, :description, :detail, :remediation,
                        :default_severity, :requires, :link, :settings

          def inherited(subclass)
            super
            subclass.id = nil
            subclass.title = nil
            subclass.description = nil
            subclass.detail = nil
            subclass.remediation = nil
            subclass.default_severity = "warning"
            subclass.requires = []
            subclass.link = nil
            subclass.settings = []
          end
        end

        def initialize(config)
          @config = config
        end

        def id
          self.class.id
        end

        def enabled?
          rules = @config["rules"] || {}
          conf = rules[id] || rules[id.to_s] || {}
          return true if conf.empty?

          v = conf["enabled"]
          v.nil? ? true : !!v
        end

        def severity
          rules = @config["rules"] || {}
          conf = rules[id] || {}
          (conf["severity"] || self.class.default_severity).to_s
        end

        # @return [Array<Finding>]
        def evaluate(_sample)
          []
        end

        protected

        def finding(fingerprint:, summary:, sample: {}, link: nil, title: nil)
          Finding.new(
            rule_id: id,
            fingerprint: fingerprint,
            title: title || self.class.title,
            summary: summary,
            severity: severity,
            link: link || self.class.link,
            sample: sample
          )
        end
      end

      module_function

      def catalog
        [
          LagStuckGrowing,
          RedisRttHigh,
          NoLiveConsumers,
          ReconcilerStale,
          FairnessIngestBackedUp,
          DltRateHigh,
          ScheduleDepthHigh,
          CronStale,
          TenantErrorRateHigh
        ]
      end

      def metadata
        catalog.map do |klass|
          {
            "id" => klass.id,
            "title" => klass.title,
            "description" => klass.description,
            "detail" => klass.detail,
            "remediation" => klass.remediation,
            "default_severity" => klass.default_severity,
            "requires" => Array(klass.requires).map(&:to_s),
            "link" => klass.link,
            "settings" => Array(klass.settings)
          }
        end
      end
    end
  end
end
