module KafkaBatchSpec
  # Records callback invocations so specs can assert which callbacks ran.
  module CallbackDoubles
    class << self
      def reset!
        @invocations = []
      end

      def record(name, args)
        invocations << { name: name, args: args }
      end

      def invocations
        @invocations ||= []
      end
    end
  end
end

# Fires both callback methods and records them.
class RecordingCallback
  def on_success(summary)
    KafkaBatchSpec::CallbackDoubles.record(:on_success, summary)
  end

  def on_complete(summary)
    KafkaBatchSpec::CallbackDoubles.record(:on_complete, summary)
  end
end

# A callback whose methods always raise (to exercise the DLT path).
class ExplodingCallback
  def on_complete(_summary)
    raise "callback boom"
  end
end

# Resolves as a class but does not define the callback method.
class MethodlessCallback
end

# Records whether the dispatch had been claimed at the moment the callback ran.
# Used to prove claim-before-invoke: Redis fence is held while the callback runs.
class OrderCheckingCallback
  def on_complete(summary)
    flag = KafkaBatch.store.callback_dispatched?(summary["batch_id"])
    KafkaBatchSpec::CallbackDoubles.record(:dispatched_at_invocation, flag)
  end
end
