module Resque
  module Plugins
    # If you want only one instance of your job queued at a time,
    # extend it with this module.
    #
    # For example:
    #
    # require 'resque/plugins/lock'
    #
    # class UpdateNetworkGraph
    #   extend Resque::Plugins::Lock
    #
    #   def self.perform(repo_id)
    #     heavy_lifting
    #   end
    # end
    #
    # No other UpdateNetworkGraph jobs will be placed on the queue,
    # the QueueLock class will check Redis to see if any others are
    # queued with the same arguments before queueing. If another
    # is queued the enqueue will be aborted.
    #
    # If you want to define the key yourself you can override the
    # `lock` class method in your subclass, e.g.
    #
    # class UpdateNetworkGraph
    #   extend Resque::Plugins::Lock
    #
    #   # Run only one at a time, regardless of repo_id.
    #   def self.lock(repo_id)
    #     "network-graph"
    #   end
    #
    #   def self.perform(repo_id)
    #     heavy_lifting
    #   end
    # end
    #
    # The above modification will ensure only one job of class
    # UpdateNetworkGraph is running at a time, regardless of the
    # repo_id. Normally a job is locked using a combination of its
    # class name and arguments.
    module Lock
      class EnqueueFailureError < StandardError; end

      # Override in your job to control the lock key. It is
      # passed the same arguments as `perform`, that is, your job's
      # payload.
      def lock(*args)
        "lock:#{name}-#{args.join('-')}"
      end

      def before_enqueue_lock(*args)
        lock_key = lock(*args)
        unless success = Resque.redis.setnx(lock_key, Time.now)
          lock_timestamp = Resque.redis.get(lock_key)
          handle_enqueue_failure(lock_key, lock_timestamp)
        end
        success
      end

      def around_perform_lock(*args)
        begin
          yield
        ensure
          # Always clear the lock when we're done, even if there is an
          # error.
          Resque.redis.del(lock(*args))
        end
      end

      def on_failure_lock(error, *args)
        Resque.redis.del(lock(*args))
      end

      # Override in your job to do something on enqueue failure
      def handle_enqueue_failure(lock_key, lock_timestamp)
      end
    end
  end
end

