require 'test/unit'
require 'resque'
require 'resque/plugins/lock'

class LockTest < Test::Unit::TestCase
  class Job
    extend Resque::Plugins::Lock

    def self.queue
      :lock_test
    end

    def self.perform
      raise "Woah woah woah, that wasn't supposed to happen"
    end
  end

  class SpecialEnqueueFailureHandlingJob < Job
    @@error = StandardError.new

    def self.error
      @@error
    end

    def self.handle_enqueue_failure
      raise @@error
    end
  end

  def setup
    Resque.redis.del('queue:lock_test')
    Resque.redis.del(Job.lock)
    Resque.redis.del(SpecialEnqueueFailureHandlingJob.lock)
  end

  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::Lock)
    end
  end

  def test_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 17
    assert Resque::Plugin.respond_to?(:before_enqueue_hooks)
  end

  def test_lock
    assert_equal 0, Resque.redis.llen('queue:lock_test')
    failure_count = 0

    3.times do
      begin
        Resque.enqueue(Job)
      rescue Resque::Plugins::Lock::EnqueueFailureError
        failure_count += 1
      end
    end

    assert_equal 1, Resque.redis.llen('queue:lock_test')
    assert_equal 2, failure_count
  end

  def test_handle_enqueue_failure
    assert_equal 0, Resque.redis.llen('queue:lock_test')
    failure_count = 0

    3.times do
      begin
        Resque.enqueue(SpecialEnqueueFailureHandlingJob)
      rescue => e
        assert_same SpecialEnqueueFailureHandlingJob.error, e
        failure_count += 1
      end
    end

    assert_equal 1, Resque.redis.llen('queue:lock_test')
    assert_equal 2, failure_count
  end

  def test_failure_hook_removes_lock
    Job.before_enqueue_lock
    assert Resque.redis.exists(Job.lock)
    Job.on_failure_lock(RuntimeError.new)
    assert !Resque.redis.exists(Job.lock)
  end
end
