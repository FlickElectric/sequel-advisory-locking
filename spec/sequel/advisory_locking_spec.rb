# frozen_string_literal: true

require 'spec_helper'

class AdvisoryLockingSpec < Minitest::Spec
  def advisory_locks
    DB[:pg_locks].where(locktype: 'advisory')
  end

  def locked_ids
    advisory_locks.select_order_map(Sequel.lit('(classid::bigint << 32) + objid::bigint'))
  end

  def sleep_until(timeout = 2)
    deadline = Time.now + timeout
    loop do
      break if yield
      raise "Thing never happened!" if Time.now > deadline
      sleep 0.01
    end
  end

  before { assert_empty advisory_locks }
  after  { assert_empty advisory_locks }

  it "should advisory lock an integer derived from the key inside the block" do
    assert_empty advisory_locks

    val = DB.advisory_lock 'key' do
      assert_equal [4354430579665871434], locked_ids
      'blah'
    end

    assert_equal 'blah', val
    assert_empty advisory_locks
  end

  it "without a block should take the lock and immediately release it, returning true" do
    assert_equal true, DB.advisory_lock('key')
  end

  it "should release the lock if an error is thrown" do
    error = assert_raises { DB.advisory_lock('key'){raise "Blah"} }
    assert_equal "Blah", error.message
  end

  it "should block if the lock isn't available" do
    DB.synchronize do
      i = 0

      DB.get{pg_advisory_lock(4354430579665871434)}

      thread = Thread.new { DB.advisory_lock('key'){i += 1} }

      sleep 0.05
      assert_equal 0, i

      DB.get{pg_advisory_unlock(4354430579665871434)}
      sleep_until { i == 1 }
      thread.join
    end
  end

  it "when the try option is given should return false immediately if the lock isn't available" do
    q1, q2 = Queue.new, Queue.new

    t1 = Thread.new do
      assert_equal true, DB.advisory_lock('key', try: true)
      assert_empty advisory_locks
      assert_equal 'blah', DB.advisory_lock('key', try: true){ q1.push(nil); q2.pop; 'blah'}
      assert_empty advisory_locks
    end

    t2 = Thread.new do
      q1.pop
      assert_equal [4354430579665871434], locked_ids
      assert_equal false, DB.advisory_lock('key', try: true)
      assert_equal [4354430579665871434], locked_ids
      assert_equal false, DB.advisory_lock('key', try: true){raise "Should not get here!"}
      assert_equal [4354430579665871434], locked_ids
      q2.push(nil)
    end

    t1.join
    t2.join
  end

  it "should raise an error if passed a bad key" do
    error = assert_raises(Sequel::AdvisoryLocking::Error){DB.advisory_lock(Object.new)}
    assert_equal "passed an invalid key type (Object)", error.message
  end
end
