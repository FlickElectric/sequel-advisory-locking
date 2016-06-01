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

  def sqls
    Thread.current[:sqls] ||= []
  end

  def assert_sqls(expected)
    assert_equal expected, sqls
    clear_sqls
  end

  def clear_sqls
    sqls.clear
  end

  before do
    assert_empty advisory_locks
    clear_sqls
  end

  after do
    assert_empty advisory_locks
  end

  it "should raise an error if passed a bad key" do
    error = assert_raises(Sequel::AdvisoryLocking::Error){DB.advisory_lock(Object.new)}
    assert_equal "passed an invalid key type (Object)", error.message
    assert_sqls []
  end

  describe "with a block" do
    it "should advisory lock an integer derived from the key for the duration of the block and return the result of the block" do
      val = DB.advisory_lock 'key' do
        assert_sqls ["SELECT pg_advisory_lock(4354430579665871434) -- 'key'"]
        assert_equal [4354430579665871434], locked_ids
        'blah'
      end

      assert_equal 'blah', val
      assert_empty advisory_locks
    end

    it "should release the lock successfully if a Ruby error is thrown inside the block" do
      error = assert_raises { DB.advisory_lock('key') { raise "Blah" } }
      assert_equal "Blah", error.message
      assert_sqls [
        "SELECT pg_advisory_lock(4354430579665871434) -- 'key'",
        "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'",
      ]
    end

    it "should release the lock successfully if a DB error is thrown inside the block"

    describe "with the :try option" do
      it "should lock the integer derived from the key for the duration of the block and return the result of the block"

      it "with a block should not run the block if the lock isn't available and return false"
    end
  end

  describe "without a block" do
    it "should take the lock and immediately release it, returning true" do
      assert_equal true, DB.advisory_lock('key')
      assert_sqls [
        "SELECT pg_advisory_lock(4354430579665871434) -- 'key'",
        "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'",
      ]
    end

    it "should still block if the lock isn't available"

    describe "with the :try option" do
      it "should return true or false depending on whether the lock was available" do
        q1, q2 = Queue.new, Queue.new

        t1 = Thread.new do
          assert_equal true, DB.advisory_lock('key', try: true)

          assert_sqls [
            "SELECT pg_try_advisory_lock(4354430579665871434) -- 'key'",
            "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'",
          ]

          assert_empty advisory_locks

          clear_sqls

          assert_equal 'blah', DB.advisory_lock('key', try: true){ q1.push(nil); q2.pop; 'blah'}

          assert_sqls [
            "SELECT pg_try_advisory_lock(4354430579665871434) -- 'key'",
            "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'",
          ]

          assert_empty advisory_locks
        end

        t2 = Thread.new do
          q1.pop
          assert_equal [4354430579665871434], locked_ids

          clear_sqls

          assert_equal false, DB.advisory_lock('key', try: true)

          assert_sqls [
            "SELECT pg_try_advisory_lock(4354430579665871434) -- 'key'",
          ]

          assert_equal [4354430579665871434], locked_ids

          clear_sqls

          assert_equal false, DB.advisory_lock('key', try: true){raise "Should not get here!"}

          assert_sqls [
            "SELECT pg_try_advisory_lock(4354430579665871434) -- 'key'",
          ]

          assert_equal [4354430579665871434], locked_ids
          q2.push(nil)
        end

        t1.join
        t2.join
      end
    end
  end

  it "should block until the lock is available" do
    DB.synchronize do
      i = 0

      DB.get{pg_advisory_lock(4354430579665871434)}

      thread = Thread.new do
        DB.get(1)
        clear_sqls
        DB.advisory_lock('key'){i += 1}
        assert_sqls [
          "SELECT pg_advisory_lock(4354430579665871434) -- 'key'",
          "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'"
        ]
      end

      sleep 0.05
      assert_equal 0, i

      DB.get{pg_advisory_unlock(4354430579665871434)}
      sleep_until { i == 1 }
      thread.join
    end
  end

  describe "when an error is raised" do
    describe "Sequel::Rollback" do
      it "should be reraised"
    end

    describe "DB errors inside a transaction" do
      it "should be reraised" do
        error = assert_raises Sequel::DatabaseError do
          DB.transaction do
            begin
              DB.advisory_lock 'key' do
                DB[:nonexistent_table].all
              end
            ensure
              assert_empty advisory_locks
            end
          end
        end

        assert_match /relation "nonexistent_table" does not exist/, error.message
        assert_sqls [
          "BEGIN",
          "SELECT pg_advisory_lock(4354430579665871434) -- 'key'",
          "SAVEPOINT autopoint_1",
          "ROLLBACK TO SAVEPOINT autopoint_1",
          "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'",
          "SELECT 1 AS \"one\" FROM \"pg_locks\" WHERE (\"locktype\" = 'advisory') LIMIT 1",
          "ROLLBACK"
        ]
      end

      it "in a savepoint should be reraised" do
        error = nil

        DB.transaction do
          error = assert_raises Sequel::DatabaseError do
            DB.transaction savepoint: true do
              DB.advisory_lock 'key' do
                DB[:nonexistent_table].all
              end
            end
          end

          assert_empty advisory_locks
        end

        assert_match /relation "nonexistent_table" does not exist/, error.message
        assert_sqls [
          "BEGIN",
          "SAVEPOINT autopoint_1",
          "SELECT pg_advisory_lock(4354430579665871434) -- 'key'",
          "SAVEPOINT autopoint_2",
          "ROLLBACK TO SAVEPOINT autopoint_2",
          "SELECT pg_advisory_unlock(4354430579665871434) -- 'key'",
          "ROLLBACK TO SAVEPOINT autopoint_1",
          "SELECT 1 AS \"one\" FROM \"pg_locks\" WHERE (\"locktype\" = 'advisory') LIMIT 1",
          "COMMIT"
        ]
      end
    end
  end
end
