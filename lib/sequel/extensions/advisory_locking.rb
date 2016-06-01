# frozen_string_literal: true

require 'digest/md5'

require 'sequel/extensions/advisory_locking/version'

module Sequel
  module AdvisoryLocking
    class Error < StandardError; end

    HEX_STRING_SLICE_RANGE         = (0..15).freeze
    POSTGRES_SIGNED_BIGINT_BOUND   = 2**63
    POSTGRES_UNSIGNED_BIGINT_BOUND = 2**64

    POSTGRES_SIGNED_BIGINT_MAXIMUM = POSTGRES_SIGNED_BIGINT_BOUND - 1
    POSTGRES_SIGNED_BIGINT_MINIMUM = -POSTGRES_SIGNED_BIGINT_BOUND
    POSTGRES_SIGNED_BIGINT_RANGE = (POSTGRES_SIGNED_BIGINT_MINIMUM..POSTGRES_SIGNED_BIGINT_MAXIMUM).freeze

    LOCK_SQL     = "SELECT pg_advisory_lock(?) -- ?".freeze
    TRY_LOCK_SQL = "SELECT pg_try_advisory_lock(?) -- ?".freeze
    UNLOCK_SQL   = "SELECT pg_advisory_unlock(?) -- ?".freeze

    def advisory_lock(key, try: false, &block)
      int = advisory_lock_key(key)

      synchronize do
        begin
          # Add key to the end so that logs read easier.
          sql = try ? TRY_LOCK_SQL : LOCK_SQL
          locked = !!self[sql, int, key].get

          if locked && block
            if in_transaction?
              transaction(savepoint: true, rollback: :reraise, &block)
            else
              yield
            end
          else
            locked
          end
        ensure
          self[UNLOCK_SQL, int, key].get if locked
        end
      end
    end

    def advisory_lock_key(key)
      case key
      when Integer
        advisory_lock_key_range_check(key)
      when String, Symbol
        # For an arbitrary string, pseudorandomly return an integer in
        # the PG bigint range.
        hex = Digest::MD5.hexdigest(key.to_s)[HEX_STRING_SLICE_RANGE].hex

        # Mimic PG's bigint rollover behavior.
        hex -= POSTGRES_UNSIGNED_BIGINT_BOUND if hex >= POSTGRES_SIGNED_BIGINT_BOUND

        # The keys we derive from strings shouldn't ever fall outside the
        # bigint range, but assert that just to be safe.
        advisory_lock_key_range_check(hex)
      else
        raise Error, "passed an invalid key type (#{key.class})"
      end
    end

    private

    def advisory_lock_key_range_check(integer)
      if POSTGRES_SIGNED_BIGINT_RANGE.cover?(integer)
        integer
      else
        raise Error, "given advisory lock integer (#{integer}) falls outside Postgres' bigint range"
      end
    end
  end

  Database.register_extension(:advisory_locking) { |db| db.extend(Sequel::AdvisoryLocking) }
end
