# frozen_string_literal: true

require 'digest/md5'

require 'sequel/extensions/advisory_locking/version'

module Sequel
  module AdvisoryLocking
    class Error < StandardError; end

    HEX_STRING_SLICE_RANGE         = 0..15
    POSTGRES_SIGNED_BIGINT_BOUND   = 2**63
    POSTGRES_UNSIGNED_BIGINT_RANGE = 2**64

    LOCK_SQL     = "SELECT pg_advisory_lock(?) -- ?".freeze
    TRY_LOCK_SQL = "SELECT pg_try_advisory_lock(?) -- ?".freeze
    UNLOCK_SQL   = "SELECT pg_advisory_unlock(?) -- ?".freeze

    def advisory_lock(key, try: false, &block)
      int = case key
            when Integer then key
            when String, Symbol
              # For an arbitrary string, pseudorandomly return an integer in
              # the PG bigint range.
              hex = Digest::MD5.hexdigest(key.to_s)[HEX_STRING_SLICE_RANGE].hex
              # Mimic PG's bigint rollover behavior.
              hex -= POSTGRES_UNSIGNED_BIGINT_RANGE if hex >= POSTGRES_SIGNED_BIGINT_BOUND
              hex
            else
              raise Error, "passed an invalid key type (#{key.class})"
            end

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
  end

  Database.register_extension(:advisory_locking) { |db| db.extend(Sequel::AdvisoryLocking) }
end
