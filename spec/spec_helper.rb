# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'minitest/autorun'
require 'minitest/hooks'
require 'minitest/pride'

require 'sequel'

DB = Sequel.connect("postgres:///sequel-advisory-locking-test")

DB.extension :advisory_locking

# Simple way to spec what queries are being run.
logger = Object.new

def logger.info(sql)
  if q = sql[/\(\d\.[\d]{6}s\) (.+)/, 1]
    Thread.current[:sqls] ||= []
    Thread.current[:sqls] << q
  end
end

def logger.error(msg)
end

DB.loggers << logger
