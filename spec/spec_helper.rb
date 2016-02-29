# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'minitest/autorun'
require 'minitest/hooks'
require 'minitest/pride'

require 'sequel'

DB = Sequel.connect("postgres:///sequel-advisory-locking-test")

DB.extension :advisory_locking
