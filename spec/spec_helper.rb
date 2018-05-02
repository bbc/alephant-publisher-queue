$LOAD_PATH << File.join(File.dirname(__FILE__), "..", "lib")

require "pry"
require "aws-sdk-sqs"
require "simplecov"

ENV['AWS_REGION'] = 'eu-west-1'

SimpleCov.start do
  add_filter "/spec/"
end

require "alephant/publisher/queue"

RSpec.configure do |config|
  config.order = "random"
end
