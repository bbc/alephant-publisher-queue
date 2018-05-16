require "aws-sdk-sqs"
require "yaml"
require "alephant/logger"

config_file = "config/aws.yaml"

if File.exist? config_file
  config = YAML.load(File.read(config_file))
  Aws.config.update(config)
end
