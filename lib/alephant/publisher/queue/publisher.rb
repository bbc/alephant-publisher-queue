module Alephant
  module Publisher
    module Queue
      class Publisher
        include Alephant::Logger

        VISIBILITY_TIMEOUT = 60
        RECEIVE_WAIT_TIME  = 15

        attr_reader :queue, :executor, :opts, :processor

        def initialize(opts, processor = nil)
          @opts = opts
          @processor = processor

          @queue = Alephant::Publisher::Queue::SQSHelper::Queue.new(
            aws_queue,
            archiver,
            opts.queue[:visibility_timeout] || VISIBILITY_TIMEOUT,
            opts.queue[:receive_wait_time]  || RECEIVE_WAIT_TIME
          )
        end

        def run!
          loop { processor.consume(@queue.message) }
        end

        private

        def archiver
          Alephant::Publisher::Queue::SQSHelper::Archiver.new(archive_storage, archiver_opts)
        end

        def archiver_opts
          options = {
            :async_store         => true,
            :log_archive_message => true,
            :log_validator       => opts.queue[:log_validator]
          }
          options.each do |key, _value|
            options[key] = opts.queue[key] == "true" if whitelist_key(opts.queue, key)
          end
        end

        def whitelist_key(options, key)
          options.key?(key) && key != :log_validator
        end

        def archive_storage
          Alephant::Storage.new(
            opts.writer[:s3_bucket_id],
            opts.writer[:s3_object_path]
          )
        end

        def get_region
          # @TODO: Where does region come from?
          opts.queue[:sqs_account_region] || Aws.config[:region] || 'eu-west-1'
        end

        def sqs_client
          @sqs_client ||= Aws::SQS::Client.new(sqs_queue_options)
        end

        def sqs_queue_options
          options = {}
          options[:endpoint] = ENV['AWS_SQS_ENDPOINT'] if ENV['AWS_SQS_ENDPOINT']
          options[:region]   = get_region

          logger.info(
            "event"   => "SQSQueueOptionsConfigured",
            "options" => options,
            "method"  => "#{self.class}#sqs_queue_options"
          )

          options
        end

        def aws_queue
          options = { queue_name: opts.queue[:sqs_queue_name] }
          options[:queue_owner_aws_account_id] = opts.queue[:aws_account_id] if opts.queue[:aws_account_id]

          queue_url = sqs_client.get_queue_url(options).queue_url

          resource = Aws::SQS::Resource.new(client: sqs_client)
          resource.queue(queue_url)
        end
      end
    end
  end
end
