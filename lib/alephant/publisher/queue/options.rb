require 'aws-sdk'
require 'alephant/logger'

module Alephant
  module Publisher
    module Queue
      class InvalidKeySpecifiedError < StandardError; end

      class Options
        include Logger

        attr_reader :queue, :writer

        QUEUE_OPTS = [
          :receive_wait_time,
          :sqs_queue_name,
          :visibility_timeout,
          :aws_account_id,
          :log_archive_message,
          :async_store
        ]

        WRITER_OPTS = [
          :lookup_table_name,
          :msg_vary_id_path,
          :renderer_id,
          :s3_bucket_id,
          :s3_object_path,
          :sequence_id_path,
          :sequencer_table_name,
          :view_path
        ]

        def initialize
          @queue  = {}
          @writer = {}
        end

        def add_queue(opts)
          execute @queue, QUEUE_OPTS, opts
        end

        def add_writer(opts)
          execute @writer, WRITER_OPTS, opts
        end

        private

        def execute(instance, type, opts)
          begin
            validate type, opts
            instance.merge! opts
          rescue Exception => e
            logger.metric "QueueOptionsInvalidKeySpecified"
            logger.error "Publisher::Queue::Options#validate: '#{e.message}'"
            puts e.message
          end
        end

        def validate(type, opts)
          opts.each do |key, value|
            raise InvalidKeySpecifiedError, "The key '#{key}' is invalid" unless type.include? key.to_sym
          end
        end
      end
    end
  end
end
