require "aws-sdk-sqs"
require "alephant/logger"

module Alephant
  module Publisher
    module Queue
      module SQSHelper
        class Queue
          WAIT_TIME = 5
          VISABILITY_TIMEOUT = 300

          include Alephant::Logger

          attr_reader :queue, :timeout, :wait_time, :archiver

          def initialize(
            queue,
            archiver  = nil,
            timeout   = VISABILITY_TIMEOUT,
            wait_time = WAIT_TIME
          )
            @queue     = queue
            @archiver  = archiver
            @timeout   = timeout
            @wait_time = wait_time
            log_queue_creation queue.url, archiver, timeout
          end

          def message
            receive.tap { |m| process(m) }
          end

          private

          def log_queue_creation(queue_url, archiver, timeout)
            logger.info(
              "event"    => "QueueConfigured",
              "queueUrl" => queue_url,
              "archiver" => archiver,
              "timeout"  => timeout,
              "method"   => "#{self.class}#initialize"
            )
          end

          def process(m)
            return unless m.size > 0

            logger.metric "MessagesReceived"
            logger.info(
              "event"     => "QueueMessageReceived",
              "messageId" => m.first.message_id,
              "method"    => "#{self.class}#process"
            )
            # @TODO: Look at archiver as should support message from collection.
            archive m.first
          end

          def archive(m)
            archiver.see(m) unless archiver.nil?
          rescue StandardError => e
            logger.metric "ArchiveFailed"
            logger.error(
              "event"     => "MessageArchiveFailed",
              "class"     => e.class,
              "message"   => e.message,
              "backtrace" => e.backtrace.join.to_s,
              "method"    => "#{self.class}#archive"
            )
          end

          def receive
            queue.receive_messages(
              :visibility_timeout     => timeout,
              :wait_time_seconds      => wait_time,
              :max_number_of_messages => 1
            )
          end
        end
      end
    end
  end
end
