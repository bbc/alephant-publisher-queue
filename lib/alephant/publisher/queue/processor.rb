require "alephant/publisher/queue/writer"

module Alephant
  module Publisher
    module Queue
      class Processor
        attr_reader :opts

        def initialize(opts = nil)
          @opts = opts
        end

        def consume(message_collection)
          return unless message_collection && message_collection.size > 0

          message = message_collection.first
          write(message)
          message.delete
        end

        private

        def writer_config
          opts ? opts.writer : {}
        end

        def write(msg)
          Writer.new(writer_config, msg).run!
        end
      end
    end
  end
end
