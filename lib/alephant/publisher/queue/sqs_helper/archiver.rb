require 'alephant/logger'
require 'date'

module Alephant
  module Publisher
    module Queue
      module SQSHelper
        class Archiver
          include Logger

          attr_reader :cache, :async

          def initialize(cache, async = true)
            @async = async
            @cache = cache
          end

          def see(message)
            return if message.nil?
            message.tap { |m| async ? async_store(m) : store(m) }
          end

          private

          def async_store(m)
            Thread.new { store(m) }
            logger.metric(:name => "PublisherQueueSQSHelperAsynchronouslyArchivedData", :unit => "Count", :value => 1)
          end

          def store(m)
            logger.metric(:name => "PublisherQueueSQSHelperSynchronouslyArchivedData", :unit => "Count", :value => 1)
            logger.info "Publisher::Queue::SQSHelper::Archiver#store: '#{m.body}' at 'archive/#{date_key}/#{m.id}'"
            cache.put("archive/#{date_key}/#{m.id}", m.body, meta_for(m))
          end

          def date_key
            DateTime.now.strftime('%d-%m-%Y_%H')
          end

          def meta_for(m)
            {
              :id                => m.id,
              :md5               => m.md5,
              :logged_at         => DateTime.now.to_s,
              :queue             => m.queue.url,
            }
          end
        end
      end
    end
  end
end

