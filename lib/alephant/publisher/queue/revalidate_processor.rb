require 'faraday'
require "aws-sdk-sqs"
require 'crimp'
require 'alephant/publisher/queue/processor'
require 'alephant/publisher/queue/revalidate_writer'
require 'json'
require 'alephant/logger'
require 'ostruct'

module Alephant
  module Publisher
    module Queue
      class RevalidateProcessor < Processor
        include Alephant::Logger

        attr_reader :opts, :url_generator, :http_response_processor

        def initialize(opts = nil, url_generator, http_response_processor)
          @opts                    = opts
          @url_generator           = url_generator
          @http_response_processor = http_response_processor
        end

        def consume(message_collection)
          return unless message_collection && message_collection.size > 0

          message = message_collection.first

          msg_body = message_content(message)

          # @TODO: This is not a http response but a data struct. We should look at renaming this.
          http_response = {
            renderer_id:   msg_body.fetch(:id),
            http_options:  msg_body,
            http_response: get(message),
            ttl:           http_response_processor.ttl(msg_body).to_s # @TODO: What happens if this is nil? Storage requires this to be a string.
          }

          http_message = build_http_message(message, ::JSON.generate(http_response))

          write(http_message)

          message.delete
          logger.info(event: 'SQSMessageDeleted', message_content: message_content(message), method: "#{self.class}#consume")

          cache.delete(inflight_message_key(message))
          logger.info(event: 'InFlightMessageDeleted', key: inflight_message_key(message), method: "#{self.class}#consume")
        end

        private

        def write(message)
          RevalidateWriter.new(writer_config, message).run!
        end

        # NOTE: If you change this, you'll need to change this in
        #       `alephant-broker` also.
        def inflight_message_key(message)
          opts = ::JSON.parse(message.body)
          version_cache_key(
            "inflight-#{opts['id']}/#{build_inflight_opts_hash(opts)}"
          )
        end

        def build_inflight_opts_hash(opts)
          opts_hash = Hash[opts['options'].map { |k, v| [k.to_sym, v] }]
          Crimp.signature(opts_hash)
        end

        def version_cache_key(key)
          cache_version = opts.cache[:elasticache_cache_version]
          [key, cache_version].compact.join('_')
        end

        def cache
          @cache ||= proc do
            endpoint = opts.cache.fetch(:elasticache_config_endpoint)
            Dalli::ElastiCache.new(endpoint).client
          end.call
        end

        def build_http_message(message, http_response)
          OpenStruct.new({
            body: http_response
          })
        end

        def get(message)
          msg_content = message_content(message)
          url         = url_generator.generate(msg_content)

          logger.info(
            event:  'Sending HTTP GET request',
            url:    url,
            method: "#{self.class}#get"
          )

          res = Faraday.get(url)

          logger.info(
            event:  'HTTP request complete',
            url:    url,
            status: res.status,
            body:   res.body,
            method: "#{self.class}#get"
          )

          http_response_processor.process(msg_content, res.status, res.body)
        end

        def message_content(message)
          ::JSON.parse(message.body, symbolize_names: true)
        end
      end
    end
  end
end
