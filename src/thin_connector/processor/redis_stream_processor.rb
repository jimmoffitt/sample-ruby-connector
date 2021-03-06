# This class implements functionality that handles a stream input and
# should be implemented to suit your own needs. In this sample app,
# it merely places the stream contents into a redis queue for processing
# by other actors

require 'redis'
require 'json'

module ThinConnector
  module Processor

    class RedisStreamProcessor
      include ThinConnector::Processor::StreamDelegate

      REDIS_NAMESPACE = "stream_processor:raw"
      attr_accessor :stream

      def initialize(the_stream)
        @logger = ThinConnector::Logger.new
        @stream = the_stream
        @logger.debug "attaching to redis with configs #{ThinConnector::Environment.instance.redis_config} and queue #{redis_queue}"
      end

      def start
        stream.start do |object|
          begin
            put_in_redis object.to_json
          rescue
            @logger.error "Error putting into redis: \n\n#{object}"
          end
        end
      end

      def stop
        stream.stop
      end

      private

      def put_in_redis(obj)
        redis_client.lpush redis_queue, obj
      end

      def redis_queue; Environment.instance.redis_namespace + ":#{REDIS_NAMESPACE}"; end

      def redis_client
        @_mongo_client ||= Redis.new ThinConnector::Environment.instance.redis_config
        @_mongo_client
      end

    end
  end
end
