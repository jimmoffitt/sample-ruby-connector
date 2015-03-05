require 'yaml'
require_relative '../src/thin_connector.rb'

##################
# Helpers
##################

def repl
  loop do
    print '> '
    cmd = gets
    handle_command cmd
  end
end

def handle_command(cmd)
  return if cmd =~ /^(\n|\r)/
  strip_cmd = (cmd.downcase.gsub /\W/, '').to_sym
  block = commands[strip_cmd]
  block.call if block
end

def commands
  {
      redis: Proc.new{ redis_processor },
      stdout: Proc.new{ print_stream_processor },
      mongo: Proc.new{ mongo_processor },
      configure: Proc.new{ configure },
      exit: Proc.new{ repl_exit },
      help: Proc.new{ puts help_msg }
  }
end

def handle_needs_configuration(cmd=nil)
  puts "#{cmd || 'This'} command required configuration. Please run 'configure'"
end

def help_msg
  <<-eos
    Welcome to the Ruby Thin Connector!
    This is a sample application that demonstrates best practices when
    consuming the Gnip set of streaming APIs

    Commands:

    configure # Run the interactive configuration
    redis # Run the redis
    stdout # Run the stdout processor
  eos
end

def repl_exit
  puts 'See you next time! :)'
  exit 0
end

def get_user_input(item_name)
  print "Enter #{item_name}: "
  input = gets.chomp
  if non_alpha_numeric_string(input) then '' else input end
end


def non_alpha_numeric_string(str)
  0 == (str =~ /^\W+$/) || str.empty?
end

def get_non_null_user_input(item_name)
  loop do
    input = get_user_input item_name
    if non_alpha_numeric_string(input)
      puts "#{item_name} cannot be empty!"
    else
      return input
    end
  end
end

def get_user_input_with_default(item_name, default)
  return get_user_input(item_name) unless default
  print "Enter #{item_name} (defaults to #{default}): "
  input = gets.chomp
  if non_alpha_numeric_string(input) then default else input end
end

def configure_application
  config = {}
  config[:gnip_username] = get_non_null_user_input 'gnip username'
  config[:gnip_password] = get_non_null_user_input 'gnip password'
  config[:gnip_url] = get_non_null_user_input 'gnip url'
  config[:log_level] = get_user_input_with_default 'debug level', 'debug'
  p config
  write_out_config application_configuration_path, environment_configuration(config)
end

def configure_redis
  configs_with_defaults = {
      host: '0.0.0.0',
      port: 6379
  }
  user_configs = configs_with_defaults.inject({}) do  |acc, key_val_arr|
    k, v = key_val_arr
    user_input_with_default = get_user_input_with_default "Redis #{k}", v
    acc[k] = user_input_with_default
    acc
  end
  write_out_config redis_configuration_path, environment_configuration(user_configs)
end

def configure_mongo
  config_with_defaults = {
      host: '0.0.0.0',
      database: 'thinConnectorProd',
      username: nil,
      password: nil
  }
  user_configs = config_with_defaults.inject({}) do  |acc, key_val_arr|
    k, v = key_val_arr
    user_input_with_default = get_user_input_with_default "Mongo #{k}", v
    acc[k] = user_input_with_default
    acc
  end
  write_out_config mongo_configuration_path, environment_configuration(user_configs)
end

def environment_configuration(user_configs)
  %w(test development production staging).map{ |env| env.to_sym }.inject({})  do |acc, val|
    acc[val] = user_configs
    acc
  end
end

def configure
  configure_application
  configure_redis
  configure_mongo
end

def application_configuration_path
  File.join File.dirname(__FILE__), '..', 'config', 'application.yml'
end


def redis_configuration_path
  File.join File.dirname(__FILE__), '..', 'config', 'redis.yml'
end

def mongo_configuration_path
  File.join File.dirname(__FILE__), '..', 'config', 'mongo.yml'
end

def write_out_config(path, config)
  File.open(path, 'w+') do |f|
    f.truncate 0
    f << config.to_yaml
  end
end

def mongo_processor
  if needs_configuration?
    handle_needs_configuration 'Mongo'
    return
  end
  stream = setup_stream
  mongo_processor = ThinConnector::Processor::MongoStreamProcessor.new stream
  run_processor mongo_processor
  puts <<-eos
    Mongo processor finished! Go check the
    Mongo server 'tweets' collection to see what we brought in!
  eos
end

def print_stream_processor
  if needs_configuration?
    handle_needs_configuration 'STDOUT'
    return
  end
  stream = setup_stream
  run_processor stream
  puts "\n\n\nWhew! That was a lot of JSON!"
end

def redis_processor
  if needs_configuration?
    handle_needs_configuration 'Redis'
    return
  end
  stream = setup_stream
  redis_processor = ThinConnector::Processor::RedisStreamProcessor.new stream
  redis = Redis.new ThinConnector::Environment.instance.redis_config
  begin
    redis.flushall
  rescue Redis::CannotConnectError
    puts "\nWhoops! Looks like we could not connect to Redis with that configuration"
    return
  end

  run_processor(redis_processor)

  puts <<-eos
Redis processor stopped. Head over to the redis-cli to see what we got!


hint, run:
> redis-cli
> KEYS *    # Command to show all keys
> llen
  eos
end

def run_processor(processor)
  processing_thread = Thread.new do
    processor.start
  end

  puts "#{processor.class.to_s} processor started. Press ENTER to stop\n\n"

  while true
    break if "\n"==gets
  end
  print 'Stopping'
  stop_thread = Thread.new do
    processor.stop
    processing_thread.join
  end

  while stop_thread.alive?
    print '.'
    sleep 1
  end
end


def setup_stream
  environment = ThinConnector::Environment.instance
  #Update with your Account Name and Product Label (likely to be 'prod'). Both are case sensitive.
  url = 'https://stream.gnip.com:443/accounts/<ACCOUNT_NAME>/publishers/twitter/streams/track/<LABEL>.json'
  headers = {
      authorization: [environment.gnip_username, environment.gnip_password],
      'Accept-Encoding' => 'gzip,deflate,sdch'
  }
  ThinConnector::Stream::GNIPStream.new url, headers
end

def needs_configuration?
  !(File.exist?(application_configuration_path) && File.exist?(redis_configuration_path ) && File.exist?(mongo_configuration_path))
end

##################
# End helpers
##################

puts help_msg
repl
