require 'ione'
require 'monitor'
require 'socket'
require_relative 'memcached-client/null_logger'
require_relative 'memcached-client/command_handlers'
require_relative 'memcached-client/protocol_handlers'

module MemcachedClient
  # Responsible for connecting to a Memcached server and getting/setting keys.
  # This object is not itself mt-safe though it does internally manage interactions
  # with an io-reactor in a thread-safe manner.
  #
  # In other words, if for some reason you want to get and set keys from multiple
  # threads that *you* control, you should create a separate {Client} for each thread.
  class Client
    DEFAULT_OPTIONS = {
      port: 11211,
      num_connections: 5,
      logger: NullLogger.new
    }
    private_constant :DEFAULT_OPTIONS

    # Default values for the optional args to the set and set_async methods.
    SET_COMMAND_DEFAULT_OPTIONS = {
      flags: 0,
      expiration: 0
    }
    private_constant :SET_COMMAND_DEFAULT_OPTIONS

    # Connects asynchronously to a memcached server
    #
    # @param host [String] host-name or IP address of memcached server
    # @param opts [Hash] extra options that affect the behavior of the {Client}.
    # @option opts [Integer] :port The port to connect to. Defaults to 11211.
    # @option opts [Integer] :num_connections The number of connections to open.
    #   Defaults to 5.
    # @option opts [#puts] :logger A logger object where debug log messages are emitted.
    #   The object must support the +puts+ message. +$stdout+ is a good example.
    #   Defaults to a null logger that throws away log messages.
    # @return [Future] whose value is the fully materialized {Client}.
    def self.connect_async(host, opts = {})
      new(host, opts).send(:connect)
    end

    # Connects synchronously to a memcached server
    #
    # @param host [String] host-name or IP address of memcached server
    # @param opts [Hash] extra options that affect the behavior of the {Client}.
    # @option opts [Integer] :port The port to connect to. Defaults to 11211.
    # @option opts [Integer] :num_connections The number of connections to open.
    #   Defaults to 5.
    # @option opts [#puts] :logger A logger object where debug log messages are emitted.
    #   The object must support the +puts+ message. +$stdout+ is a good example.
    #   Defaults to a null logger that throws away log messages.
    #
    # @return [Client] a client that's ready to execute commands.
    def self.connect(host, opts = {})
      connect_async(host, opts).value
    end

    # Issue a +get+ command to the memcached server, asynchronously.
    #
    # @param key_or_list [String, Array<String>] the key or list of keys to retrieve.
    # @return [Future] whose value is a hash keyed on the successfully retrieved keys.
    #   The value is a hash with keys :flags and :value.
    def get_async(key_or_list, handler_index = nil)
      # NOTE: the handler_index is a "hidden" optional parameter that locks in the
      # request handling to a particular connection. It's only used for demonstration
      # or testing.
      handler = handler_index ? @protocol_handlers[handler_index] : get_handler
      handler.get(key_or_list)
    end

    # Issue a +get+ command to the memcached server, synchronously.
    #
    # @param key_or_list [String, Array<String>] the key or list of keys to retrieve.
    # @return [Hash<String, Hash>] keyed on the successfully retrieved keys.
    #   The value is a hash with keys :flags and :value.
    def get(key_or_list)
      get_async(key_or_list).value
    end

    # Issue a +set+ command to the memcached server, asynchronously.
    #
    # @param key [String] the key to set.
    # @param value [String] the value to set.
    # @param opts [Hash] extra options that configure the set command.
    # @option opts [Integer] :flags the flags argument to the memcached +set+ command.
    #   Defaults to 0
    # @option opts [Integer,Time] :expiration interval in seconds after which the key
    #   should expire, or the time at which the key should expire. If this option value
    #   is greater than 30 days (in seconds), it is interpreted as an absolute timestamp.
    #   Defaults to 0, indicating that the key never expires.
    # @return [Future] whose value will be nil. The main purpose of getting the future's
    #   value is to block until the set completes.
    def set_async(key, value, opts = {}, handler_index = nil)
      # NOTE: the handler_index is a "hidden" optional parameter that locks in the
      # request handling to a particular connection. It's only used for demonstration
      # or testing.
      handler = handler_index ? @protocol_handlers[handler_index] : get_handler

      options = SET_COMMAND_DEFAULT_OPTIONS.merge(opts)

      handler.set(key, value, options)
    end

    # Issue a +set+ command to the memcached server, synchronously.
    #
    # @param key [String] the key to set.
    # @param value [String] the value to set.
    # @param opts [Hash] extra options that configure the set command.
    # @option opts [Integer] :flags the flags argument to the memcached +set+ command.
    #   Defaults to 0
    # @option opts [Integer,Time] :expiration interval in seconds after which the key
    #   should expire, or the time at which the key should expire. If this option value
    #   is greater than 30 days (in seconds), it is interpreted as an absolute timestamp.
    #   Defaults to 0, indicating that the key never expires.
    def set(key, value, opts = {})
      options = SET_COMMAND_DEFAULT_OPTIONS.merge(opts)
      set_async(key, value, options).value
    end

    private

    def initialize(host, opts = {})
      @options = DEFAULT_OPTIONS.merge(opts)
      @logger = @options[:logger]
      @host = host

      # Yell about extra options that were specified that don't exist.
      validate_arguments

      # Set up io-reactor
      @reactor = Ione::Io::IoReactor.new

      # Initialize array of connections (which are really wrapped in handlers)
      @protocol_handlers = []
      @next_handler_index = 0

      # Hold onto the connections also, to make it easy to close later.
      @connections = []

      # Define a finalizer to close connections upon destruction.
      ObjectSpace.define_finalizer(self,
                                   self.class.finalize(@logger, @reactor, @connections))
    end

    def connect
      # Set up connections.
      f = @reactor.start

      # This is a little tricky. We want to return a future that when fulfilled,
      # it indicates that all of our connections / protocol-handlers are ready to go.
      # Create a promise that will be fulfilled by the last "protocol-handler" future.
      #
      # If any connection failure occurs, we break the promise.
      #
      # TODO: If we do this for real, we should be smarter and work with the connections
      # that did succeed... although chances are, if one connection fails, they all
      # will, so maybe this isn't a big deal.

      connections_ready_promise = Ione::Promise.new

      (1..@options[:num_connections]).each do
        connection_future = f.flat_map { @reactor.connect(@host, @options[:port]) }
        connection_future.on_failure do |e|
          connections_ready_promise.fail(e)
        end
        handler_future = connection_future.map do |connection|
          @connections << connection
          MemcachedProtocolHandler.new(connection, @logger)
        end
        handler_future.on_value do |protocol_handler|
          @protocol_handlers << protocol_handler
          if @protocol_handlers.length == @options[:num_connections] &&
            !connections_ready_promise.future.completed?

            connections_ready_promise.fulfill(self)
          end
        end
      end

      connections_ready_promise.future
    end

    def validate_arguments
      raise 'host must be non-empty' if @host.nil? || @host.empty?

      extras = @options.keys - DEFAULT_OPTIONS.keys
      unless extras.empty?
        raise "Encountered the following unknown options: #{extras.join(',')}"
      end

      if !@options[:port].is_a?(Integer) || @options[:port] <= 0
        raise 'port must be a positive integer'
      end

      if @options[:logger].nil? || !@options[:logger].respond_to?(:puts)
        raise 'logger must have a puts method'
      end

      if !@options[:num_connections].is_a?(Integer) || @options[:num_connections] <= 0
        raise 'num_connections must be a positive integer'
      end
    end

    def self.finalize(logger, reactor, connections)
      proc {
        logger.puts "shutting down reactor and #{connections.length} connections"
        connections.each(&:close)
        reactor.shutdown
      }
    end

    def get_handler
      # Round-robin through connections. The idea is that we can run several commands
      # in parallel on different connections. If we run out of connections, a command
      # may queue on an active connection, but it will run when it can. But since it'll
      # queue on the connection that has the oldest-running request, it's likely to
      # run soon.
      #
      # TODO: A more sophisticated policy could be implemented where we find the
      # first handler with the least number of pending commands.

      while @protocol_handlers[@next_handler_index].state == :bad
        @protocol_handlers.delete_at(@next_handler_index)

        raise 'all connections are dead' if @protocol_handlers.empty?

        # We might have deleted the last handler. Update next_handler_index accordingly.
        @next_handler_index %= @protocol_handlers.length
      end

      handler = @protocol_handlers[@next_handler_index]
      @next_handler_index = (@next_handler_index + 1) % @protocol_handlers.length
      handler
    end
  end
end
