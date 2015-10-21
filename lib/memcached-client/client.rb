require 'ione'
require 'monitor'

module MemcachedClient
  class Client
    DEFAULT_OPTIONS = {
      port: 11211,
      num_connections: 5,
      logger: NullLogger.new
    }

    # Connects asynchronously to a memcached server
    #
    # @param host [String] host-name or IP address of memcached server
    # @param opts [Hash] extra options that affect the behavior of the [Client].
    #
    # @return [Future] whose value is the fully materialized [Client].
    def self.connect_async(host, opts = {})
      new(host, opts).send(:connect)
    end

    # Connects synchronously to a memcached server
    #
    # @param host [String] host-name or IP address of memcached server
    # @param opts [Hash] extra options that affect the behavior of the [Client].
    #
    # @return [Future] whose value is the fully materialized [Client].
    def self.connect(host, opts = {})
      connect_async(host, opts).value
    end

    private

    def initialize(host, opts = {})
      @options = DEFAULT_OPTIONS.merge(opts)
      @logger = @options[:logger]

      # Yell about extra options that were specified that don't exist.
      validate_options

      # Set up io-reactor
      @reactor = Ione::Io::IoReactor.new

      # Initialize array of connections (which are really wrapped in handlers)
      @protocol_handlers = []
      @next_handler_index = 0

      # Hold onto the connections also, to make it easy to close later.
      @connections = []
      ObjectSpace.define_finalizer(self, self.class.finalize(@logger, @reactor, @connections))
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
      # that did succeed... # although chances are, if one connection fails, they all
      # will, so maybe this isn't a big deal.

      connections_ready_promise = Ione::Promise.new

      (1..@options[:num_connections]).each do
        connection_future = f.flat_map { @reactor.connect(@options[:host], @options[:port]) }
        connection_future.on_failure do |e|
          connections_ready_promise.fail(e)
        end
        handler_future = connection_future.map { |connection|
          @connections << connection
          MemcachedProtocolHandler.new(connection, @logger)
        }
        handler_future.on_value { |protocol_handler|
          @protocol_handlers << protocol_handler
          if @protocol_handlers.length == @options[:num_connections] && !connections_ready_promise.future.completed?
            connections_ready_promise.fulfill(self)
          end
        }
      end

      connections_ready_promise.future
    end

    def validate_options
      extras = @options.keys - DEFAULT_OPTIONS.keys
      unless extras.empty?
        raise "Encountered the following unknown options: #{extras.join(',')}"
      end

      # TODO: Verify host, port, logger exists and has puts method.
      if @options[:num_connections] <= 0 || !@options[:num_connections].is_a?(Integer)
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
      # may back-log on an active connection, but it will run when it can.
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

    public

    def get_async(key_or_list, handler_index = nil)
      handler = handler_index ? @protocol_handlers[handler_index] : get_handler
      handler.get(key_or_list)
    end

    def get(key_or_list)
      get_async(key_or_list).value
    end

    def set_async(key, value, flags = 0, expiration = 0, handler_index = nil)
      handler = handler_index ? @protocol_handlers[handler_index] : get_handler
      handler.set(key, value, flags, expiration)
    end

    def set(key, value, flags = 0, expiration = 0)
      set_async(key, value, flags, expiration).value
    end
  end

  # This is almost completely copied from the ione redis_client example.
  class LineProtocolHandler
    def initialize(connection, logger)
      @logger = logger
      @connection = connection
      @connection.on_data(&method(:process_data))
      @buffer = Ione::ByteBuffer.new
    end

    def on_line(&listener)
      @line_listener = listener
    end

    def write(command_string)
      @connection.write(command_string)
    end

    def process_data(new_data)
      lines = []
      @buffer << new_data
      while (newline_index = @buffer.index("\r\n"))
        line = @buffer.read(newline_index + 2)
        line.chomp!
        lines << line
      end
      lines.each do |line|
        @line_listener.call(line) if @line_listener
      end
    end
  end

  class MemcachedProtocolHandler
    include MonitorMixin

    attr_reader :state

    def initialize(connection, logger)
      mon_initialize

      @logger = logger
      @line_protocol = LineProtocolHandler.new(connection, @logger)
      @line_protocol.on_line(&method(:handle_line))

      @state = :good

      # Maintain a queue of pending commands on this connection. The first element
      # is the active one.
      @handler_queue = []
    end

    def get(key_or_list)
      # Unify to an array of keys
      keys = key_or_list.is_a?(Array) ? key_or_list : [key_or_list]

      @logger.puts "#{self.object_id}: got to get of #{keys.inspect}"

      # Set up the lower-level handler for the 'get' command and the promise that
      # we'll ultimately fulfill. It's important to do this *before* writing the
      # get command to the connection since the io-reactor thread may send the command,
      # get a response, and invoke our callbacks before this thread exits from this
      # method.

      promise = register_command(GetCommandHandler.new(method(:handle_response)))

      # Write the command out.
      @line_protocol.write("get #{keys.join(' ')}\r\n")

      # Return the future...
      promise.future
    end

    def set(key, value, flags = 0, expiration = 0)
      @logger.puts "#{self.object_id}: got to set of #{key}"

      promise = register_command(SetCommandHandler.new(method(:handle_response), key))

      # Write the command out.
      @line_protocol.write("set #{key} #{flags} #{expiration} #{value.length}\r\n#{value}\r\n")

      # Return the future...
      promise.future
    end

    def register_command(cmd)
      promise = Ione::Promise.new
      self.synchronize do
        @handler_queue << {
          handler: cmd,
          promise: promise
        }
      end
      promise
    end

    def handle_response(result, error=false)
      first_handler = synchronize do
        @handler_queue.shift
      end
      if error
        first_handler[:promise].fail(StandardError.new(result))
      else
        first_handler[:promise].fulfill(result)
      end
    end

    def handle_line(line)
      first_handler = synchronize do
        @handler_queue.first
      end
      first_handler[:handler].handle_line(line)
    rescue
      # This means an internal error occurred while processing this command response.
      # This implies that the connection is no longer in a usable state, so we should
      # no longer process commands on it. Ideally, we'd want to remove this handler
      # from the array in the Client instance, but we don't have access to that. So we
      # mark ourselves bad and the Client can skip us for processing future commands.

      # NB: We don't strictly need to grab the lock because MRI guarantees atomic
      # assignment.
      @state = :bad
    end

    class CommandHandler
      def initialize(result_handler)
        @result_handler = result_handler
      end

      def complete(result)
        @result_handler.call(result)
      end

      def fail(message, do_raise = false)
        @result_handler.call(message, true)
        raise message if do_raise
      end
    end

    class GetCommandHandler < CommandHandler
      def initialize(result_handler)
        super(result_handler)
        @results = {}

        # When processing a get command response, we're in one of two states:
        # status_line: we're waiting for a status line.
        # data_line: we're waiting for one or more lines of data.
        @state = :status_line
      end

      def handle_line(line)
        case @state
        when :status_line
          if line == 'END'
            # We're done!
            complete(@results)
          elsif line =~ /^(CLIENT|SERVER)_ERROR (.*)/
            fail("get failed: #{$2}")
          elsif line =~ /^VALUE /
            @current_key, flags, len = line.split(' ')[1..3]
            @data_len = len.to_i
            @results[@current_key] = {
              flags: flags.to_i,
              value: ''
            }

            # Update the state; we expect some data next.
            @state = :data_line
          else
            fail("could not parse line: #{line}", true)
          end
        when :data_line
          # If this is not the first line of data, add on a \r\n to the data before
          # appending the new line. This is because the LineProtocolHandler strips off
          # trailing \r\n before sending the line to us.
          unless @results[@current_key][:value].empty?
            @results[@current_key][:value] << "\r\n"
          end
          @results[@current_key][:value] << line

          if @results[@current_key][:value].bytesize == @data_len
            # We're done with data; switch back to status_line mode.
            @state = :status_line
          end
        else
          fail("internal error: request handler in unknown state: #{state}", true)
        end
      end
    end

    class SetCommandHandler < CommandHandler
      def initialize(result_handler, key)
        super(result_handler)
        @key = key
      end

      def handle_line(line)
        case line
        when 'ERROR'
          fail("internal error: set call failed for key #{key}", true)
        when /^(CLIENT|SERVER)_ERROR (.*)/
          fail("set failed: #{$2}")
        when 'STORED'
          # success!
          complete(nil)
        else
          fail("could not parse line: #{line}", true)
        end
      end
    end
  end
end

