module MemcachedClient
  # This class is almost completely copied from the ione redis_client example.
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

      # This is a good handler/connection until told otherwise.
      @state = :good

      # Maintain a queue of pending commands on this connection. The first element
      # is the active one.
      @handler_queue = []
    end

    def get(key_or_list)
      raise 'one or more keys must be provided' if key_or_list.nil? ||
        (key_or_list.respond_to?(:empty?) && key_or_list.empty?)

      # Unify to an array of keys
      keys = key_or_list.is_a?(Array) ? key_or_list : [key_or_list]

      @logger.puts "#{self.object_id}: got to get of #{keys.inspect}"

      keys.each do |k|
        validate_key(k)
      end

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

    def set(key, value, options)
      validate_key(key)

      # TODO: Validate that the flags and expiration keys in the options hash are
      # valid.

      @logger.puts "#{self.object_id}: got to set of #{key}"

      promise = register_command(SetCommandHandler.new(method(:handle_response), key))

      # Write the command out.
      @line_protocol.write("set #{key} #{options[:flags]} #{options[:expiration].to_i}" +
                             " #{value.length}\r\n#{value}\r\n")

      # Return the future...
      promise.future
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

      first_handler[:handler].handle_line(line) unless first_handler.nil?
    rescue => e
      @logger.puts "Caught exception: #{e.message}"

      # This means an internal error occurred while processing this command response.
      # This implies that the connection is no longer in a usable state, so we should
      # no longer process commands on it. Ideally, we'd want to remove this handler
      # from the array in the Client instance, but we don't have access to that. So we
      # mark ourselves bad and the Client can skip us for processing future commands.

      @state = :bad

      # Since we consider this connection dead, fail all pending command promises.
      handler_queue = nil
      synchronize do
        handler_queue = @handler_queue
        @handler_queue = []
      end
      handler_queue.each do |h|
        h[:promise].fail(StandardError.new('Cascaded failure on connection; see an ' +
                                             'earlier future for the originating error'))
      end
    end

    private

    def register_command(cmd)
      promise = Ione::Promise.new
      self.synchronize do
        # We do this under a lock because the io-reactor may invoke our
        # response-callback to fulfill the active promise, which involves mutating
        # this array.
        # TODO: Consider replacing this with a proper Queue, which is mt-safe.
        @handler_queue << {
          handler: cmd,
          promise: promise
        }
      end
      promise
    end

    def validate_key(key)
      raise 'key must be a non-empty string' if !key.is_a?(String) || key.empty?
      raise 'key must not contain CR or LF characters' if key.index(/[\r\n]/)
    end
  end

  private_constant :MemcachedProtocolHandler
  private_constant :LineProtocolHandler
end

