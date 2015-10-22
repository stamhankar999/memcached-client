module MemcachedClient
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
        case line
        when 'END'
          # We're done!
          complete(@results)
        when 'ERROR'
          fail('get failed: unknown error')
        when /^(CLIENT|SERVER)_ERROR (.*)/
          fail("get failed: #{$2}")
        when /^VALUE /
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
        fail("set failed for key #{@key}: unknown error")
      when /^(CLIENT|SERVER)_ERROR (.*)/
        fail("set failed for key #{@key}: #{$2}")
      when 'STORED'
        # success!
        complete(nil)
      else
        fail("could not parse line: #{line}", true)
      end
    end
  end

  private_constant :CommandHandler
  private_constant :GetCommandHandler
  private_constant :SetCommandHandler
end
