# No-op logger class
module MemcachedClient
  class NullLogger
    def puts(*args)
      # no-op
    end
  end

  private_constant :NullLogger
end
