$LOAD_PATH.unshift("lib")
require 'memcached_client'
def load_test
  c = MemcachedClient::Client.connect(logger: $stdout, host: '10.200.1.109')

  # Set a lot of keys on connection 0.
  future_set = nil
  num_keys = 100
  (1..num_keys).each do |ctr|
    future_set = c.set_async("ruby_#{ctr}", 'val', 0, 0, 0)
  end

  # Get the 25th key on connection 1. Because it ran in a separate connection, it
  # should execute before the set that occurs late in the first connection.

  result = c.get_async("ruby_#{num_keys}", 1).value
  p result

  # Now block on the 25th key being set.
  future_set.value

  # Now get that key again.
  p c.get_async("ruby_#{num_keys}", 1).value
end

# client_future = MemcachedClient::Client.connect(logger: $stdout, host: '10.200.1.109')
# c = client_future.value
# future_result = c.get_async(['mykey', 'e', 'f', 'g'])
# p future_result
# result = future_result.value
# p result

# set_future = c.set_async('ruby1', '36', 9)
# res = set_future.value
# p res


load_test

sleep 10

# client_future = nil
# c = nil
# future_result1 = nil

GC.start

sleep 10

# client_future = nil
# c = nil
# future_result = nil
# result = nil
#
# GC.start
# sleep(10)
