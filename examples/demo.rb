$LOAD_PATH.unshift('lib', '../lib')
require 'memcached_client'

# Run a bunch of sets, forced on one connection, and a get of the last key-to-be-set
# on a separate connection, to demonstrate that the connection pool is working properly.
def parallel_test(host, key_base)
  c = MemcachedClient::Client.connect(host, logger: $stdout)

  # Set some keys on connection 0. We assume these keys do not exist apriori.
  # If the system is really fast or unfair, it might actually do all the sets before
  # the get on the other connection. Bump val (the payload) size to give the set's
  # more of a disadvantage. Disabling logging may help as well.

  future_set = nil
  num_keys = 5
  val = 'x' * 30000
  (1..num_keys).each do |ctr|
    future_set = c.set_async("#{key_base}_#{ctr}", val, {}, 0)
  end

  # Get the num_key'th key on connection 1. Because it ran in a separate connection, it
  # should execute before the set that occurs late in the first connection.

  result = c.get_async("#{key_base}_#{num_keys}", 1).value
  puts "We are still in the midst of setting a bunch of keys, so the 'get' for " +
         ' the last key should be empty'
  p result

  # Now block on the num_key'th key being set.
  future_set.value

  # Now get that key again. It should be there this time!
  puts 'We we waited for the last set to complete, so getting the key should now succeed'
  p c.get_async("#{key_base}_#{num_keys}", 1).value
end

# Connect to a memcached server and then unset variables and force the GC to run and
# do cleanup. Connections should be closed.
def connection_closure(host)
  c = MemcachedClient::Client.connect(host, logger: $stdout)

  puts 'Use "netstat -an | grep 11211" to verify that there are 5 connections opened ' +
         'to the memcached server and hit enter to continue.'
  gets

  c = nil
  GC.start
  puts "The Client object has been nil'ed and the GC invoked. The connections should " +
         'be closed now, even though our Ruby process is still running. Use netstat to ' +
         'verify and then hit enter to continue.'
  gets
end

# Connect asynchronously, then do something random, then wait for the connection to
# complete and use it.
def async_connect(host)
  client_future = MemcachedClient::Client.connect_async(host, logger: $stdout)
  puts 'being random'
  c = client_future.value
  c.set('async_key', 'val')
  p c.get('async_key')
  puts "Note that the object-id's printed in the debug output indicate we're using " +
         'different connections for different requests.'
end

# Some examples of setting and getting keys
def set_and_get(host)
  c = MemcachedClient::Client.connect(host, logger: $stdout)

  # The parallel_test function shows how we do async get/set, so we'll just do
  # synchronous ops here.

  # Set a key with a flag.
  puts '-- flagged --'
  c.set('flagged', 'flag val', flags: 42)
  p c.get('flagged')

  # Set a key with an expiration time of 3 seconds in the future.
  puts '-- expiring --'
  c.set('expiring', 'expire val', expiration: 3)
  p c.get('expiring')
  sleep 5
  # The key has expired, so getting it again should show that it no longer exists.
  p c.get('expiring')

  # Try again with an absolute expiration time 3 seconds in the future.
  puts '-- abs expiring --'
  c.set('expiring', 'expire val', expiration: Time::now + 3)
  p c.get('expiring')
  sleep 5
  # The key has expired, so getting it again should show that it no longer exists.
  p c.get('expiring')

  # Try a multi-line value
  puts '-- multiline --'
  expected_value = "abc\n123\r\n456\r\n"
  c.set('multiline', expected_value)
  result = c.get('multiline')
  p result
  comp = result['multiline'][:value] == expected_value
  puts "set and get values match: #{comp}"

  # Try getting multiple keys in one go.
  puts '-- multi key --'
  c.set('some_key', 'some_value')
  p c.get(['flagged', 'nonexist', 'some_key'])
  puts 'Note that there is no reference to the nonexist key in the result since it ' +
         'does not exist.'
rescue => e
  puts "excp: #{e.message}"
end

# The intention is for the user to uncomment one of these lines and try it out.
# Rinse and repeat until you're done.

#parallel_test('127.0.0.1', 't1')
#connection_closure('127.0.0.1')
#async_connect('127.0.0.1')
set_and_get('127.0.0.1')
