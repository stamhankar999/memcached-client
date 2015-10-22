require 'spec_helper'

describe MemcachedClient::Client do
  let(:handlers) { [double('Handler1'), double('Handler2')] }
  let(:client) { MemcachedClient::Client.new('fakehost') }

  before do
    client.instance_variable_set(:@protocol_handlers, handlers.dup)
  end

  describe :get_async do
    it 'should choose the first handler' do
      expect(handlers[0]).to receive(:state).and_return(:good)
      expect(handlers[0]).to receive(:get).with('mykey')

      client.get_async('mykey')
    end

    it 'should support overriding the handler' do
      expect(handlers[1]).to receive(:get).with('mykey')

      client.get_async('mykey', 1)
    end
  end

  describe :set_async do
    it 'should choose the first handler' do
      expect(handlers[0]).to receive(:state).and_return(:good)
      expect(handlers[0]).to receive(:set).with(
                               'mykey',
                               'my value',
                               {flags: 0, expiration: 0}
                             )

      client.set_async('mykey', 'my value')
    end

    it 'should support overriding the handler' do
      expect(handlers[1]).to receive(:set).with(
                               'mykey',
                               'my value',
                               {flags: 0, expiration: 0}
                             )

      client.set_async('mykey', 'my value', {}, 1)
    end

    it 'should handle flags override' do
      expect(handlers[0]).to receive(:state).and_return(:good)
      expect(handlers[0]).to receive(:set).with(
                               'mykey',
                               'my value',
                               {flags: 42, expiration: 0}
                             )

      client.set_async('mykey', 'my value', flags: 42)
    end

    it 'should handle expiration override' do
      expect(handlers[0]).to receive(:state).and_return(:good)
      expect(handlers[0]).to receive(:set).with(
                               'mykey',
                               'my value',
                               {flags: 0, expiration: 42}
                             )

      client.set_async('mykey', 'my value', expiration: 42)
    end

    it 'should handle flags and expiration override' do
      expect(handlers[0]).to receive(:state).and_return(:good)
      expect(handlers[0]).to receive(:set).with(
                               'mykey',
                               'my value',
                               {flags: 5, expiration: 42}
                             )

      client.set_async('mykey', 'my value', flags: 5, expiration: 42)
    end
  end

  describe 'argument validation' do
    it 'should raise if host is empty' do
      expect {
        MemcachedClient::Client.new('')
      }.to raise_error(RuntimeError)
    end

    it 'should raise if host is nil' do
      expect {
        MemcachedClient::Client.new(nil)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if optionals contain unrecognized elements' do
      expect {
        MemcachedClient::Client.new('fakehost', dummy: 5)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if port is nil' do
      expect {
        MemcachedClient::Client.new('fakehost', port: nil)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if port is not positive' do
      expect {
        MemcachedClient::Client.new('fakehost', port: 0)
      }.to raise_error(RuntimeError)
      expect {
        MemcachedClient::Client.new('fakehost', port: -4)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if port is not an integer' do
      expect {
        MemcachedClient::Client.new('fakehost', port: 'abc')
      }.to raise_error(RuntimeError)
    end

    it 'should raise if logger is nil' do
      expect {
        MemcachedClient::Client.new('fakehost', logger: nil)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if logger does not handle puts' do
      expect {
        MemcachedClient::Client.new('fakehost', logger: 'foo')
      }.to raise_error(RuntimeError)
    end

    it 'should raise if num_connections is nil' do
      expect {
        MemcachedClient::Client.new('fakehost', num_connections: nil)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if num_connections is not positive' do
      expect {
        MemcachedClient::Client.new('fakehost', num_connections: 0)
      }.to raise_error(RuntimeError)
      expect {
        MemcachedClient::Client.new('fakehost', num_connections: -4)
      }.to raise_error(RuntimeError)
    end

    it 'should raise if num_connections is not an integer' do
      expect {
        MemcachedClient::Client.new('fakehost', num_connections: 'abc')
      }.to raise_error(RuntimeError)
    end
  end

  xcontext :connect do
    # This method does a lot of stuff with the reactor and futures and would take
    # a fair bit of effort to test here. I think if we had a stub io-reactor that
    # returned fake futures, we could do something smart like let connect run and then
    # call all of the callbacks registered during connect, and then verify the end
    # result (e.g. we have the right number of connections / protocol-handlers, and the
    # future returned by connect is completed with value or error).

    it 'should create the right number of connections' do

    end

    it 'should fail the promise when connections fail' do

    end

    it 'should fulfill the promise when connections succeed' do

    end
  end

  context :get_handler do
    it 'should cycle through handler list round robin style' do
      expect(handlers[0]).to receive(:state).and_return(:good).twice
      expect(handlers[1]).to receive(:state).and_return(:good).twice

      expect(client.send(:get_handler)).to be(handlers[0])
      expect(client.send(:get_handler)).to be(handlers[1])
      expect(client.send(:get_handler)).to be(handlers[0])
      expect(client.send(:get_handler)).to be(handlers[1])
    end

    it 'should skip bad handlers' do
      expect(handlers[0]).to receive(:state).and_return(:bad)
      expect(handlers[1]).to receive(:state).and_return(:good).exactly(3).times

      expect(client.send(:get_handler)).to be(handlers[1])
      expect(client.send(:get_handler)).to be(handlers[1])
      expect(client.send(:get_handler)).to be(handlers[1])
    end
  end
end
