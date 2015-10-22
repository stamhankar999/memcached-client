require 'spec_helper'

# The classes we're testing are actually private/internal.
# We hack up some public sub-classes to make testing easier.
module MemcachedClient
  class VisibleMemcachedProtocolHandler < MemcachedProtocolHandler
    def initialize(connection, logger)
      super(connection, logger)
    end

    def state
      @state
    end

    def handler_queue
      @handler_queue
    end
  end
end

xdescribe 'MemcachedClient::LineProtocolHandler#process_data' do
  # This class was copied almost verbatim from the ione redis_client example.
  # In the interest of time, we're not going to test it. However, if we did,
  # the process_data method is the thing to focus on.

  it 'should not action on partial data' do
    # verify that it accrues the data in its buffer.
  end

  it 'should invoke the line-listener when there is a completed line' do
    # verify that it accrues the data in its buffer.
  end
end

describe 'MemcachedClient::MemcachedProtocolHandler' do
  let(:connection) { double('connection') }
  let(:logger) { double('logger') }
  let(:handler) { MemcachedClient::VisibleMemcachedProtocolHandler.new(connection, logger)}

  before do
    allow(logger).to receive(:puts)
    expect(connection).to receive(:on_data)
  end

  it 'should start in a good state' do
    expect(handler.state).to eq(:good)
  end

  context :get do
    it 'should register a new GetCommandHandler in the handler-queue and emit the right command' do
      expect(connection).to receive(:write).with("get mykey\r\n")

      handler.get('mykey')
      expect(handler.handler_queue.first[:handler].class.to_s).
        to eq('MemcachedClient::GetCommandHandler')
    end

    it 'should emit the right command for multiple keys' do
      expect(connection).to receive(:write).with("get mykey key2\r\n")

      handler.get(['mykey', 'key2'])
    end

    it 'should raise an error if no keys are provided' do
      expect {
        handler.get('')
      }.to raise_error(RuntimeError)

      expect {
        handler.get([])
      }.to raise_error(RuntimeError)
    end
  end

  context :set do
    it 'should register a new SetCommandHandler in the handler-queue and emit the right command' do
      expect(connection).to receive(:write).with("set mykey 8 7 5\r\n12345\r\n")

      handler.set('mykey', '12345', {flags: 8, expiration: 7})
      expect(handler.handler_queue.first[:handler].class.to_s).
        to eq('MemcachedClient::SetCommandHandler')
    end

    it 'should  emit the right command when expiration is a Time' do
      expire_time = Time::now

      expect(connection).to receive(:write).
                              with("set mykey 8 #{expire_time.to_i} 5\r\n12345\r\n")

      handler.set('mykey', '12345', {flags: 8, expiration: expire_time})
    end

    it 'should raise an error if the key argument is nil' do
      expect {
        handler.set(nil, '12345', {flags: 0, expiration: 0})
      }.to raise_error(RuntimeError)
    end

    it 'should raise an error if the key argument is not a string' do
      expect {
        handler.set(3, '12345', {flags: 0, expiration: 0})
      }.to raise_error(RuntimeError)
    end

    it 'should raise an error if the key argument is an empty string' do
      expect {
        handler.set('', '12345', {flags: 0, expiration: 0})
      }.to raise_error(RuntimeError)
    end
  end

  it 'should mark the protocol-handler as bad if the command-handler raises' do
    allow(connection).to receive(:write)
    handler.set('mykey', '12345', {flags: 8, expiration: 7})

    allow(handler.handler_queue.first[:handler]).to receive(:handle_line).and_raise("yikes")

    handler.handle_line('foo')

    expect(handler.state).to eq(:bad)
  end

  it 'should fail pending promises if the command-handler raises' do
    allow(connection).to receive(:write)
    handler.set('mykey', '12345', {flags: 8, expiration: 7})

    # Add on a fake pending command-handler/promise.
    p = double('promise')
    handler.handler_queue << {handler: 'foo', promise: p}

    allow(handler.handler_queue.first[:handler]).to receive(:handle_line).and_raise("yikes")
    expect(p).to receive(:fail)

    handler.handle_line('foo')
  end
end
