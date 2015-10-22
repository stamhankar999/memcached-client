require 'spec_helper'

# The classes we're testing are actually private/internal.
# We hack up some public sub-classes to make testing easier.
module MemcachedClient
  class VisibleCommandHandler < CommandHandler
    def initialize(result_handler)
      super(result_handler)
    end
  end

  class VisibleGetCommandHandler < GetCommandHandler
    def initialize(result_handler)
      super(result_handler)
    end
  end

  class VisibleSetCommandHandler < SetCommandHandler
    def initialize(result_handler, key)
      super(result_handler, key)
    end
  end
end

describe 'MemcachedClient::CommandHandler' do
  let(:result_handler) { double('result_handler') }
  let(:command_handler) { MemcachedClient::VisibleCommandHandler.new(result_handler) }

  it 'should call the result handler upon success' do
    expect(result_handler).to receive(:call).with('success')

    command_handler.complete('success')
  end

  it 'should call the result handler upon failure' do
    expect(result_handler).to receive(:call).with('failure', true)

    command_handler.fail('failure')
  end

  it 'should call the result handler and raise on catastrophe' do
    expect(result_handler).to receive(:call).with('failure', true)

    expect {
      command_handler.fail('failure', true)
    }.to raise_error('failure')
  end
end

describe 'MemcachedClient::GetCommandHandler' do
  let(:result_handler) { double('result_handler') }
  let(:command_handler) { MemcachedClient::VisibleGetCommandHandler.new(result_handler) }
  let(:payload) {
    <<-EOF
VALUE mykey 5 5
12345
END
    EOF
  }
  let(:multiline_payload) {
    <<-EOF
VALUE mykey 5 7
123
45
END
    EOF
  }
  let(:empty_payload) {
    <<-EOF
VALUE mykey 5 0

END
    EOF
  }
  let(:multi_payload) {
    <<-EOF
VALUE mykey 5 5
12345
VALUE mykey2 7 5
98765
END
    EOF
  }
  it 'should handle a single value payload' do
    expected_result = {
      'mykey' => {
        value: '12345',
        flags: 5
      }
    }
    expect(result_handler).to receive(:call).with(expected_result)

    payload.lines.each do |line|
      command_handler.handle_line(line.chomp)
    end
  end

  it 'should handle a multi-line value payload' do
    expected_result = {
      'mykey' => {
        value: "123\r\n45",
        flags: 5
      }
    }
    expect(result_handler).to receive(:call).with(expected_result)

    multiline_payload.lines.each do |line|
      command_handler.handle_line(line.chomp)
    end
  end

  it 'should handle a empty value payload' do
    expected_result = {
      'mykey' => {
        value: '',
        flags: 5
      }
    }
    expect(result_handler).to receive(:call).with(expected_result)

    empty_payload.lines.each do |line|
      command_handler.handle_line(line.chomp)
    end
  end

  it 'should handle a multi-value payload' do
    expected_result = {
      'mykey' => {
        value: '12345',
        flags: 5
      },
      'mykey2' => {
        value: '98765',
        flags: 7
      }
    }
    expect(result_handler).to receive(:call).with(expected_result)

    multi_payload.lines.each do |line|
      command_handler.handle_line(line.chomp)
    end
  end

  it 'should handle CLIENT_ERROR errors' do
    expect(result_handler).to receive(:call).with('get failed: failure yuck', true)
    command_handler.handle_line('CLIENT_ERROR failure yuck')
  end

  it 'should handle SERVER_ERROR errors' do
    expect(result_handler).to receive(:call).with('get failed: failure yuck', true)
    command_handler.handle_line('SERVER_ERROR failure yuck')
  end

  it 'should handle ERROR errors' do
    expect(result_handler).to receive(:call).with('get failed: unknown error', true)
    command_handler.handle_line('ERROR')
  end

  it 'should handle handle parse errors' do
    expect(result_handler).to receive(:call).with('could not parse line: FUNKY', true)
    expect {
      command_handler.handle_line('FUNKY')
    }.to raise_error(RuntimeError)
  end
end

describe 'MemcachedClient::SetCommandHandler' do
  let(:result_handler) { double('result_handler') }
  let(:command_handler) { MemcachedClient::VisibleSetCommandHandler.new(result_handler, 'mykey') }

  it 'should handle STORED response properly' do
    expect(result_handler).to receive(:call).with(nil)
    command_handler.handle_line('STORED')
  end

  it 'should handle CLIENT_ERROR errors' do
    expect(result_handler).to receive(:call).with('set failed for key mykey: failure yuck', true)
    command_handler.handle_line('CLIENT_ERROR failure yuck')
  end

  it 'should handle SERVER_ERROR errors' do
    expect(result_handler).to receive(:call).with('set failed for key mykey: failure yuck', true)
    command_handler.handle_line('SERVER_ERROR failure yuck')
  end

  it 'should handle ERROR errors' do
    expect(result_handler).to receive(:call).with('set failed for key mykey: unknown error', true)
    command_handler.handle_line('ERROR')
  end

  it 'should handle handle parse errors' do
    expect(result_handler).to receive(:call).with('could not parse line: FUNKY', true)
    expect {
      command_handler.handle_line('FUNKY')
    }.to raise_error(RuntimeError)
  end
end
