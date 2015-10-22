# encoding: utf-8

$:.unshift File.expand_path('../lib', __FILE__)

require 'memcached-client/version'

Gem::Specification.new do |gem|
  gem.name          = 'memcached-client'
  gem.version       = MemcachedClient::VERSION
  gem.authors       = ['Sandeep Tamhankar']
  gem.email         = 'stamhankar@gmail.com'
  gem.description   = %q{Connects to a memcached instance and gets/sets keys.}
  gem.summary       = %q{Connects to a memcached instance and gets/sets keys.}
  gem.homepage      = 'https://github.com/stamhankar999/memcached-client'
  gem.files         = Dir['lib/memcached-client/*.rb', 'lib/*.rb']
  gem.test_files    = []
  gem.license       = 'MIT'
  gem.required_ruby_version = '>= 1.9.3'

  gem.add_development_dependency 'rake', '~> 10.0'

  gem.add_dependency 'ione', '~> 1.2'
end
