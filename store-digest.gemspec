# -*- mode: enh-ruby -*-
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "store/digest/version"

Gem::Specification.new do |spec|
  spec.name          = 'store-digest'
  spec.version       = Store::Digest::VERSION
  spec.authors       = ['Dorian Taylor']
  spec.email         = ['code@doriantaylor.com']
  spec.license       = 'Apache-2.0'
  spec.homepage      = 'https://github.com/doriantaylor/rb-store-digest'
  spec.summary       = 'Lightweight, multi-digest content-addressable store'
  spec.description   = <<-DESC
  DESC

  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      f.match(%r{^(test|spec|features)/}) 
    end
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  # ruby
  spec.required_ruby_version = '~> 2.0'

  # dev/test dependencies
  spec.add_development_dependency 'bundler', '~> 1.17'
  spec.add_development_dependency 'rake',    '~> 10.0'
  spec.add_development_dependency 'rspec',   '~> 3.0'

  # stuff we use
end
