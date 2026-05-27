require 'spec_helper'

require 'store/digest/readwrapper'
require 'stringio'

RSpec.describe Store::Digest::ReadWrapper do
  context 'input coercion' do
    SDRW = Store::Digest::ReadWrapper

    it 'should coerce a string' do
      io = SDRW.coerce 'hi lol'

      expect(io).to be_a(SDRW)
    end

    it 'should noop when you pass in something that quacks like an IO' do
      io = SDRW[StringIO.new 'yo dawg']
      expect(io).to be_a(StringIO)
    end
  end

  context 'handling callables' do
    it 'should run a callable with no arguments to get a read handle' do
    end

    it 'should run a callable with no arguments to get an enumerable' do
    end

    it 'should run a callable where the first argument is a write handle' do
      text = 'a very lovely text'
      test = -> x { x.write text }
      io = SDRW[test]

      expect(io.read).to eq(text)
    end
  end
end
