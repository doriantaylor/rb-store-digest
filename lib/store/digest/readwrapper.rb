require 'store/digest/version'

require 'thread'

# This class is an attempt to normalize input so that it can be
# {#read} like an IO handle. Use the class method {.coerce} to
# determine if it's even necessary.
#
class Store::Digest::ReadWrapper

  private

  # Test if the object quacks like an IO.
  #
  # @param obj [Object] said object
  #
  # @return [false, true]
  #
  def self.quacks? obj
    obj.is_a?(IO) or %i[gets read close].all? do |m|
      obj.respond_to? m
    end
  end

  # close the pipe and join the thread.
  #
  # @return [void]
  #
  def cleanup
    @mutex.synchronize do
      return if @done
      @done = true
      @read.close unless @read.closed?
    end

    @thread.join
  end

  public

  # Attempt to coerce a suitable object or no-op.
  #
  # @param obj [Object] an object to be coerced
  #
  # @raise [ArgumentError] object not sufficiently coercible
  #
  # @return [ReadWrapper] a new around whatever this is
  #
  def self.coerce obj
    return obj if obj.is_a? self

    return obj if quacks? obj # no need for this if it can read

    # response bodies /don't do this but other stuff does
    if obj.respond_to? :call and obj.method(:call).arity == 0
      out = obj.call
      raise ArgumentError,
        'a `call` with no arguments must return an IO-like object' unless
        quacks? out

      return out
    end

    new obj
  end

  class << self
    alias_method :[], :coerce
  end

  # Initialize a wrapper.
  #
  # @param obj [#call, #each] a suitable object
  #
  # @raise [ArgumentError] said object is unsuitable
  #
  def initialize obj
    if obj.respond_to? :call
      raise ArgumentError,
        'Callable object needs an argument to take a write handle' if
        obj.method(:call).arity.abs == 0
    elsif obj.is_a? String
      obj = [obj]
    elsif !obj.respond_to?(:each)
      raise ArgumentError, 'Argument must respond to #call(write_fh) or #each'
    end

    @done  = false
    @mutex = Mutex.new

    @read, @write = IO.pipe

    @thread = Thread.new do
      if obj.respond_to? :call
        obj.call @write
      else
        obj.each { |x| @write << x.to_s.b }
      end
    rescue Errno::EPIPE # => e
      nil # not sure if we do anything here
    ensure
      @write.close unless @write.closed?
    end
  end

  # `gets` for parity with IO
  #
  # @return [String, nil]
  #
  def gets sep = $/, chomp = false
    unless @read.closed?
      out = @read.gets sep, chomp
      cleanup if out.nil?

      out
    end
  end

  # `read` for parity with IO
  #
  # @param maxlen [Integer] the length to read
  # @param string [String] an optional string
  #
  # @return [String, nil]
  #
  def read maxlen = nil, string = nil
    unless @read.closed?
      out = @read.read maxlen, string
      cleanup if out.nil?

      out
    end
  end

  # `close` for parity with IO
  #
  def close
    cleanup
    nil # close returns nil
  end
end
