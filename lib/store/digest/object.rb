require 'store/digest/version'

require 'uri'
require 'uri/ni'

class Store::Digest::Object

  private

  BLOCKSIZE = 65536

  TOKEN = '[^\x0-\x20()<>@,;:\\\"/\[\]?=\x7f-\\xff]+'

  # { key: [pattern, normalizer] } - assumes stripped and downcased
  TOKENS = {
    type:     [/^(#{TOKEN}(?:\/#{TOKEN})?)$/on, -> c { c }],
    charset:  [/^(#{TOKEN})$/on, -> c { c } ],
    encoding: [/^(#{TOKEN})$/on,
      -> c { m = /^x-(compress|gzip)$/.match(c); m ? m[1] : c } ],
    language: [/^([a-z]{2,3}(?:[-_][0-9a-z]+)*)$/, -> c { c.tr(?_, ?-) }],
  }

  # flag constants
  TYPE_CHECKED     = 1 << 0
  TYPE_VALID       = 1 << 1
  CHARSET_CHECKED  = 1 << 2
  CHARSET_VALID    = 1 << 3
  ENCODING_CHECKED = 1 << 4
  ENCODING_VALID   = 1 << 5
  SYNTAX_CHECKED   = 1 << 6
  SYNTAX_VALID     = 1 << 7

  LABELS = {
    size:     'Size (Bytes)',
    ctime:    'Added to Store',
    mtime:    'Last Modified',
    ptime:    'Properties Modified',
    dtime:    'Deleted',
    type:     'Content Type',
    language: '(Natural) Language',
    charset:  'Character Set',
    encoding: 'Content Encoding',
  }.freeze

  FLAG  = %i[content-type charset content-encoding syntax].freeze
  STATE = %i[unverified invalid recheck valid].freeze

  def coerce_time t, k
    case t
    when nil then nil
    when Time then t
    when -> dt { dt.respond_to? :to_time }
      t.to_time
    when Integer
      raise ArgumentError,
        "#{k} given as Integer must be non-negative" if t < 0
      Time.at t
    else
      raise ArgumentError, "Invalid type for #{k}: #{t.class}"
    end
  end

  def coerce_token t, k
    t = t.to_s.strip.downcase
    pat, norm = TOKENS[k]
    raise "#{k} does not match #{pat}" unless m = pat.match(x)
    norm.call m[1]
  end

  public
    
  #
  def initialize content = nil, digests: {}, size: 0,
      type: 'application/octet-stream', charset: nil, language: nil,
      encoding: nil, ctime: nil, mtime: nil, ptime: nil, dtime: nil, flags: 0

    # check input on content
    @content = case content
               when nil then nil
               when IO, Proc then content
               when String then StringIO.new content
               when Pathname then -> { content.open('rb') }
               else
                 raise ArgumentError,
                   "Cannot accept content given as #{content.class}"
               end

    # check input on digests
    @digests = case digests
               when Hash
                 # hash must be clean
                 digests.map do |k, v|
                   raise ArgumentError,
                     'Digest keys must be symbol-able' unless
                     k.respond_to? :to_sym
                   k = k.to_sym
                   raise ArgumentError,
                     'Digest values must be URI::NI' unless
                     v.is_a? URI::NI
                   raise ArgumentError,
                     'Digest key must match value algorithm' unless
                     k == v.algorithm
                   [k.to_sym, v.dup.freeze]
                 end.to_h
               when nil then {} # empty hash
               when Array
                 # only accepts array of URI::NI
                 digests.map do |x|
                   raise ArgumentError,
                     "Digests given as array can only be URI::NI, not #{x}" \
                     unless x.is_a? URI::NI
                   [x.algorithm, x.dup.freeze]
                 end.to_h
               when URI::NI then { digests.algorithm => digests.dup.freeze }
               else
                 # everything else is invalid
                 raise ArgumentError,
                   "Cannot coerce digests given as #{digests.inspect}"
               end

    # ctime, mtime, ptime, dtime should be all nil or nonnegative
    # integers or Time or DateTime
    b = binding
    %i[ctime mtime ptime dtime].each do |k|
      v = coerce_time(b.local_variable_get k, k)
      instance_variable_set "@#{k}", v
    end

    # size and flags should be non-negative integers
    %i[size flags].each do |k|
      x = b.local_variable_get k
      v = case x
          when nil then 0
          when Integer
            raise ArgumentError, "#{k} must be non-negative" if x < 0
            x
          else
            raise ArgumentError, "#{k} must be nil or an Integer"
          end
      instance_variable_set "@#{k}", v
    end

    # the following can be strings or symbols:
    TOKENS.keys.each do |k|
      if x = b.local_variable_get(k)
        instance_variable_set "@#{k}", coerce_token(x, k).freeze
      end
    end
  end

  attr_reader :digests, :size, :type, :charset, :language, :encoding,
    :ctime, :mtime, :ptime, :dtime, :flags

  #
  def self.scan content, digests: URI::NI.algorithms, mtime: nil,
      type: nil, language: nil, charset: nil, encoding: nil, &block
    self.new.scan content, digests: digests, mtime: mtime, type: type,
      language: language, charset: charset, encoding: encoding, &block
  end

  def scan content = nil, digests: URI::NI.algorithms, mtime: nil,
      type: nil, charset: nil, language: nil, encoding: nil,
      blocksize: BLOCKSIZE, &block
    # we put all the scanning stuff in here
    content = case content
              when nil      then self.content
              when IO       then content
              when String   then StringIO.new content
              when Pathname then content.open('rb')
              when Proc     then content.call
              else
                raise ArgumentError,
                  "Cannot scan content of type #{content.class}"
              end
    content.binmode unless content.binmode?

    # sane default for mtime
    mtime ||= content.respond_to?(:mtime) ? content.mtime : Time.now
    @mtime = mtime = coerce_time mtime, :mtime

    # eh, *some* code reuse
    b = binding
    TOKENS.keys.each do |k|
      if x = b.local_variable_get(k)
        instance_variable_set "@#{k}", coerce_token(x, k).freeze
      end
    end

    digests = case digests
              when Array  then digests
              when Symbol then [digests]
              else
                raise ArgumentError, 'Digests must be one or more symbol'
              end
    raise ArgumentError,
      "Invalid digest list #{digests - URI::NI.algorithms}" unless
      (digests - URI::NI.algorithms).empty?

    # set up the contexts
    digests = digests.map { |d| [d, URI::NI.context(d)] }.to_h

    # sample for mime type checking
    sample = StringIO.new
    
    while buf = content.read(blocksize)
      sample << buf if sample.pos < 2**12
      digests.values.each { |ctx| ctx << buf }
      block.call buf if block_given?
    end

    @digests = digests.transform_values { |v| URI::NI.compute v }.freeze

    self
  end

  # 
  def digest symbol
    digests[symbol]
  end

  alias_method :"[]", :digest

  # 
  def content
    content.is_a?(Proc) ? @content.call : @content
  end

  def content?
    !!@content
  end

  # Returns true if the content type has been checked.
  def type_checked?
    0 != @flags & TYPE_CHECKED
  end

  # Returns true if the content type has been checked _and_ is valid.
  def type_valid?
    0 != @flags & (TYPE_CHECKED|TYPE_VALID)
  end

  # Returns true if the character set has been checked.
  def charset_checked?
    0 != @flags & CHARSET_CHECKED
  end

  # Returns true if the character set has been checked _and_ is valid.
  def charset_valid?
    0 != @flags & (CHARSET_CHECKED|CHARSET_VALID)
  end

  # Returns true if the content encoding (e.g. gzip, deflate) has
  # been checked.
  def encoding_checked?
    0 != @flags & ENCODING_CHECKED
  end

  # Returns true if the content encoding has been checked _and_ is valid.
  def encoding_valid?
    0 != @flags & (ENCODING_CHECKED|ENCODING_VALID)
  end

  # Returns true if the blob's syntax has been checked.
  def syntax_checked?
    0 != @flags & SYNTAX_CHECKED
  end

  # Returns true if the blob's syntax has been checked _and_ is valid.
  def syntax_valid?
    0 != @flags & (SYNTAX_CHECKED|SYNTAX_VALID)
  end

  # Outputs a human-readable string representation of the object.
  def to_s
    out = "#{self.class}\n  Digests:\n"

    # disgorge the digests
    digests.each { |d| out << "    #{digest d}\n" }

    # now the fields
    MANDATORY.each { |m| out << "  #{LABELS[m]}: #{call m}\n" }
    OPTIONAL.each do |o|
      val = call o
      out << "  #{LABELS[o]}: #{val}\n" if val
    end

    # now the validation statuses
    out << "Validation:\n"
    FLAG.each_index do |i|
      x = f >> (3 - i) & 3
      out << ("    %-16s: %s\n" % [FLAG[i], STATE[x]])
    end

    out
  end
end
