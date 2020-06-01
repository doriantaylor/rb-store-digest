require 'store/digest/version'

require 'uri'
require 'uri/ni'
require 'mimemagic'
require 'mimemagic/overlay'

class MimeMagic
  # XXX erase this when these methods get added

  unless self.method_defined? :binary?
    def self.binary? thing
      sample = nil

      # get some stuff out of the IO or get a substring
      if %i[tell seek read].all? { |m| thing.respond_to? m }
        pos = thing.tell
        thing.seek 0, 0
        sample = thing.read 1024
        thing.seek pos
      elsif thing.respond_to? :to_s
        sample = thing.to_s[0,1024]
      else
        raise ArgumentError, "Cannot sample an instance of {thing.class}"
      end

      # consider this to be 'binary' if empty
      return true if sample.nil? or sample.empty?
      # control codes minus ordinary whitespace
      /[\x0-\x8\xe-\x1f\x7f]/n.match?(sample) ? true : false
    end
  end

  unless self.method_defined? :default_type
    def self.default_type thing
      new self.binary?(thing) ? 'application/octet-stream' : 'text/plain'
    end
  end
end

class Store::Digest::Object

  private

  SAMPLE    = 2**13 # must be big enough to detect ooxml
  BLOCKSIZE = 2**16

  CHARSETS = [
    %w[utf8 utf-8],
    %w[iso8859-1 iso-8859-1],
  ].map { |k, v| [k.freeze, v.freeze] }.to_h.freeze

  ENCODINGS = [
    %w[x-compress compress],
    %w[x-gzip gzip],
  ].map { |k, v| [k.freeze, v.freeze] }.to_h.freeze

  TOKEN = '[^\x0-\x20()<>@,;:\\\"/\[\]?=\x7f-\\xff]+'

  # { key: [pattern, normalizer] } - assumes stripped and downcased
  TOKENS = {
    type:     [/^(#{TOKEN}(?:\/#{TOKEN})?)$/on, -> c { c.downcase }],
    charset:  [/^(#{TOKEN})$/on,
      -> c { c = c.tr(?_, ?-).downcase; CHARSETS.fetch c, c } ],
    encoding: [/^(#{TOKEN})$/on,
      -> c { c = c.tr(?_, ?-).downcase; ENCODINGS.fetch c, c } ],
    language: [/^([a-z]{2,3}(?:[-_][0-9a-z]+)*)$/,
      -> c { c.downcase.tr(?_, ?-).gsub(/-*$/, '') } ],
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

  MANDATORY = %i[size ctime mtime ptime]
  OPTIONAL  = %i[dtime type language charset encoding]
  FLAG      = %i[content-type charset content-encoding syntax].freeze
  STATE     = %i[unverified invalid recheck valid].freeze

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
    raise "#{k} #{t} does not match #{pat}" unless m = pat.match(t)
    norm.call m[1]
  end

  public

  #
  def initialize content = nil, digests: {}, size: 0,
      type: 'application/octet-stream', charset: nil, language: nil,
      encoding: nil, ctime: nil, mtime: nil, ptime: nil, dtime: nil, flags: 0,
      strict: true

    # check input on content
    @content = case content
               when nil then nil
               when IO, StringIO, Proc then content
               when String then StringIO.new content
               when Pathname then -> { content.expand_path.open('rb') }
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
      v = coerce_time(b.local_variable_get(k), k)
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
        x = if strict
              coerce_token(x, k)
            else
              coerce_token(x, k) rescue nil
            end
        instance_variable_set "@#{k}", x.freeze if x
      end
    end
  end

  # XXX come up with a policy for these that isn't stupid, plus input sanitation
  attr_reader :digests, :size
  attr_accessor :type, :charset, :language, :encoding,
    :ctime, :mtime, :ptime, :dtime, :flags

  #
  def self.scan content, digests: URI::NI.algorithms, mtime: nil,
      type: nil, language: nil, charset: nil, encoding: nil,
      blocksize: BLOCKSIZE, strict: true, &block
    self.new.scan content, digests: digests, mtime: mtime, type: type,
      language: language, charset: charset, encoding: encoding,
      blocksize: blocksize, strict: strict, &block
  end

  def scan content = nil, digests: URI::NI.algorithms, mtime: nil,
      type: nil, charset: nil, language: nil, encoding: nil,
      blocksize: BLOCKSIZE, strict: true, &block
    # we put all the scanning stuff in here
    content = case content
              when nil          then self.content
              when IO, StringIO then content
              when String       then StringIO.new content
              when Pathname     then content.open('rb')
              when Proc         then content.call
              when -> x { %i[read seek pos].all? { |m| x.respond_to? m } }
                content
              else
                raise ArgumentError,
                  "Cannot scan content of type #{content.class}"
              end
    content.binmode if content.respond_to? :binmode

    # sane default for mtime
    @mtime = coerce_time(mtime || @mtime ||
      (content.respond_to?(:mtime) ? content.mtime : Time.now), :mtime)

    # eh, *some* code reuse
    b = binding
    TOKENS.keys.each do |k|
      if x = b.local_variable_get(k)
        x = if strict
              coerce_token(x, k)
            else
              coerce_token(x, k) rescue nil
            end
        instance_variable_set "@#{k}", x.freeze if x
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
    sample = StringIO.new ''
    @size  = 0
    while buf = content.read(blocksize)
      @size += buf.size
      sample << buf if sample.pos < SAMPLE
      digests.values.each { |ctx| ctx << buf }
      block.call buf if block_given?
    end

    # seek the content back to the front and store it
    content.seek 0, 0
    @content = content

    # set up the digests
    @digests = digests.map do |k, v|
      [k, URI::NI.compute(v, algorithm: k).freeze]
    end.to_h.freeze

    # obtain the sampled content type
    ts = MimeMagic.by_magic(sample) || MimeMagic.default_type(sample)
    if content.respond_to? :path
      # may as well use the path if it's available and more specific
      ps = MimeMagic.by_path(content.path)
      ts = ps if ps and ps.child_of?(ts)
    end
    @type = !type || ts.child_of?(type) ? ts.to_s : type

    self
  end

  # Return the algorithms used in the object.
  # @return [Array]
  def algorithms
    (@digests || {}).keys.sort
  end

  # Return a particular digest. Returns nil if there is no match.
  # @param symbol [Symbol, #to_s, #to_sym] the digest
  # @return [Symbol, nil]
  def digest symbol
    raise ArgumentError, "This method takes a symbol" unless
      symbol.respond_to? :to_sym
    digests[symbol.to_sym]
  end

  alias_method :"[]", :digest

  # Returns the content stored in the object.
  # @return [IO]
  def content
    @content.is_a?(Proc) ? @content.call : @content
  end

  # Determines if there is content embedded in the object.
  # @return [false, true]
  def content?
    !!@content
  end

  # Returns the type and charset, suitable for an HTTP header.
  # @return [String]
  def type_charset
    out = type.to_s
    out += ";charset=#{charset}" if charset
    out
  end

  # Determines if the object has been scanned.
  # @return [false, true]
  def scanned?
    !@digests.empty?
  end

  # Returns true if the content type has been checked.
  # @return [false, true]
  def type_checked?
    0 != @flags & TYPE_CHECKED
  end

  # Returns true if the content type has been checked _and_ is valid.
  # @return [false, true]
  def type_valid?
    0 != @flags & (TYPE_CHECKED|TYPE_VALID)
  end

  # Returns true if the character set has been checked.
  # @return [false, true]
  def charset_checked?
    0 != @flags & CHARSET_CHECKED
  end

  # Returns true if the character set has been checked _and_ is valid.
  # @return [false, true]
  def charset_valid?
    0 != @flags & (CHARSET_CHECKED|CHARSET_VALID)
  end

  # Returns true if the content encoding (e.g. gzip, deflate) has
  # been checked.
  # @return [false, true]
  def encoding_checked?
    0 != @flags & ENCODING_CHECKED
  end

  # Returns true if the content encoding has been checked _and_ is valid.
  # @return [false, true]
  def encoding_valid?
    0 != @flags & (ENCODING_CHECKED|ENCODING_VALID)
  end

  # Returns true if the blob's syntax has been checked.
  # @return [false, true]
  def syntax_checked?
    0 != @flags & SYNTAX_CHECKED
  end

  # Returns true if the blob's syntax has been checked _and_ is valid.
  # @return [false, true]
  def syntax_valid?
    0 != @flags & (SYNTAX_CHECKED|SYNTAX_VALID)
  end

  %i[ctime mtime ptime dtime].each do |k|
    define_method "#{k}=" do |v|
      instance_variable_set "@#{k}", coerce_time(v, k).freeze
    end
  end

  %i[type charset encoding language].each do |k|
    define_method "#{k}=" do |v|
      instance_variable_set "@#{k}", coerce_token(v, k).freeze
    end

    define_method "#{k}_ok?" do |v|
      TOKENS[k].first.match? v
    end
  end

  # Just a plain old predicate to determine whether the blob has been
  # deleted from the store (but implicitly the metadata record
  # remains).
  # @return [false, true]
  def deleted?
    !!@dtime
  end

  # Return the object as a hash. Omits the content by default.
  # @param content [false, true] include the content if true
  # @return [Hash] the object as a hash
  def to_h content: false
    main = %i[content digests]
    main.shift unless content
    (main + MANDATORY + OPTIONAL + [:flags]).map do |k|
      [k, send(k).dup]
    end.to_h
  end

  # Outputs a human-readable string representation of the object.
  def to_s
    out = "#{self.class}\n  Digests:\n"

    # disgorge the digests
    digests.values.sort { |a, b| a.to_s <=> b.to_s }.each do |d|
      out << "    #{d}\n"
    end

    # now the fields
    MANDATORY.each { |m| out << "  #{LABELS[m]}: #{send m}\n" }
    OPTIONAL.each do |o|
      val = send o
      out << "  #{LABELS[o]}: #{val}\n" if val
    end

    # now the validation statuses
    out << "Validation:\n"
    FLAG.each_index do |i|
      x = flags >> (3 - i) & 3
      out << ("  %-16s: %s\n" % [FLAG[i], STATE[x]])
    end

    out
  end
end
