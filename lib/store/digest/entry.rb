require 'store/digest/readwrapper'
require 'store/digest/error'

require 'uri'
require 'uri/ni'
require 'mimemagic'

# This class represents an entry in the content-addressable store.
#
# An entry can be initialized with:
#
# * a `String` (or anything that can `#to_s`)
# * an `Array` of strings (or anything that can `#each`)
# * a `Pathname` (as long as it refers to a file that can be opened for reading)
# * an `IO` object (as long as it's finite, such as a `File`, but
#   it's your problem to ensure that it is)
# * anything that can `#read` (same deal on the finitude)
# * and two kinds of `#call`s:
#   * zero arity, which is expected return something that quacks like
#     a file handle,
#   * nonzero arity, where the first argument is expected to be
#     something that [behaves like a write
#     handle](https://github.com/rack/rack/blob/main/SPEC.rdoc#streaming-body).
#
# This behaviour is so {Store::Digest::Entry} instances can be dropped
# into `Rack` request and response bodies and replace/consume whatever
# was in there before. As such, this class implements {#each},
# {#gets}, {#read}, {#rewind}, and {#close} to emulate an `Enumerable`
# and/or `IO` handle.
#
# Content is scanned lazily (i.e., not until you invoke any of the
# accessors or the {#scan}/{#scan!} or {#add_to} methods) unless you
# tell the constructor to be `eager:`. These objects are not
# associated with a store by default. You must {#initialize} with a
# reference to `store:`, {#add_to} a store later, or use
# {Store::Digest#add}, which returns one of these objects.
#
# If you initialize one of these objects with one or more hashes, it
# is assumed that it has already been scanned and the hashes are
# representative. If, however, you force a {#scan!}, it _will_ raise
# an error if the supplied hashes don't match.
#
class Store::Digest::Entry

  # These is a struct for the bank of flags, with a couple of extra
  # methods for parsing
  #
  Flags = Struct.new(
    'Flags', :type_checked, :type_valid, :charset_checked, :charset_valid,
    :encoding_checked, :encoding_valid, :syntax_checked, :syntax_valid, :cache
  ) do |cls|

    class << cls
      # Initialize a struct of flags from arbitrary input
      #
      # @param arg [Store::Digest::Entry::Flags, Integer, #to_h, #to_a]
      #
      # @return [Store::Digest::Entry::Flags]
      #
      def from arg
        # get the length since we use it in a few places
        len = self.members.size

        if arg.is_a? Integer
          tmp = arg.digits(2).first(len)
        elsif arg.is_a? self
          # noop
          return arg
        elsif arg.is_a? Hash
          tmp = arg.slice(*self.members).transform_values do |v|
            !!(v && v != 0)
          end
          return self.[](**tmp)
        elsif arg.respond_to? :to_a
          tmp = arg.to_a.first(len)
        else
          raise ArgumentError, 'Input must be an integer or array'
        end

        # append these
        tmp += [false] * (len - tmp.size) if tmp.size < len

        # make sure these are true/false
        tmp.map! { |b| !!(b && b != 0) }

        # we do this because `new` doesn't do this
        self.[](*tmp)
      end

      # Turn an arbitrary {Array} back into an {Integer}.
      #
      # @param array [Array]
      #
      # @return [Integer]
      #
      def to_i array
        array.to_a.reverse.reduce(0) { |acc, b| (acc << 1) | (b ? 1 : 0) }
      end
    end

    # wish there was a cleaner way to do derive individual instance
    # methods from class methods
    begin
      cm = singleton_method :to_i
      define_method(:to_i) { cm.call to_a }
    end
  end

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
  IS_CACHE         = 1 << 8

  LABELS = {
    size:     'Size (Bytes)',
    ctime:    'Added to Store',
    mtime:    'Last Modified',
    ptime:    'Properties Modified',
    dtime:    'Deleted (Expires)',
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

  # Create a new object, naively recording whatever is handed
  #
  # @note use {.scan} or {#scan} to populate
  #
  # @param content [IO, String, Proc, File, Pathname, ...] some content
  # @param store [Store::Digest] the associated store, if present
  # @param digests [Hash] the digests ascribed to the content
  # @param size [Integer] assert the object's size
  # @param type [String] assert the object's MIME type
  # @param charset [String] the character set, if applicable
  # @param language [String] the (RFC5646) language tag, if applicable
  # @param encoding [String] the content-encoding (e.g. compression)
  # @param ctime [Time] assert object creation time
  # @param mtime [Time] assert object modification time
  # @param ptime [Time] assert object metadata parameter modification time
  # @param dtime [Time] assert object deletion time
  # @param flags [Integer] validation state flags
  # @param strict [true, false] raise an error on bad input
  # @param fresh [true, false] assert "freshness" of object vis-a-vis the store
  #
  # @return [Store::Digest::Entry] the object in question
  #
  def initialize content = nil, store: nil, digests: {}, size: 0,
      type: 'application/octet-stream', charset: nil, language: nil,
      encoding: nil, ctime: nil, mtime: nil, ptime: nil, dtime: nil,
      flags: 0, strict: true, fresh: false

    # snag this immediately
    @fresh = !!fresh

    # check input on content
    @content = case content
               when nil then nil
               when IO, StringIO, Proc then content
               when String then StringIO.new content
               when Pathname then -> { content.expand_path.open('rb') }
               when -> x { %i[read seek pos].all? { |m| x.respond_to? m } }
                 content
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

    # set the flags
    @flags = Flags.from(flags || 0)

    @size = case size
            when nil then 0
            when Numeric
              raise ArgumentError, 'size must be non-negative' if size < 0
              size.to_i
            else
              raise ArgumentError, 'size must be nil or Numeric'
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

  # Scan a blob and return the digests and byte count.
  #
  # @note The `content` is assumed to be at position zero.
  #
  # @param content [#read] the object to be scanned
  # @param algorithms [Array<Symbol,#to_sym>] the algorithms
  # @param blocksize [Integer] the block size to use
  # @param type [false, true] scan content for media type
  #
  # @yieldparam [String] a chunk of input
  #
  # @raise [ArgumentError] the content can't be coerced to
  #  something that quacks like `#read`
  # @raise [ArgumentError] the algorithms supplied aren't supported
  #
  # @return [Array(Hash{Symbol=>URI::NI}, Integer)] a pair containing
  #  a hash of the digests and the size in bytes of the blob.
  #
  def self.scan_raw content, algorithms: URI::NI.algorithms,
      blocksize: BLOCKSIZE, type: false, &block
    # this will raise if it can't be coerced
    content = Store::Digest::ReadWrapper.coerce content

    digests = begin
                case algorithms
                when Array,  -> x { x.respond_to? :to_a }
                  algorithms.to_a.map(&:to_sym)
                when Symbol, -> x { x.respond_to? :to_sym }
                  [algorithms.to_sym]
                else
                  raise ArgumentError
                end
              rescue ArgumentError, TypeError, NoMethodError
                raise ArgumentError,
                  "Digest algorithms must be coercible to an Array of Symbols"
              end

    # oh this shouldn't be empty btw
    raise ArgumentError, 'Algorithm list should not be empty' if digests.empty?

    # double-check if the digests are supported
    raise ArgumentError,
      "Unsupported digest algorithm(s) #{digests - URI::NI.algorithms}" unless
      (digests - URI::NI.algorithms).empty?

    # now queue up the contexts
    digests = digests.map { |d| [d, URI::NI.context(d)] }.to_h

    # we'll just make a uniform sequence to cycle through, why not
    procs = digests.values.map { |u| -> buf { u << buf } }
    procs << block if block

    if type
      sample = StringIO.new
      procs << -> buf do
        sample << buf
        # take this out of the loop if we have enough
        procs.pop if sample.pos >= SAMPLE
      end
    end

    bytes = 0
    while buf = content.read(blocksize)
      buf = buf.to_s.b # ensure these are bytes we're reading
      bytes += buf.size
      procs.each { |b| b.call buf }
    end

    # apparently i do this because i painted myself into a corner with
    # URI::NI and/or past me previously discovered that there is much
    # more to the hash state than just the digest itself and forgot to
    # tell later-past me when i discovered it a second time around
    digests = digests.map do |k, v|
      [k, URI::NI.compute(v, algorithm: k).freeze]
    end.to_h

    # return the gathered information; everything else is out of band
    out = [digests, bytes]

    if sample
      # felt cute lol
      out << %i[by_magic default_type].lazy.filter_map do |m|
        sample.rewind
        MimeMagic.send m, sample
      end.first
    end

    out
  end

  # Add this entry to a {Store::Digest} instance.
  #
  # @note This entry will become associated with the store if it isn't
  #  already. If this entry has already been scanned, it will be
  #  scanned again.
  #
  def add store = nil
    store ||= @store
    raise TypeError, 'Argument must be an instance of Store::Digest' unless
      store.is_a? Store::Digest
    raise 
    # set the store unless we already have one
    @store ||= store
    content, meta = store.add_raw(@content, **get_meta)

    self
  end

  # Remove this entry from a store.
  #
  # 
  #
  def remove store = nil, copy: false, forget: false
    dtime = Time.now
    store.remove_raw digests, dtime: dtime, forget: forget

    self
  end

  # Preemptively scan a blob and return an entry.
  #
  # @param content [String, Pathname, IO, #each, #read, #call]
  #  anything that represents bytes or can be coerced or wrapped by
  #  {Store::Digest::ReadWrapper}
  #
  # @param store [Store::Digest]
  # @param digests [Array<Symbol,#to_sym,URI::NI>, Hash{Symbol=>URI::NI}]
  #
  # @return [Store::Digest::Entry]
  #
  def self.scan content, store: nil, digests: URI::NI.algorithms, mtime: nil,
      type: nil, language: nil, charset: nil, encoding: nil,
      blocksize: BLOCKSIZE, , &block
    self.new content, store: store, digests: digests, mtime: mtime,
      type: type, language: language, charset: charset, encoding: encoding,
      blocksize: blocksize, strict: strict, scan: scan, &block
  end

  # Scan the blob if it hasn't already been scanned.
  #
  #
  #
  # @return [self]
  #
  def scan
    # 
    scan! unless scanned?
    self
  end

  # Scan the blob unconditionally. May raise an error 
  # 
  def scan!
  end

  def scanned?
  end

  def scan content = nil, into = nil, digests: URI::NI.algorithms, mtime: nil,
      type: nil, charset: nil, language: nil, encoding: nil,
      blocksize: BLOCKSIZE, strict: true, fresh: nil, &block
    # update freshness if there is something to update
    @fresh = !!fresh unless fresh.nil?
    # we put all the scanning stuff in here
    content = case content
              when nil          then self.content
              when IO, StringIO then content
              when String       then StringIO.new content
              when Pathname     then content.open('rb')
              when Proc
                if content.arity == 0
                  content.call
                else
                  x = StringIO.new
                  content.call x
                  x
                end
              when -> x { %i[read seek pos].all? { |m| x.respond_to? m } }
                content
              else
                raise ArgumentError,
                  "Cannot scan content of type #{content.class}"
              end
    content.binmode if content.respond_to? :binmode

    # sane default for mtime
    @mtime = coerce_time(mtime || @mtime ||
                         (content.respond_to?(:mtime) ? content.mtime : Time.now(in: ?Z)), :mtime)

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
      block.call buf if block
    end

    # seek the content back to the front and store it
    content.seek 0, 0
    @content = content

    # set up the digests
    @digests = digests.map do |k, v|
      [k, URI::NI.compute(v, algorithm: k).freeze]
    end.to_h.freeze

    # ensure there is the most generic of possible types
    type ||= 'application/octet-stream'.freeze

    # obtain the sampled content type
    ts = MimeMagic.by_magic(sample) || MimeMagic.default_type(sample)
    if content.respond_to? :path
      # may as well use the path if it's available and more specific
      ps = MimeMagic.by_path(content.path.to_s)
      # XXX the need to do ts.to_s is a bug in mimemagic
      ts = ps if ps and ps.descendant_of?(ts.to_s)
    end

    # set the type to ts if it is more specific
    @type = ts.descendant_of?(type.to_s) ? ts.to_s.freeze :
      type.to_s.dup.downcase.freeze

    self
  end

  # Force the rescanning of the entry object.
  #
  # @raise if supplied hashes do not match
  # @raise if the object is deleted
  #
  def scan!
  end

  # Iterate over the blob contents.
  #
  # @yieldparam chunk [String] the chunk of blob
  #
  # @return [self]
  #
  def each &block
    scan

    while buf = read(BLOCKSIZE)
      block.call buf
    end

    self
  end

  # Emulate {IO#read}.
  #
  # @param length [Integer] the number of bytes to read
  #
  # @return [String, nil] up to `length` bytes or `nil` on EOF
  #
  def read length = nil, buffer = nil
    scan

    # the first read kicks off the proxy
  end

  # Emulate {IO#gets}.
  #
  # @return [String] the next character
  #
  def gets sep = $/, chomp = false
    scan
    # 
  end

  # Emulate {IO#rewind}.
  #
  # @return [0] always zero
  #
  def rewind
    # if we hit this and we haven't switched out 
    0
  end

  # No-op of {IO#open}.
  #
  # @return [self]
  #
  def open
    rewind
    self
  end

  # No-op of {IO#close}.
  #
  # @return [self]
  #
  def close
    rewind
    self
  end

  # Determine (or set) whether the object is "fresh", i.e. whether it
  # is new (or restored), or had been previously been in the store.
  #
  # @return [true, false]
  #
  def fresh?
    !!@fresh
  end

  # Override the freshness state
  #
  # @param state [false, true]
  #
  # @return [void]
  #
  def fresh= state
    @fresh = !!state
  end

  # Return the algorithms used in the object.
  #
  # @return [Array]
  #
  def algorithms
    (digests || {}).keys.sort
  end

  # Return a particular digest. Returns nil if there is no match.
  #
  # @param symbol [Symbol, #to_s, #to_sym] the digest
  #
  # @return [URI::NI, nil]
  #
  def digest symbol
    raise ArgumentError, "This method takes a symbol" unless
      symbol.respond_to? :to_sym
    digests[symbol.to_sym]
  end

  alias_method :"[]", :digest

  # Returns the content stored in the object.
  #
  # @return [self, nil] no-op if 
  #
  def content
    self if @content
  end

  # Determines if there is content embedded in the object.
  #
  # @return [false, true]
  #
  def content?
    !!@content
  end

  # Returns the type and charset, suitable for an HTTP header.
  #
  # @return [String]
  #
  def type_charset
    out = type.to_s
    out += ";charset=#{charset}" if charset
    out
  end

  # Determines if the object has been scanned.
  #
  # @return [false, true]
  #
  def scanned?
    !@digests.empty?
  end

  # Returns whether the object is cache.
  #
  # @return [false, true]
  #
  def cache?
    !!@flags.cache
  end

  # Assigns the cache status.
  #
  # @param value [false, true] anything falsy/truthy
  #
  # @return [void]
  #
  def cache= value
    @flags.cache = !!value
  end

  # XXX i'm keeping these as-is for now

  # Returns true if the content type has been checked.
  #
  # @return [false, true]
  #
  def type_checked?
    0 != @flags.to_i & TYPE_CHECKED
  end

  # Returns true if the content type has been checked _and_ is valid.
  #
  # @return [false, true]
  #
  def type_valid?
    0 != @flags.to_i & (TYPE_CHECKED|TYPE_VALID)
  end

  # Returns true if the character set has been checked.
  #
  # @return [false, true]
  #
  def charset_checked?
    0 != @flags.to_i & CHARSET_CHECKED
  end

  # Returns true if the character set has been checked _and_ is valid.
  #
  # @return [false, true]
  #
  def charset_valid?
    0 != @flags.to_i & (CHARSET_CHECKED|CHARSET_VALID)
  end

  # Returns true if the content encoding (e.g. gzip, deflate) has
  # been checked.
  #
  # @return [false, true]
  #
  def encoding_checked?
    0 != @flags.to_i & ENCODING_CHECKED
  end

  # Returns true if the content encoding has been checked _and_ is valid.
  #
  # @return [false, true]
  #
  def encoding_valid?
    0 != @flags.to_i & (ENCODING_CHECKED|ENCODING_VALID)
  end

  # Returns true if the blob's syntax has been checked.
  #
  # @return [false, true]
  #
  def syntax_checked?
    0 != @flags.to_i & SYNTAX_CHECKED
  end

  # Returns true if the blob's syntax has been checked _and_ is valid.
  #
  # @return [false, true]
  #
  def syntax_valid?
    0 != @flags.to_i & (SYNTAX_CHECKED|SYNTAX_VALID)
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
  #
  # @return [false, true]
  #
  def deleted?
    !!@dtime
  end

  # Return the object as a hash. Omits the content by default.
  #
  # @param content [false, true] include the content if true
  # @return [Hash] the object as a hash
  #
  def to_h content: false
    main = %i[content digests]
    main.shift unless content
    (main + MANDATORY + OPTIONAL + [:flags]).map do |k|
      [k, send(k).dup]
    end.to_h
  end

  # Outputs a human-readable string representation of the object.
  #
  # @return [String] said representation
  #
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
      x = flags.to_i >> (3 - i) & 3
      out << ("  %-16s: %s\n" % [FLAG[i], STATE[x]])
    end

    out
  end
end

Store::Digest::Object = Store::Digest::Entry
