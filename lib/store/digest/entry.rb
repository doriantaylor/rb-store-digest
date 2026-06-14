require 'store/digest/readwrapper'
require 'store/digest/error'

require 'uri'
require 'uri/ni'
require 'mimemagic-dorian'

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
  Flags = Struct.new('Flags', :type_checked, :type_valid, :charset_checked,
                     :charset_valid, :encoding_checked, :encoding_valid,
                     :syntax_checked, :syntax_valid, :cache) do

    class << self
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
    type:     [/^(#{TOKEN}(?:\/#{TOKEN})?)$/on, -> c { MimeMagic[c] }],
    charset:  [/^(#{TOKEN})$/on,
               -> c { c = c.tr(?_, ?-).downcase; CHARSETS.fetch c, c } ],
    encoding: [/^(#{TOKEN})$/on,
               -> c { c = c.tr(?_, ?-).downcase; ENCODINGS.fetch c, c } ],
    language: [/^([a-z]{2,3}(?:[-_][0-9a-z]+)*)$/,
               -> c { c.downcase.tr(?_, ?-).gsub(/-*$/, '') } ],
  }

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

  def coerce_nn_int i
    case i
    when nil then 0
    when Numeric
      raise ArgumentError, 'size must be non-negative' if i < 0
      i.to_i
    else
      raise TypeError, 'size must be nil or Numeric'
    end
  end

  #
  def coerce_time t, k = nil
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
      raise TypeError, "Invalid type for #{k}: #{t.class}"
    end
  end

  def coerce_token t, k
    t = t.to_s.strip.downcase
    pat, norm = TOKENS[k]
    raise "#{k} #{t} does not match #{pat}" unless m = pat.match(t)
    norm.call m.captures.first
  end

  def coerce_digests digests, empty: false, normative: nil
    # we just sneak in the instance's algorithms
    self.class.coerce_digests digests, algorithms: algorithms,
      empty: empty, normative: normative
  end

  CACHE_TTL = 86400

  def compute_cache cache
    return unless cache
    if cache.is_a? Numeric
      # cache dtime should be relative to metadata parameter change time
      @ptime + cache
    elsif cache.is_a? Time
      cache
    elsif cache.respond_to? :to_time
      cache.to_time
    else
      (@store ? @store.cache_ttl : CACHE_TTL)
    end
  end

  # Returns metadata without calling the accessors and triggering a
  # scan.
  #
  # @return [Hash] the current set of metadata
  #
  def meta_hash content: false, store: false
    keys = %i[digests size ctime mtime ptime dtime
              flags type charset encoding language]
    keys.unshift :store   if store && @store
    keys.unshift :content if content && @content

    keys.each_with_object({}) do |k, h|
      v = "@#{k}"
      h[k] = instance_variable_get(v) if instance_variable_defined?(v)
    end
  end

  # Merge a metadata hash into the object.
  #
  # @param hash [Hash{Symbol=>Object}]
  #
  # @raise [Store::Digest::Error::Integrity]
  #
  # @return [void]
  #
  def merge_meta hash, content: false
    # do itt
    @content = hash[:content] if content and hash[:content]

    # check the byte size
    if hash[:size]
      s = coerce_nn_int hash[:size]
      raise Store::Digest::Error::Integrity,
        "Scanned size #{s} does not match asserted #{@size}" if
        @size and s != @size
      @size = s
    end

    # check the digests
    if hash[:digests]
      digests = coerce_digests(hash[:digests], normative: true)
      (@digests.keys & digests.keys).each do |k|
        scanned  = digests[k]
        asserted = @digests[k]
        raise Store::Digest::Error::CryptographicIntegrity,
          "Scanned digest #{scanned} does not match asserted #{asserted}" if
          scanned != asserted
      end

      # make sure wee also do the algorithms for parity
      @digests    = digests.transform_values(&:freeze).freeze
      @algorithms = digests.keys.to_set.freeze
    end

    # only update the type if it's more specific than the asserted one
    if hash[:type]
      t = coerce_token hash[:type], :type

      # warn "#{@type.inspect} -> #{t.inspect}"

      @type = (t.canonical || t) unless @type and !t.descendant_of?(@type)
      # @type = (t.canonical || t) if !@type || t.descendant_of?(@type)
    end

    %i[charset encoding language].each do |key|
      val = coerce_token(hash[key], key).freeze if hash[key]
      # note the distinction
      instance_variable_set("@#{key}", val) if hash.key? key
    end

    # mtime is special
    if hash[:mtime]
      # XXX TODO preserve older newer
      @mtime = coerce_time hash[:mtime], :mtime
    end

    %i[ctime ptime dtime].each do |key|
      val = coerce_time(hash[key], key).freeze if hash[key]
      # again note the distinction
      instance_variable_set("@#{key}", val) if hash.key? key
    end

    # finally we do the flags
    @flags = Flags.from(hash[:flags]) if hash[:flags]

    nil
  end

  # this is to swtich the content over
  #
  def dereference?
    @content = @content.call if @content.respond_to? :call
  end

  def seekable? io
    return false unless io.respond_to? :seek
    begin
      # this should be a noop
      io.seek 0, IO::SEEK_CUR
      true
    rescue Errno::ESPIPE, Errno::EINVAL
      false
    end
  end

  public

  # Create a new object, naively recording whatever is handed
  #
  # @note use {.scan} or {#scan} to populate
  #
  # @param content [IO, String, Proc, File, Pathname, ...] some content
  # @param store [Store::Digest] the associated store, if present
  # @param digests [Hash] the digests ascribed to the content
  # @param type [String] assert the object's MIME type
  # @param charset [String] the character set, if applicable
  # @param language [String] the (RFC5646) language tag, if applicable
  # @param encoding [String] the content-encoding (e.g. compression)
  # @param mtime [Time] assert object modification time
  # @param flags [Integer, Flags] validation state flags
  # @param strict [true, false] raise an error on bad input
  #
  # @return [Store::Digest::Entry] the object in question
  #
  def initialize content = nil, store: nil, digests: nil, mtime: nil,
      type: nil, charset: nil, encoding: nil, language: nil, flags: 0,
      cache: false, strict: false, scan: false, &block

    # set the associated store, if one is passed in
    if store
      raise 'Store must be an instance of Store::Digest' unless
        store.is_a? Store::Digest
      @store = store
    end


    now = Time.now
    @mtime   = mtime || now
    @digests = {}
    @scanned = false

    if content
      # this will give us something suitable to scan or it'll bail
      @content = Store::Digest::ReadWrapper.coerce content,
        thunk: true if content

      if !type and @content.respond_to?(:path) and path = @content.path
        type = MimeMagic.by_path(@content.path)
      end
    end

    type ||= MimeMagic[nil]

    # the following can be strings or symbols:
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

    # warn "wtf #{@type.inspect}"

    # we let the empty through
    digests = coerce_digests digests, empty: true
    if digests.is_a? Hash
      @digests    = digests
      @algorithms = digests.empty? ? algorithms : digests.keys.to_set
      @scanned    = !digests.empty?
    elsif !digests.empty?
      @algorithms = digests.to_set
    end

    # we use this for `#get`
    if block
      hash = block.call @content

      raise TypeError,
        "Block return value must be Hash, not #{hash.class}" unless
        hash.is_a? Hash
      #
      @scanned = true if hash[:digests]
      merge_meta hash, content: true
    elsif @content.nil?
      raise ArgumentError,
        'Must initialize with either content, or a block, or both'
    end

    # just make sure the times
    @ctime ||= now
    @mtime ||= mtime || @ctime
    @ptime ||= @ctime

    # set the flags
    @flags ||= Flags.from(flags || 0)
    if cache
      raise NotImplementedError, 'Associated store does not support caching' if
        @store and !@store.can_cache?
      @flags.cache = !!cache
      @dtime = compute_cache cache
    end

    # scan preemptively if so directed
    scan! if scan
  end

  attr_reader :store, :type, :charset, :language, :encoding,
    :ctime, :mtime, :ptime, :dtime, :flags

  TOKENS.keys.each do |key|
    define_method("#{key}=") { |val| coerce_token val, key }
  end

  # This will take an array or hash or individual symbol or string or
  # {URI::NI} object and try to coerce it into something it can use.
  #
  # * Individual strings/symbols/{URI::NI} objects will get wrapped in
  #   an array.
  # * Strings will be scanned for conformance to RFC6920 and
  #   transformed into {URI::NI} objects if they match, otherwise they
  #   will be turned into symbols and matched against the repertoire
  #   of hash algorithms.
  # * If a {URI::NI} object isn't valid (e.g., not the full length,
  #   algorithm not supported), this will raise an error; likewise if
  #   the symbol is not in the repertoire of algorithms.
  # * Arrays must contain all the same kind of thing (strings,
  #   symbols, {URI::NI} objects)
  # * Hash keys must coerce to symbols (via `#to_s`, `#to_sym`) that
  #   match the repertoire of algorithms.
  # * Hash values must either be a string representing the decimal,
  #   base64, or hexadecimal digest of a length corresponding to the
  #   algorithm in the key, or a string representing an RFC6920 URI,
  #   or a {URI::NI}.
  # * (Base64 strings may be padded or not, and use the standard
  #   non-URL-safe representation, or not)
  # * Strings will then subsequently be transformed into {URI::NI}
  #   objects.
  # * Hash values that are (either already or coerced into) {URI::NI}
  #   objects must be valid and their algorithms must match the hash
  #   key with which they are associated.
  #
  # The input (and thus the output) has two "moods":
  #
  # 1. _Anticipative_: "These are the digest algorithms we want to see
  #    hashes for."
  # 2. _Normative_: "These are the hashes we already have for the
  #    input, and it should match them when scanned."
  #
  # In general inputs that coerce to arrays (except arrays whose
  # contents coerce to {URI::NI} objects, which in turn will coerce to
  # hashes) are considered anticipative, whereas inputs that coerce to
  # hashes are considered normative. The return value will depend on
  # the adjudicated intent: `Array` for anticipative, `Hash` for
  # normative. The caller should inspect the return value to see which
  # it is, because the difference is whether a subsequent scan of the
  # content is intended to verify it (normative) or not (anticipative).
  #
  # @param digests [#to_sym, #to_s, URI::NI,
  #  #to_a<#to_sym,#to_s,URI::NI>, #to_h{#to_sym=>#to_s},
  #  #to_h{#to_sym=>URI::NI}] the thing to be coerced into digests
  # @param empty [false, true] whether the set is allowed to be empty
  # @param normative [nil, false, true] whether to assert the
  #  normative mood (`true`), the anticipative mood (`false`), or
  #  leave it to the caller (`nil`)
  #
  # @return [Array<Symbol>,Hash{Symbol=>URI::NI}]
  #
  def self.coerce_digests digests, algorithms: nil, empty: false, normative: nil
    algorithms ||= URI::NI.algorithms

    # handle nil
    digests = [] if digests.nil?

    # first we coerce into an array; note hashes respond to `#to_a`
    digests = [digests] unless digests.respond_to? :to_a

    raise ArgumentError,
      'Digest list can\'t be empty' if !empty and digests.empty?

    if digests.is_a? Hash
      out = digests.map do |k, v|
        # keys must go to symbols; symbols must be valid
        k = k.to_s.downcase.to_sym unless k.is_a? Symbol
        raise ArgumentError,
          "#{k} is not a supported algorithm in this configuration" unless
          algorithms.include? k

        # this should raise on any invalid values
        v = URI::NI.ingest k, v

        # then we assert that the result itself is valid
        raise ArgumentError, "Hash URI #{v} is invalid" unless v.valid?

        [k, v]
      end.to_h

      # note we are explicitly looking to see if normative is false
      # rather than nil
      return normative == false ? out.keys : out
    end

    # otherwise it should be an array so we'll make it into a set
    digests = digests.to_a.map do |thing|
      case thing
      when Symbol then thing
      when URI then URI::NI.ingest thing
      else
        # whatever it is, it should now be a string
        thing = thing.to_s
        if %r{^(?i:ni|https?)://}.match?(thing) and uri = try_uri(thing)
          uri
        else
          # turn it into a symbol
          thing.strip.downcase.to_sym
        end
      end
    end.uniq

    if digests.all? { |d| d.is_a? URI::NI }
      # we are expressly asking for anticipative if normative is literally false
      return digests.map(&:algorithm) if normative == false

      # otherwise if these are all digest URIs then this is normative;
      # return as a hash
      return digests.map do |d|
        raise ArgumentError,
          "#{d} is not a supported algorithm" unless
          algorithms.include? d.algorithm

        [d.algorithm.to_sym, d]
      end.to_h
    elsif digests.all? { |d| d.is_a? Symbol }
      raise ArgumentError, 'Normative expressly normative' if normative

      return digests
    end

    # if we get here, it's an error
    raise ArgumentError,
      'Input must coerce to either all URIs or all Symbols'
  end

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

    # coerce digests

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
    out = { digests: digests, size: bytes }

    if sample
      # felt cute lol
      out[:type] = %i[by_magic default_type].lazy.filter_map do |m|
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
    raise ArgumentError,
      'no store associated with the entry and none passed in' if
      [store, @store].all?(&:nil?)

    # use the internal store if one is not supplied
    # set the internal store if one is supplied and not present

    store ||= @store
    raise TypeError, 'Argument must be an instance of Store::Digest' unless
      store.is_a? Store::Digest

    # set the store unless we already have one
    @store ||= store

    # doyy obviously we need to do this
    rewind if scanned?

    # ok add the thing
    hash = store.send :add_raw, @content, **meta_hash
    merge_meta hash, content: true

    self
  end

  # Remove this entry from a store. Dissociates the entry from the
  # store in the process. Will not signal if the entry wasn't in the
  # store to begin with.
  #
  # @param store [nil, Store::Digest] the store to remove the entry
  # @param forget [false, true] whether to purge the entry completely
  #  from the metadata or just delete the blob
  #
  def remove store = nil, forget: false
    raise ArgumentError,
      'no store associated with the entry and none passed in' if
      [store, @store].all?(&:nil?)
    store ||= @store

    raise TypeError, 'store must be a Store::Digest instance' unless
      store.is_a? Store::Digest

    # eliminate the relationship
    @store = nil if @store.equal? store

    rm = forget ? :forget : true
    # this circumvents `private`; ignore return value
    store.send :get_raw, digests[store.primary], remove: rm

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
      blocksize: BLOCKSIZE, &block
    self.new content, store: store, digests: digests, mtime: mtime,
      type: type, language: language, charset: charset, encoding: encoding,
      scan: blocksize, &block
  end

  # Scan the blob if it hasn't already been scanned (idempotent).
  #
  # @return [self]
  #
  def scan
    scan! if @content && !scanned?
    self
  end

  STRINGIO_MAX = 2**16

  # Scan the blob unconditionally. May raise an error if the byte size
  # or digests are asserted in the constructor and don't match the scan.
  #
  # @raise [Store::Digest::Error:Integrity]
  #
  # @return [self]
  #
  def scan!
    raise Store::Digest::Error::Deleted, 'Entry has no content' unless @content

    if @store
      # we use the store if one is associated
      hash = @store.send :add_raw, @content, **meta_hash

      @content = hash[:content]
    elsif @content.respond_to? :rewind and seekable?(@content)
      # we don't need a temporary file; we'll just reuse this file handle
      @content.rewind
      hash = self.class.scan_raw @content, algorithms: @algorithms, type: true
      @content.rewind
    else
      # start with a stringio
      tmp = StringIO.new
      lam = -> buf do
        tmp << buf

        # check if it's too big
        if tmp.size >= STRINGIO_MAX
          # make an actual file
          file = Tempfile.create anonymous: true, binmode: true

          # put the string into it
          tmp.rewind
          file << tmp.read

          # reassign tmp with the file
          tmp = file

          # reassign lam with this condition removed so we don't
          # needlessly test it over and over with every iteration
          lam = -> buf { file << buf }
        end
      end

      # now we wrap lam in another block so it picks up the reassignment
      hash = self.class.scan_raw(
        @content, algorithms: @algorithms, type: true) { |buf| lam.call buf }
      tmp.rewind
      @content = tmp
    end

    # i suppose this is where the integrity is checked
    if @scanned
      # size
      raise Store::Digest::Error::Integrity,
        "Scanned size #{hash[:size]} does not match asserted #{@size}" if
        hash[:size] != @size

      # digests
      (@digests.keys & hash[:digests].keys).each do |k|
        scanned  = hash[:digests][k]
        asserted = @digests[k]
        raise Store::Digest::Error::CryptographicIntegrity,
          "Scanned digest #{scanned} does not match asserted #{asserted}" if
          scanned != asserted
      end
      # XXX also do content type??
    end

    merge_meta hash

    # unconditionally set this now
    @scanned = true

    self
  end

  # Returns true if the entry has already been scanned.
  #
  # @return [false, true]
  #
  def scanned?
    !!@scanned
  end

  # Iterate over the blob contents.
  #
  # @yieldparam chunk [String] the chunk of blob
  #
  # @return [self]
  #
  def each sep = $/, limit = nil, chomp: false, &block
    scan
    dereference?
    @content.each(sep, limit, chomp: chomp, &block)
  end

  # Emulate {IO#read}.
  #
  # @param length [Integer] the number of bytes to read
  #
  # @return [String, nil] up to `length` bytes or `nil` on EOF
  #
  def read length = nil, buffer = nil
    scan
    dereference?
    # this should be set by scan
    @content.read length, buffer
  end

  # Emulate {IO#gets}.
  #
  # @return [String] the next character
  #
  def gets sep = $/, chomp = false
    scan
    dereference?
    @content.gets sep, chomp
  end

  def seek offset, whence = IO::SEEK_SET
    scan
    dereference?
    @content.seek offset, whence
  end

  def pos
    scan
    dereference?
    @content.pos
  end

  alias_method :tell, :pos

  def pos= position
    scan
    dereference?
    @content.pos = position
  end

  # Emulate {IO#rewind}.
  #
  # @return [0] always zero
  #
  def rewind
    scan
    dereference?

    # content should be rewindable after a scan
    @content.rewind
  end

  # No-op of {IO#open} for parity.
  #
  # @note Once the blob is scanned, an internal file handle is opened
  #  and stays open.
  #
  # @return [self]
  #
  def open *args
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

  # Determine (if possible) if the object is in the store. Returns
  # `nil` if no store is associated with the entry, otherwise it will
  # query the store.
  #
  # @return [nil, false, true] the status of the entry
  #
  def stored?
    @store.has?(digests) if @store
  end

  # Return the algorithms used in the object.
  #
  # @return [Array]
  #
  def algorithms
    @algorithms ||= (@store || URI::NI).algorithms.to_set
  end

  #
  def digests
    scan
    @digests
  end

  # 
  #
  def size
    scan
    @size
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
  # @note This is a vestigial method since {Store::Digest::Entry}
  #  now proxies {IO} calls.
  #
  # @return [self, nil] no-op if there is content, nil if not.
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

  def flags= val
    @flags = Flags.from val
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
    @flags.type_checked
  end

  # Returns true if the content type has been checked _and_ is valid.
  #
  # @return [nil, false, true]
  #
  def type_valid?
    return nil unless @flags.type_checked
    @flags.type_valid
  end

  # Returns true if the character set has been checked.
  #
  # @return [false, true]
  #
  def charset_checked?
    @flags.charset_checked
  end

  # Returns true if the character set has been checked _and_ is valid.
  #
  # @return [nil, false, true]
  #
  def charset_valid?
    return nil unless @flags.charset_checked
    @flags.charset_valid
  end

  # Returns true if the content encoding (e.g. gzip, deflate) has
  # been checked.
  #
  # @return [false, true]
  #
  def encoding_checked?
    @flags.encoding_checked
  end

  # Returns true if the content encoding has been checked _and_ is valid.
  #
  # @return [nil, false, true]
  #
  def encoding_valid?
    return nil unless @flags.encoding_checked
    @flags.encoding_valid
  end

  # Returns true if the blob's syntax has been checked.
  #
  # @return [false, true]
  #
  def syntax_checked?
    @flags.syntax_checked
  end

  # Returns true if the blob's syntax has been checked _and_ is valid.
  #
  # @return [nil, false, true]
  #
  def syntax_valid?
    return nil unless @flags.syntax_checked
    @flags.syntax_valid
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

  # If the entry is flagged as cache and the expiry time is in the
  # past, then the entry is stale.
  #
  def stale?
    cache? && @dtime && @dtime < Time.now
  end

  # Just a plain old predicate to determine whether the blob has been
  # deleted from the store (but implicitly the metadata record
  # remains).
  #
  # @return [false, true]
  #
  def deleted?
    stale? or @dtime && !cache?
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
