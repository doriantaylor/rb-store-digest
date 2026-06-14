require 'store/digest/version'
require 'store/digest/driver'
require 'store/digest/entry'

# This is a general-purpose content-addressable store that interfaces
# via [RFC6920](https://datatracker.ietf.org/doc/html/rfc6920) addresses.
#
# Since a content-addressable store traffics in immutable blobs of
# bytes, the main interface is remarkably terse:
#
# * {#add} a blob-like object or existing {Store::Digest::Entry},
# * {#get} an entry from the store (if it exists), if you know one of
#   its hash URIs,
# * or, {#remove} it.
#
# {Store::Digest} scans and stores multiple digest algorithms at once,
# since clients may only have a hash for a blob in a particular
# algorithm, and individual algorithms may get compromised from time
# to time. The set of algorithms is configurable, and fixed for each
# store instance when it is created.
#
# The currency of {Store::Digest}, then, is the {URI::NI} and the
# {Store::Digest::Entry}. There is also {Store::Digest::ReadWrapper},
# a small helper class capable of coercing non-IO-like objects
# (particulary those which one might find in a {Rack} message body)
# into something that behaves enough like an {IO} blob that it can be
# scanned. {Store::Digest::Entry} objects also masquerade as blobs
# with additional metadata.
#
class Store::Digest
  private

  def coerce_object obj, type: nil, charset: nil,
      language: nil, encoding: nil, mtime: nil, strict: true
    obj = case obj
          when Store::Digest::Entry
            obj.dup
          when URI::NI
            # just return the uri
            Store::Digest::Entry.new digests: obj,
              type: type, charset: charset, language: language,
              encoding: encoding, mtime: mtime
          when IO, String, StringIO,
              -> x { %i[seek pos read].all? { |m| x.respond_to? m } }
            # assume this is going to be scanned later
            Store::Digest::Entry.new obj,
              type: type, charset: charset, language: language,
              encoding: encoding, mtime: mtime
          when Pathname
            # actually open pathnames that are handed directly into S::D
            Store::Digest::Entry.new obj.expand_path.open('rb'),
              type: type, charset: charset, language: language,
              encoding: encoding, mtime: mtime
          else
            raise ArgumentError,
              "Can't coerce a #{obj.class} to Store::Digest::Entry"
          end

    # overwrite the user-mutable metadata
    b = binding
    %i[type charset language encoding mtime].each do |field|
      begin
        if x = b.local_variable_get(field)
          obj.send "#{field}=", x
        end
      rescue RuntimeError => e
        raise e if strict
      end
    end

    obj
  end

  # Squeeze a digest URI (or several) out of the input, if possible.
  #
  # @param obj [URI::NI, Array<URI::NI>, Hash{Symbol=>URI::NI},
  #  Store::Digest::Entry] the thing to get URIs from
  # @param select [false, true] whether to pick the "best" URI from a
  #  set or hash thereof
  #
  # @raise [ArgumentError] if the URIs can't be coerced
  #
  # @return [URI::NI, Hash{Symbol=>URI::NI}]
  #
  def coerce_uri obj, select: true
    if obj.is_a? Store::Digest::Entry
      digests = obj.digests

      # this shouldn't happen but you never know
      raise ArgumentError, 'Digest list is empty' if digests.empty?
    else
      # this can also raise if it fails to coerce
      digests = Store::Digest::Entry.coerce_digests obj, normative: true
    end

    # we should have a hash at this point
    return digests.values unless select

    # if we have this then return it
    return digests[primary] if digests.key? primary

    # grab this
    lengths = URI::NI.lengths

    # just pick the longest one i guess
    digests.slice(*lengths.keys).values.sort do |a, b|
      lengths[b.algorithm] <=> lengths[a.algorithm]
    end.first
  end

  # From a metadata hash, determine if the entry is cache.
  #
  # @param meta [Hash] the metadata hash from the store
  #
  # @return [false, true]
  #
  def cache? meta
    (meta[:flags] & Store::Digest::Entry::IS_CACHE).nonzero?
  end

  # From a metadata hash, determine if the entry should be deleted.
  #
  # @param meta [Hash] the metadata hash from the store
  #
  # @return [false, true]
  #
  def deleted? meta
    return false unless dtime = meta[:dtime]
    cache?(meta) && dtime <= Time.now
  end

  # From an RFC6920 URI, get a raw hash
  #
  # @param uri [URI::NI] a digest URI
  # @param tombstone [false, true] whether to return deleted metadata
  #  records
  # @param remove [false, true, :forget] whether to remove (and
  #  forget) the record
  #
  # @return [Hash] the raw entry data
  #
  def get_raw uri, tombstone: false, remove: false
    uri = coerce_uri uri

    if remove
      # this is how we pun
      mm = remove == :forget ? :remove_meta : :mark_meta_deleted
      bm = :remove_blob
    else
      mm = :get_meta
      bm = :get_blob
    end

    transaction readonly: remove do
      if meta = send(mm, uri)
        if blob = send(bm, meta[:digests][primary].digest)
          meta.merge content: blob
        elsif tombstone
          meta
        end
      end
    end
  end

  # The difference between this and {#add} is that this takes a raw
  # blob, eagerly scans it, and returns a `Hash`, whereas {#add}
  # returns a {Store::Digest::Entry} object which can optionally scan
  # lazily.
  #
  def add_raw content, **params
    # slice out the subset
    params = params.slice :type, :charset, :language, :encoding, :mtime, :cache
    # this will automatically coerce nil to application/octet-stream
    params[:type] = MimeMagic[params[:type]]
    # add a modification time if missing
    mtime = params[:mtime] ||= Time.now

    # managed temporary file handle
    tmp = temp_blob

    # get the basic scannable values (digests, size, type)
    scanned = Entry.scan_raw(
      content, algorithms: algorithms,
      blocksize: blocksize, type: true) { |buf| tmp << buf }

    # remove the scanned type if it is less specific than supplied
    scanned.delete(:type) if params[:type] &&
      !scanned[:type].descendant_of?(params[:type])

    # now merge the scanned params into the supplied ones
    params.merge! scanned

    transaction do

      # warn "asserted: #{params[:type]} -> scanned: #{scanned[:type]} #{scanned[:type].descendant_of?(params[:type])}"

      # warn params.inspect

      # replace the content with the settled blob
      content = settle_blob params[:digests][primary].digest, tmp, mtime: mtime

      # `set_meta` returns nil if unchanged
      meta = set_meta(params) || params

      # warn meta.inspect

      # return the hash with the content
      meta.merge(content: content)
    end
  end

  # okay so:
  #
  # * the scanning nominally comes from the entry (class method)
  #   * hashes
  #   * size (bytes)
  #   * content-type (sampled)
  # * the temp blob comes from the store
  # * so does the settled blob (which could also be the temp blob)
  # * everything else comes from the user (whether from params or entry)
  #
  # issues:
  #
  # * the store doesn't trust the entry to do the scanning so it has
  #   to do its own scan
  #   * (therefore make the actual scanning a class method)
  # * however an entry that has an internal reference to the store
  #   should delegate scanning to it
  #   * the entry could just run `Store#add` that returns a fresh
  #     entry and shuck it for its contents and then throw it away
  #     * although Store::Digest::Entry deliberately obscures its
  #       contents so no that's no good

  # * we don't want a turducken of entry objects; we want the raw file
  #   handle (or rather the lambda that returns a handle) and a wad of
  #   metadata

  # * so i think `#add_raw` is the right idea but the question is what
  #   is its interface
  #   * the blob to be scanned
  #   * all known metadata
  #   * it should return the blob to use (or blob-returning
  #     lambda/closure/whatever) and whatever metadata comes out of
  #     scanning (hashes, size, content type)
  #     * content type and encoding may be different
  #     * ctime and ptime may be different from expected
  #       * dtime may be different
  #     * flags may be different (eg cache flag cleared)
  #   * actually fuck it just give back the equivalent of `Entry#to_h`
  #     (which has a `content:` key)
  #
  def add_raw2
    transaction do
      tmp = tmp_blob

      Entry.scan_raw2(content, tmp, algorithms: algorithms, type: true) do

        content = settle_blob digests[primary].digest, tmp, mtime: mtime

        # `set_meta` returns nil if unchanged
        meta = set_meta(params, preserve: preserve) || params

        hash
      end
    end
  end

  public

  # Initialize a content-addressable store.
  #
  # @note See individual drivers for driver-specific options.
  #
  # @see Store::Digest::Driver::LMDB
  #
  # @param driver [Module, Symbol, #to_sym] the driver to use
  # @param blocksize [Integer] the default block size for scanning blobs
  # @param mtimes [:preserve, :older, :newer] modification time overwrite policy
  #
  # @return [void]
  #
  def initialize driver: Store::Digest::Driver::LMDB,
      blocksize: 2**16, mtimes: :preserve, **options
    driver ||= Store::Digest::Driver::LMDB

    @blocksize = blocksize
    @mtimes = mtimes || :preserve

    unless driver.is_a? Module
      # coerce to symbol
      driver = driver.to_s.to_sym
      raise ArgumentError,
        "There is no storage driver Store::Digest::Driver::#{driver}" unless
        Store::Digest::Driver.const_defined? driver
      driver = Store::Digest::Driver.const_get driver
    end

    raise ArgumentError,
      "Driver #{driver} is not a Store::Digest::Driver" unless
      driver.ancestors.include? Store::Digest::Driver

    # bolt the driver onto the instance
    extend driver

    # aaaand bootstrap it
    setup(**options)
  end

  attr_reader :blocksize, :mtimes

  # XXX this is not right; leave it for now
  # def to_s
  #   '<%s:0x%016x objects=%d deleted=%d bytes=%d>' %
  #     [self.class, self.object_id, objects, deleted, bytes]
  # end

  # alias_method :inspect, :to_s

  # Add an object to the store. Will accept pretty much anything that makes
  # sense to throw at it.
  #
  # @note Already-scanned {Store::Digest::Entry} instances will have
  #  to be rescanned, since the store can't trust the digests. Use
  #  {#add} or {Store::Digest::Entry#add_to} on an unscanned entry to
  #  scan only once.
  #
  # @note `:preserve` will cause a noop if object metadata is identical
  #   save for `:ctime` and `:mtime` (`:ctime` is always ignored).
  #
  # @param obj [IO,File,Pathname,String,Store::Digest::Entry] the object
  # @param type [String] the content type
  # @param charset [String] the character set, if applicable
  # @param language [String] the language, if applicable
  # @param encoding [String] the encoding (eg compression) if applicable
  # @param mtime [Time] the modification time, if not "now"
  # @param cache [false, true, Numeric, Time] whether the object should be
  #  treated as cache, and/or when to evict it
  # @param scan [false, true] eagerly scan the contents
  #
  # @return [Store::Digest::Entry] The (potentially pre-existing) entry
  #
  def add obj, digests: nil, mtime: nil, type: nil, charset: nil,
      encoding: nil, language: nil, cache: false, scan: false

    # XXX this circumvents the integrity check
    return obj.add(self) if obj.is_a? Store::Digest::Entry

    raise ArgumentError, 'entry can\'t be nil' if obj.nil?

    # turducken-ass call graph lol
    Store::Digest::Entry.new obj, store: self, digests: digests, mtime: mtime,
      type: type, charset: charset, encoding: encoding, language: language,
      cache: cache, scan: scan
  end

  # Returns true if the entry is in the store.
  #
  # @param entry [URI::NI, Store::Digest::Entry] the hash address of
  #  an entry, or an entry object itself
  # @param tombstone [false, true] whether to return "tombstone"
  #  metadata records of deleted entries
  #
  # @return [false, true] whether the entry (or its tombstone) is
  #  present in the store
  #
  def has? entry, tombstone: false
    # coerce just because
    tombstone = !!tombstone

    transaction readonly: true do
      # obviously false if there's no record
      break false unless h = get_meta(entry)

      # a metadata record is considered a tombstone if it has a dtime
      # at all if it's an ordinary entry, and in the past if it's cache
      tombstone || !deleted?(h)
    end
  end

  # Retrieve an entry from the store.
  #
  # @note I'm not sure why you would want to `#get` an entry that you
  #  already had, but you can.
  #
  # @param obj [URI::NI, Array<URI::NI>, Hash{Symbol=>URI::NI},
  #  Store::Digest::Entry] some means of resolving an entry
  #
  # @return [Store::Digest::Entry, nil]
  #
  def get obj, tombstone: false
    uri = coerce_uri obj

    if hash = get_raw(uri, tombstone: tombstone)
      Store::Digest::Entry.new(store: self) { hash }
    end
  end

  # Remove an object from the store, optionally "forgetting" it ever existed.
  #
  # @param entry [URI::NI, Store::Digest::Entry] the hash address of
  #  an entry, or an entry object itself
  # @param tombstone [false, true] whether to return "tombstone"
  #  metadata records of deleted entries
  # @param forget [false, true] whether to delete the metadata or just
  #  mark it as deleted
  #
  # @return [Store::Digest::Entry, nil]
  #
  def remove obj, tombstone: false, forget: false
    uri = coerce_uri obj
    rm  = forget ? :forget : true

    if hash = get_raw(uri, tombstone: tombstone, remove: rm)
      Store::Digest::Entry.new { hash }
    end
  end

  # Remove an object from the store and "forget" it ever existed,
  # i.e., purge it from the metadata.
  #
  def forget obj
    remove obj, forget: true
  end

  # Determine if the store is cache-aware.
  #
  # @return [false, true]
  #
  def can_cache?
    respond_to? :cache_ttl
  end

  # Return statistics on the store
  def stats
    Stats.new(**meta_get_stats)
  end

  # This class represents a set of rudimentary statistics for the
  # contents of the store.
  #
  class Stats
    private

    # i dunno do you wanna come up with funny labels? here's where you put em
    LABELS = {
      charsets: "Character sets",
    }.transform_values(&:freeze).freeze

    # lol, petabytes
    MAGNITUDES = %w[B KiB MiB GiB TiB PiB].freeze

    public

    attr_reader :ctime, :mtime, :objects, :deleted, :bytes

    # At this juncture the constructor just puts whatever you throw at
    # it into the object. See
    # {Store::Digest::Meta::LMDB#meta_get_stats} for the real magic.
    # @param options [Hash]
    def initialize **options
      # XXX help i am so lazy
      options.each { |k, v| instance_variable_set "@#{k}", v }
    end

    # Return a human-readable byte size.
    # @return [String] a representation of the byte size of the store.
    def human_size
      # the deci-magnitude also happens to conveniently work as an array index
      mag  = @bytes == 0 ? 0 : (Math.log(@bytes, 2) / 10).floor
      if mag > 0
        '%0.2f %s (%d bytes)' % [(@bytes.to_f / 2**(mag * 10)).round(2),
          MAGNITUDES[mag], @bytes]
      else
        "#{@bytes} bytes"
      end
    end

    def label_struct
      out = {}
      %i[types languages charsets encodings].each do |k|
        stats = instance_variable_get("@#{k}")
        if stats and !stats.empty?
          # XXX note that all these plurals are just inflected with
          # 's' so clipping off the last character is correct
          ks = k.to_s[0, k.to_s.length - 1].to_sym
          x = out[ks] ||= [LABELS.fetch(k, k.capitalize), {}]
          stats.keys.sort.each do |s|
            x.last[s] = stats[s]
          end
        end
      end
      out
    end

    # Return the stats object as a nicely formatted string.
    # @return [String] no joke.
    def to_s

      out = <<-EOT
#{self.class}
  Statistics:
    Created:         #{@ctime}
    Last modified:   #{@mtime}
    Total objects:   #{@objects}
    Deleted records: #{@deleted}
    Repository size: #{human_size}
      EOT

      %i[types languages charsets encodings].each do |k|
        stats = instance_variable_get("@#{k}")
        if stats and !stats.empty?
          out << "  #{LABELS.fetch k, k.capitalize}: #{stats.count}\n"
          stats.keys.sort.each do |s|
            out << "    #{s}: #{stats[s]}\n"
          end
        end
      end

      out
    end
  end

end
