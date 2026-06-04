require 'store/digest/version'
require 'store/digest/driver'
require 'store/digest/entry'

# This is a general-purpose content-addressable store that interfaces
# via RFC6920 addresses.
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

  public

  # Initialize a storage
  def initialize **options
    driver = options.delete(:driver) || Store::Digest::Driver::LMDB

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

    extend driver

    #
    setup(**options)
  end

  # XXX this is not right; leave it for now
  # def to_s
  #   '<%s:0x%016x objects=%d deleted=%d bytes=%d>' %
  #     [self.class, self.object_id, objects, deleted, bytes]
  # end

  # alias_method :inspect, :to_s

  # The difference between this and {#add} is that this takes a raw
  # blob, eagerly scans it, and returns a `Hash`, whereas {#add}
  # returns a {Store::Digest::Entry} object which can optionally scan
  # lazily.
  #
  def add_raw content, blocksize: nil, preserve: false, **params
    # slice out the subset
    params.slice! :type, :charset, :language, :encoding, :mtime, :cache
    # this will automatically coerce
    params[:type] = MimeMagic[params[:type]]
    # add a modification time if missing
    params[:mtime] ||= Time.now

    transaction do
      # temporary file handle
      tmp = temp_blob

      # get the basic scannable values
      digests, size, stype = Entry.scan_raw(
        content, algorithms: algorithms, type: true) { |buf| tmp << buf }

      # always add these
      params[:digests] = digests

      # the size is authoritative
      params[:size] = size

      if params[:type]
        # only overwrite the type if it's a descendant of the supplied
        params[:type] = stype if stype.descendant_of? params[:type]
      else
        # otherwise add unconditionally
        params[:type] = stype
      end

      # replace the content with the settled blob
      content = settle_blob digests[primary].digest, tmp, mtime: mtime

      # `set_meta` returns nil if unchanged
      meta = set_meta(params, preserve: preserve) || params

      # return the ensemble
      [content, meta]
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
  #
  def add_raw2
    transaction do
      tmp = tmp_blob

      Entry.scan_raw2(content, tmp, algorithms: algorithms, type: true) do

        content = settle_blob digests[primary].digest, tmp, mtime: mtime

        # `set_meta` returns nil if unchanged
        meta = set_meta(params, preserve: preserve) || params

        [content, meta]
      end
    end
  end

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
  # @param strict [true, false] strict checking on metadata input
  # @param preserve [false, true] preserve existing modification time
  # @param cache [false, true, Numeric, Time] whether the object should be
  #  treated as cache, and/or when to evict it
  #
  # @return [Store::Digest::Entry] The (potentially pre-existing) entry
  #
  def add obj, type: nil, charset: nil, language: nil, encoding: nil,
      mtime: nil, strict: true, preserve: false, cache: nil
    return unless obj

    transaction do # |txn|
      obj = coerce_object obj, type: type, charset: charset, language: language,
        encoding: encoding, mtime: mtime, strict: strict
      raise ArgumentError, 'We need something to store!' unless obj.content?

      # this method is helicoptered in
      tmp = temp_blob

      # XXX this is stupid; figure out a better way to do this

      # get our digests
      obj.scan(digests: algorithms, blocksize: 2**16, strict: strict,
        type: type, charset: charset, language: language,
        encoding: encoding, mtime: mtime) do |buf|
        tmp << buf
      end

      # if we are scanning an object it is necessarily not deleted
      obj.dtime = nil

      # set_meta will return nil if there is no difference in what is set
      if h = set_meta(obj, preserve: preserve)
        # warn h.inspect
        # replace the object

        content = obj.content

        # do this to prevent too many open files
        if content.is_a? File
          path = Pathname(content.path).expand_path
          content = -> { path.open('rb') }
        end

        obj = Store::Digest::Entry.new content, fresh: true, **h

        # now settle the blob into storage
        settle_blob obj[primary].digest, tmp, mtime: obj.mtime
      else
        tmp.close
        tmp.unlink

        # warn "got here lolol"

        # eh just do this
        obj = get obj
        obj.fresh = false # object is not fresh since we already have it
      end

      obj
    end
  end

  # Retrieve an object from the store.
  #
  # @param obj [URI, Store::Digest::Entry]
  #
  # @return [Store::Digest::Entry, nil]
  def get obj
    transaction readonly: true do
      obj = coerce_object obj
      if h = get_meta(obj) # bail if this does not exist
        b = get_blob h[:digests][primary].digest # may be nil
        Store::Digest::Entry.new b, **h
      end
    end
  end

  # Remove an object from the store, optionally "forgetting" it ever existed.
  # @param obj
  def remove obj, forget: false
    obj  = coerce_object obj
    unless obj.scanned?
      raise ArgumentError,
        'Cannot scan object because there is no content' unless obj.content?
      obj.scan digests: algorithms, blocksize: 2**20
    end

    # remove or mark metadata entry as deleted and remove blob
    transaction do
      if meta = forget ? remove_meta(obj) : mark_meta_deleted(obj)
        if blob = remove_blob(meta[:digests][primary].digest)
          Store::Digest::Entry.new blob, **meta
        end
      end
    end
  end

  # Remove an object from the store and "forget" it ever existed,
  # i.e., purge it from the metadata.
  #
  def forget obj
    remove obj, forget: true
  end

  # Return statistics on the store
  def stats
    Stats.new(**meta_get_stats)
  end

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
