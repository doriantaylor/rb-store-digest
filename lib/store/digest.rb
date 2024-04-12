require 'store/digest/version'
require 'store/digest/driver'
require 'store/digest/object'

class Store::Digest
  private

  def coerce_object obj, type: nil, charset: nil,
      language: nil, encoding: nil, mtime: nil, strict: true
    obj = case obj
          when Store::Digest::Object
            obj.dup
          when URI::NI
            # just return the uri
            Store::Digest::Object.new digests: obj,
              type: type, charset: charset, language: language,
              encoding: encoding, mtime: mtime
          when IO, String, StringIO,
              -> x { %i[seek pos read].all? { |m| x.respond_to? m } }
            # assume this is going to be scanned later
            Store::Digest::Object.new obj,
              type: type, charset: charset, language: language,
              encoding: encoding, mtime: mtime
          when Pathname
            # actually open pathnames that are handed directly into S::D
            Store::Digest::Object.new obj.expand_path.open('rb'),
              type: type, charset: charset, language: language,
              encoding: encoding, mtime: mtime
          else
            raise ArgumentError,
              "Can't coerce a #{obj.class} to Store::Digest::Object"
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

  # Add an object to the store. Takes pretty much anything that makes
  # sense to throw at it.
  #
  # @note Prefabricated {Store::Digest::Object} instances will be
  #   rescanned.
  #
  # @note `:preserve` will cause a noop if object metadata is identical
  #   save for `:ctime` and `:mtime` (`:ctime` is always ignored).
  #
  # @param obj [IO,File,Pathname,String,Store::Digest::Object] the object
  # @param type [String] the content type
  # @param charset [String] the character set, if applicable
  # @param language [String] the language, if applicable
  # @param encoding [String] the encoding (eg compression) if applicable
  # @param mtime [Time] the modification time, if not "now"
  # @param strict [true, false] strict checking on metadata input
  # @param preserve [false, true] preserve existing modification time
  #
  # @return [Store::Digest::Object] The (potentially pre-existing) entry
  #
  def add obj, type: nil, charset: nil, language: nil, encoding: nil,
      mtime: nil, strict: true, preserve: false
    return unless obj
    #transaction do # |txn|
      obj = coerce_object obj, type: type, charset: charset,
        language: language, encoding: encoding, mtime: mtime, strict: strict
      raise ArgumentError, 'We need something to store!' unless obj.content?

      # this method is helicoptered in
      tmp = temp_blob

      # XXX this is stupid; figure out a better way to do this

      # get our digests
      obj.scan(digests: algorithms, blocksize: 2**20, strict: strict,
        type: type, charset: charset, language: language,
        encoding: encoding, mtime: mtime) do |buf|
        tmp << buf
      end

      # if we are scanning an object it is necessarily not deleted
      obj.dtime = nil

      # set_meta will return nil if there is no difference in what is set
      if h = set_meta(obj, preserve: preserve)
        # replace the object

        content = obj.content

        # do this to prevent too many open files
        if content.is_a? File
          path = Pathname(content.path).expand_path
          content = -> { path.open('rb') }
        end

        obj = Store::Digest::Object.new content, fresh: true, **h

        # now settle the blob into storage
        settle_blob obj[primary].digest, tmp, mtime: obj.mtime
        #txn.commit
      else
        tmp.close
        tmp.unlink

        # eh just do this
        obj = get obj
        obj.fresh? false # object is not fresh since we already have it
      end

      obj
    #end
  end

  # Retrieve an object from the store.
  # @param obj [URI]
  def get obj
    body = -> do
      obj = coerce_object obj
      h = get_meta(obj) or return # bail if this does not exist
      b = get_blob h[:digests][primary].digest # may be nil
      Store::Digest::Object.new b, **h
    end
    transaction(&body)
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

    # remove blob and mark metadata entry as deleted
    meta = nil
    transaction do
      meta = forget ? remove_meta(obj) : mark_meta_deleted(obj)
    end

    if meta
      if blob = remove_blob(meta[:digests][primary].digest)
        return Store::Digest::Object.new blob, **meta
      end
    end
    nil
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
