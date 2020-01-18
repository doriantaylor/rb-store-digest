require "store/digest/version"
require 'store/digest/driver'
require 'store/digest/object'

class Store::Digest
  private

  def coerce_object obj
    case obj
    when Store::Digest::Object
      obj
    when URI::NI
      # just return the uri
      Store::Digest::Object.new digests: obj
    when IO, String, StringIO
      # assume this is going to be scanned later
      Store::Digest::Object.new obj
    when Pathname
      # actually open pathnames that are handed directly into S::D
      Store::Digest::Object.new obj.expand_path.open('rb')
    else
      raise ArgumentError,
        "Can't coerce a #{obj.class} to Store::Digest::Object"
    end
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

  # Add an object to the store.
  # @note Prefabricated {Store::Digest::Object} instances will be rescanned.
  # @param obj [IO,File,Pathname,String,Store::Digest::Object] the object
  # @return [Store::Digest::Object] The (potentially pre-existing) entry
  def add obj
    transaction do
      obj = coerce_object obj
      raise ArgumentError, 'We need something to store!' unless obj.content?

      tmp = temp_blob

      # get our digests
      obj.scan(digests: algorithms, blocksize: 2**20) { |buf| tmp << buf }
      obj.dtime = nil
      if h = set_meta(obj)
        # replace the object

        content = obj.content

        # do this to prevent too many open files
        if content.is_a? File
          path = Pathname(content.path).expand_path
          content = -> { path.open('rb') }
        end

        obj = Store::Digest::Object.new content, **h

        # now settle the blob into storage
        settle_blob obj[primary].digest, tmp, mtime: obj.mtime
      else
        tmp.close
        tmp.unlink

        # eh just do this
        obj = get obj
      end

      obj
    end
  end

  # Retrieve an object from the store.
  # @param 
  def get obj
    transaction do
      obj = coerce_object obj
      h = get_meta(obj) or break # bail if this does not exist
      b = get_blob h[:digests][primary].digest # may be nil
      Store::Digest::Object.new b, **h
    end
  end

  # Remove an object from the store, optionally "forgetting" it ever existed.
  # @param obj
  def remove obj, forget: false
    obj  = coerce_object obj
    unless obj.scanned?
      raise ArgumentError,
        'Cannot scan object because there is no content' unless obj.content?
      obj.scan digests: digests, blocksize: 2**20
    end

    # remove blob and mark metadata entry as deleted
    meta = nil
    transaction do
      meta = forget ? remove_meta(obj) : mark_deleted(obj)
    end

    if meta
      if blob = remove_blob(meta[:digest][primary])
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
  end

  class Stats
    def to_s
    end
  end
end
