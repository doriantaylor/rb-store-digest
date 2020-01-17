require "store/digest/version"
require 'store/digest/driver'
require 'store/digest/object'

class Store::Digest
  private

  def coerce_object obj
    case obj
    when Store::Digest::Object
      
    when URI::NI
      # just return the uri
      Store::Digest::Object.new digests: obj
    when IO, String
      # assume this is going to be scanned
      Store::Digest::Object.new obj
    else
      raise ArgumentError,
        "Can't coerce a #{obj.class} to Store::Digest::Object"
    end
  end

  public

  # Initialize a storage
  def initialize **options
    driver = options.delete(:driver) || Store::Digest::Driver::LMDB

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

  # Add an object to the store
  def add obj
    transaction do
      obj = coerce_object obj
      tmp = tmpfile
      obj.scan(digests: algorithms) { |buf| tmp << buf }
      set_meta obj

      pri = obj[primary]
      settle_blob pri.digest, tmp, mtime: obj.mtime
    end
  end

  #
  def get uri
    h = get_meta uri
    b = get_blob h[:digest][primary]
    Store::Digest::Object.new b, **h
  end

  #
  def remove obj, forget: false
    # remove blob and mark metadata entry as deleted
    transaction do
      obj  = coerce_object obj
      meta = forget ? remove_meta(obj) : mark_deleted(obj)
      remove_blob meta[:digest][primary]
    end

    obj
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
  end
end
