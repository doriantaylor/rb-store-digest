# grab the symbols so we don't have to indent
require_relative '../version'

# This is an abstract module for metadata operations. All required
# methods are defined, and raise {NotImplementedError}.
module Store::Digest::Meta

  private

  PRIMARY = :"sha-256"

  DIGESTS = {
    md5:       16,
    "sha-1":   20,
    "sha-256": 32,
    "sha-384": 48,
    "sha-512": 64,
  }.freeze

  INTS   = %i[size ctime mtime ptime dtime flags].map do |k|
    [k, :to_i]
  end.to_h.freeze

  protected

  # This method is run on initialization to bootstrap or otherwise
  # verify the integrity of the database.
  #
  # @param options [Hash] whatever the parameters entail; it's your
  #  driver lol
  #
  # @return [void]
  #
  def setup **options
    raise NotImplementedError
  end

  # Set/add an individual object's metadata to the database.
  #
  # @param obj [Store::Digest::Object] the object to store
  # @param preserve [false, true] flag to preserve modification time
  #
  # @return [void]
  #
  def set_meta obj, preserve: false
    raise NotImplementedError
  end

  # Retrieve a hash of the metadata stored for the given object.
  #
  # @param obj [#to_h, URI::ni] something hash-like which has keys that
  #  correspond to the identifiers for the digest algorithms, or
  #  otherwise a `ni:` URI.
  #
  # @return [Hash]
  #
  def get_meta obj, preserve: false
    raise NotImplementedError
  end

  # Remove the metadata from the database and return it.
  #
  # @see #get_meta
  #
  # @param obj [#to_h, URI::ni] The object/identifier
  #
  # @return [Hash] The eliminated metadata.
  #
  def remove_meta obj, preserve: false
    raise NotImplementedError
  end

  # Mark the object's record as "deleted" (but do not actually delete
  # it) and return the updated record.
  #
  # @see #get_meta
  #
  # @param obj [#to_h, URI::ni] The object/identifier
  #
  # @return [Hash] The updated metadata.
  #
  # @return [Hash]
  #
  def mark_meta_deleted obj
    raise NotImplementedError
  end

  # Retrieve storage statistics from the database itself.
  #
  # @return [Hash] global stats for the database.
  #
  def meta_get_stats
    raise NotImplementedError
  end

  public

  # Wrap the block in a transaction.
  #
  # @param block [Proc] whatever you pass into the transaction.
  #
  # @return [Object] whatever the block returns.
  #
  def transaction &block
    raise NotImplementedError
  end

  # Return the set of algorithms initialized in the database.
  #
  # @return [Array] the algorithms
  #
  def algorithms
    raise NotImplementedError
  end

  # Return the primary digest algorithm.
  #
  # @return [Symbol] the primary algorithm
  #
  def primary
    raise NotImplementedError
  end

  # Return the number of objects in the database.
  #
  # @return [Integer]
  #
  def objects
    raise NotImplementedError
  end

  # Return the number of objects whose payloads are deleted but are
  # still on record.
  #
  # @return [Integer]
  #
  def deleted
    raise NotImplementedError
  end

  # Return the number of bytes stored in the database (notwithstanding
  # the database itself).
  #
  # @return [Integer]
  #
  def bytes
    raise NotImplementedError
  end

  # Return a list of objects matching the given criteria. The result
  # set will be the intersection of all supplied parameters. `:type`,
  # `:charset`, `:encoding`, and `:language` are treated like discrete
  # sets, while the rest of the parameters are treated like ranges
  # (two-element arrays). Single values will be coerced into arrays;
  # single range values will be interpreted as an inclusive lower
  # bound. To bound only at the top, use a two-element array with its
  # first value `nil`, like so: `size: [nil, 31337]`. The sorting
  # criteria are the symbols of the other parameters.
  #
  # @param type [nil, String, #to_a]
  # @param charset [nil, String, #to_a]
  # @param encoding [nil, String, #to_a]
  # @param language [nil, String, #to_a]
  # @param size [nil, Integer, #to_a] byte size range
  # @param ctime [nil, Time, DateTime, #to_a] creation time range
  # @param mtime [nil, Time, DateTime, #to_a] modification time range
  # @param ptime [nil, Time, DateTime, #to_a] medatata property change range
  # @param dtime [nil, Time, DateTime, #to_a] deletion time range
  # @param sort [nil, Symbol, #to_a] sorting criteria
  #
  # @return [Array] the list
  #
  def list type: nil, charset: nil, encoding: nil, language: nil,
      size: nil, ctime: nil, mtime: nil, ptime: nil, dtime: nil, sort: nil
    raise NotImplementedError
  end

  class CorruptStateError < RuntimeError
  end
end
