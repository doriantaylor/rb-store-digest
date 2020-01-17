require 'store/digest/meta'
require 'store/digest/trait'

require 'lmdb'

module Store::Digest::Meta::LMDB
  include Store::Digest::Meta
  include Store::Digest::Trait::RootDir

  private

  DIGESTS = {
    md5:       16,
    "sha-1":   20,
    "sha-256": 32,
    "sha-384": 48,
    "sha-512": 64,
  }.freeze

  PRIMARY = :"sha-256"

  protected

  def setup **options
    # dir/umask
    super

    # now initialize our part
    mapsize = options[:mapsize] || 2**27
    raise ArgumentError, 'Mapsize must be a positive integer' unless
      mapsize.is_a? Integer and mapsize > 0

    lmdbopts = { mode: 0666 &  ~umask, mapsize: mapsize }
    @lmdb = ::LMDB.new dir, lmdbopts

    algos = options[:algorithms] || DIGESTS.keys
    raise ArgumentError, "Invalid algorithm specification #{algos}" unless
      algos.is_a? Array and (algos - DIGESTS.keys).empty?

    popt = options[:primary] || PRIMARY
    raise ArgumentError, "Invalid primary algorithm #{popt}" unless
      popt.is_a? Symbol and DIGESTS[popt]

    @lmdb.transaction do
      @dbs = { control: @lmdb.database('control', create: true) }

      if a = algorithms
        raise ArgumentError,
          "Supplied algorithms #{algos.sort} do not match instantiated #{a}" if
          algos.sort != a
      else
        a = algos.sort
        @dbs[:control]['algorithms'] = a.join ?,
      end

      if pri = primary
        raise ArgumentError,
          "Supplied algorithm #{popt} does not match instantiated #{pri}" if
          popt != pri
      else
        pri = popt
        @dbs[:control]['primary'] = popt.to_s
      end

      # clever if i do say so myself
      %w[objects deleted bytes].each do |x|
        @dbs[:control][x] = [0].pack 'Q>' unless send(x.to_sym)
      end

      # XXX we might actually wanna dupsort 
      dbs = %i[type charset language encoding].map do |k|
        [k, [:dupsort]]
      end.to_h.merge(a.map { |k| [k, []] }.to_h)
      
      @dbs.merge!(dbs.map do |name, flags|
        [name, @lmdb.database(name.to_s,
          (flags + [:create]).map { |f| [f, true] }.to_h)]
      end.to_h).freeze
    end

    @lmdb.sync
  end

  public

  # Return the set of algorithms initialized in the database.
  # @return [Array] the algorithms
  def algorithms
    @lmdb.transaction do
      ret = @dbs[:control]['algorithms'] or return
      ret.strip.split(/\s*,+\s*/).map(&:to_sym)
    end
  end

  # Return the primary digest algorithm.
  # @return [Symbol] the primary algorithm
  def primary
    @lmdb.transaction do
      ret = @dbs[:control]['primary'] or return
      ret.strip.to_sym
    end
  end

  # Return the number of objects in the database.
  # @return [Integer]
  def objects
    @lmdb.transaction do
      ret = @dbs[:control]['objects'] or return
      ret.unpack1 'Q>'
    end
  end

  # Return the number of objects whose payloads are deleted but are
  # still on record.
  # @return [Integer]
  def deleted
    @lmdb.transaction do
      ret = @dbs[:control]['deleted'] or return
      ret.unpack1 'Q>'
    end
  end

  # Return the number of bytes stored in the database (notwithstanding
  # the database itself).
  # @return [Integer]
  def bytes
    @lmdb.transaction do
      ret = @dbs[:control]['bytes'] or return
      ret.unpack1 'Q>'
    end
  end
end
