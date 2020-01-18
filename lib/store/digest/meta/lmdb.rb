require 'store/digest/meta'
require 'store/digest/trait'

require 'lmdb'

module Store::Digest::Meta::LMDB
  include Store::Digest::Meta
  include Store::Digest::Trait::RootDir

  private

  PRIMARY = :"sha-256"
  DIGESTS = {
    md5:       16,
    "sha-1":   20,
    "sha-256": 32,
    "sha-384": 48,
    "sha-512": 64,
  }.freeze

  FORMAT = 'Q>NNNNCZ*Z*Z*Z*'.freeze
  RECORD = %i[
    size ctime mtime ptime dtime flags type language charset encoding].freeze
  INTS   = %i[
    size ctime mtime ptime dtime flags].map { |k| [k, :to_i] }.to_h.freeze

  def inflate bin, rec
    rec = rec.dup
    digests = (algorithms - [primary]).map do |a|
      uri = URI::NI.build(scheme: 'ni', path: "/#{a}")
      uri.digest = rec.slice!(0, DIGESTS[a])
      [a, uri]
    end.to_h

    # don't forget the primary!
    digests[primary] = URI::NI.build(scheme: 'ni', path: "/#{primary}")
    digests[primary].digest = bin

    # size ctime mtime ptime dtime flags type language charset encoding
    hash = RECORD.zip(rec.unpack(FORMAT)).to_h
    hash[:digests] = digests

    %i[ctime ptime mtime dtime].each do |k|
      hash[k] = (hash[k] == 0) ? nil : Time.at(hash[k])
    end

    %i[type language charset encoding].each do |k|
      hash[k] = nil if hash[k].empty?
    end
    hash
  end

  def deflate obj
    obj = obj.to_h unless obj.is_a? Hash
    algos = (algorithms - [primary]).map { |a| obj[:digests][a].digest }.join
    rec   = RECORD.map { |k| v = obj[k]; v.send INTS.fetch(k, :to_s) }
    algos + rec.pack(FORMAT)
  end

  protected

  def setup **options
    # dir/umask
    super

    # now initialize our part
    mapsize = options[:mapsize] || 2**27
    raise ArgumentError, 'Mapsize must be a positive integer' unless
      mapsize.is_a? Integer and mapsize > 0

    lmdbopts = { mode: 0666 & ~umask, mapsize: mapsize }
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

      # XXX we might actually wanna dupsort the non-primary digests
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

  # returns a metadata hash or nil if no changes have been made
  def set_meta obj
    raise ArgumentError,
      'Object does not have a complete set of digests' unless
      (algorithms - obj.algorithms).empty?
    @lmdb.transaction do |t|
      # noop if object is present and not deleted and no details have changed
      bin  = obj[primary].digest
      newh = obj.to_h
      now  = Time.now

      oldrec = @dbs[primary][bin]
      newh   = if oldrec
                 inflate(bin, oldrec).merge(newh) do |k, ov, nv|
                   case k
                   when :ctime then ov
                   when :mtime, :ptime then nv || ov || now
                   else nv
                   end
                 end
               else
                 %i[ctime mtime ptime].each { |k| newh[k] ||= now }
                 newh
               end
      newrec = deflate newh

      # this returns nil
      break if newrec == oldrec

      # these only need to be done if they haven't been done before
      (algorithms - [primary]).each do |algo|
        @dbs[algo][obj[algo].digest] = bin
      end unless oldrec

      # this only needs to be done if there are changes
      @dbs[primary][bin] = newrec

      t.commit

      newh
    end
  end

  def get_meta obj
    @lmdb.transaction do
      # find/inflate master record
      algo = if obj[primary]
               primary
             else
               raise ArgumentError, 'Object must have digests' unless
                 obj.scanned?
               obj.algorithms.sort do |a, b|
                cmp = DIGESTS[b] <=> DIGESTS[a]
                cmp == 0 ? a <=> b : cmp
              end.first
             end
      bin = obj[algo].digest

      unless algo == primary
        bin = @dbs[algo][bin] or return
      end

      # actually raise maybe? because this should never happen
      rec = @dbs[primary][bin] or return

      # return just a hash of all the elements
      inflate bin, rec
    end
  end

  def remove_meta obj
    @lmdb.transaction do
      object_ok? obj

      # delete the object
    end
  end

  def mark_meta_deleted obj
    @lmdb.transaction do
      object_ok? obj
      # find/inflate master record
      # set dtime
      # update master record
    end
  end

  public

  def transaction &block
    @lmdb.transaction(&block)
  end

  # Return the set of algorithms initialized in the database.
  # @return [Array] the algorithms
  def algorithms
    @algorithms ||= @lmdb.transaction do
      ret = @dbs[:control]['algorithms'] or return
      ret.strip.split(/\s*,+\s*/).map(&:to_sym)
    end
  end

  # Return the primary digest algorithm.
  # @return [Symbol] the primary algorithm
  def primary
    @primary ||= @lmdb.transaction do
      ret = @dbs[:control]['primary'] or return
      ret.strip.to_sym
    end
  end

  # Return the number of objects in the database.
  # @return [Integer]
  def objects
    @lmdb.transaction do
      ret = @dbs[:control]['objects'] or return
      ret.unpack1 'Q>' # 64-bit unsigned network-endian integer
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
