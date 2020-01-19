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
  PACK   = {
    # control records
    objects:  'Q>',
    deleted:  'Q>',
    bytes:    'Q>',
    # object records
    size:     'Q>',
    ctime:    ?N, # - also used in control
    mtime:    ?N, # - ditto
    ptime:    ?N,
    dtime:    ?N,
    flags:    ?C,
    type:     'Z*',
    language: 'Z*',
    charset:  'Z*',
    encoding: 'Z*',
  }.transform_values(&:freeze).freeze

  def inflate bin, rec
    rec = rec.dup
    digests = algorithms.map do |a|
      uri = URI::NI.build(scheme: 'ni', path: "/#{a}")
      uri.digest = a == primary ? bin : rec.slice!(0, DIGESTS[a])
      [a, uri]
    end.to_h

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

  # NOTE these are all internal methods meant to be used inside other
  # transactions so they do not run in transactions themselves

  def control_add key, val
    if ov = @dbs[:control][key.to_s]
      fmt = case ov.length
            when 4 then ?N
            when 8 then 'Q>'
            else
              raise RuntimeError, "#{key} must be 4 or 8 bytes long"
            end
      ov = ov.unpack1 fmt
    else
      ov = 0
    end

    nv = ov + val

    @dbs[:control][key.to_s] = [nv].pack 'Q>'

    nv
  end

  def control_get key
    key = key.to_sym
    raise ArgumentError, "Invalid control key #{key}" unless
      %[ctime mtime objects deleted bytes].include? key
    if val = @dbs[:control][key.to_s]
      val.unpack1 PACK[key]
    end
  end

  def index_pack key
    case key
    when nil     then return
    when Time    then [key.to_i].pack ?N
    when Integer then [key].pack 'Q>'
    when String  then key.b # no \0: key length is stored in the record
    else raise ArgumentError, "Invalid type #{key.class}"
    end
  end

  def index_add index, key, bin
    key   = index_pack(key) or return
    # check first or it will just stupidly keep adding duplicate records
    @dbs[index].put key, bin unless @dbs[index].has? key, bin
  end

  def index_rm  index, key, bin
    key = index_pack(key) or return
    # soft delete baleets only when there is something to baleet
    @dbs[index.to_sym].delete? key, bin
  end

  # return an enumerator 
  def index_get index, min, max = nil, &block
    # min and max will be binary values and the cursor will return a range
    min = index_pack(min)
    max = index_pack(max)
    return unless min || max

    return enum_for :index_get, index, min, max unless block_given?

    @dbs[index.to_sym].cursor do |c|
      # lmdb cursors are a pain in the ass because 'set' advances the
      # cursor so you can't just run the whole thing in a loop, you
      # have to do this instead:
      if rec = min ? c.set_range(min) : c.first
        break unless max or min == rec.first
        block.call(*rec)
        block.call(*rec) while rec = c.next_range(max || min)
      end
    end
    nil
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

      now = Time.now
      %w[ctime mtime].each do |t|
        unless @dbs[:control].has? t
          @dbs[:control][t] = [now.to_i].pack ?N
        end
      end

      # clever if i do say so myself
      %w[objects deleted bytes].each do |x|
        @dbs[:control][x] = [0].pack 'Q>' unless send(x.to_sym)
      end

      # XXX we might actually wanna dupsort the non-primary digests too
      dbs = RECORD.map do |k|
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

      change = newh[:dtime] ? -1 : 1 # net change in records
      oldrec = @dbs[primary][bin]
      oldh   = nil
      newh   = if oldrec
                 oldh = inflate bin, oldrec
                 oldh.merge(newh) do |k, ov, nv|
                   case k
                   when :ctime then ov
                   when :mtime, :ptime then nv || ov || now
                   when :dtime
                     # net change is zero if both or neither are set
                     change = 0 if (nv && ov) || (!nv && !ov)
                     nv
                   else nv
                   end
                 end
               else
                 %i[ctime mtime ptime].each { |k| newh[k] ||= now }
                 newh
               end
      newrec = deflate newh

      # we have to *break* out of blocks, not return!
      break if newrec == oldrec

      # these only need to be done if they haven't been done before
      (algorithms - [primary]).each do |algo|
        @dbs[algo][obj[algo].digest] = bin
      end unless oldrec

      # this only needs to be done if there are changes
      @dbs[primary][bin] = newrec

      # if old dtime is nil and new dtime is non-nil then we are deleting
      # if old dtime is non-nil and new dtime is nil then we are restoring

      if !oldrec
        # new record: increment object count (by 1), increment byte
        # count (by size)
        control_add :objects, 1
        if change > 0
          control_add :bytes, newh[:size]
        elsif change < 0
          # note objects *and* deleted counts get incremented;
          # allowing for the possibility that a fresh object can be
          # added to the store "deleted".
          control_add :deleted, 1
        end
      elsif change > 0
        # restored record: decrement deleted count (by 1), increment
        # byte count (by size)
        control_add :deleted, -1
        control_add :bytes, newh[:size]
      elsif change < 0
        # "deleted" record: increment deleted count (by 1), decrement
        # byte count (by size)
        control_add :deleted, 1
        control_add :bytes, -newh[:size]
      end
      # otherwise do nothing

      # note that actually *removing* a record is a separate process.

      # okay now we update the indexes
      RECORD.each do |k|
        index_rm  k, oldh[k], bin if oldh and oldh[k] and oldh[k] != newh[k]
        index_add k, newh[k], bin # will noop on nil
      end

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

      # look up the primary digest based on a secondary
      unless algo == primary
        bin = @dbs[algo][bin] or break
      end

      # actually raise maybe? because this should never happen
      rec = @dbs[primary][bin] or break

      # return just a hash of all the elements
      inflate bin, rec
    end
  end

  def remove_meta obj
    @lmdb.transaction do |t|
      hash = get_meta(obj) or break
      bin  = hash[:digests][primary].digest
      RECORD.each { |k| index_rm k, hash[k], bin }
      hash[:digests].each { |algo, uri| @dbs[algo].delete uri.digest }

      # remove counts
      control_add :objects, -1
      if hash[:dtime]
        control_add :deleted, -1
      else
        control_add :bytes, -hash[:size]
        hash[:dtime] = Time.now
      end

      t.commit

      hash
    end
  end

  def mark_meta_deleted obj
    @lmdb.transaction do
      # the object has to be in here to delete it
      oldh = get_meta(obj) or break
      # if the object is already "deleted" we do nothing
      break if oldh[:dtime]

      bin = oldh[:digests][primary].digest
      now = Time.now

      newh = oldh.merge(obj.to_h) do |k, ov, nv|
        case k
        when :digests then ov  # - old values are guaranteed complete
        when :size    then ov  # - we don't trust the new value
        when :type    then ov  # - this gets set by default
        when :dtime   then now # - what we came here to do
        else nv || ov
        end
      end

      @dbs[primary][bin] = deflate(newh)
      control_add :deleted, 1
      control_add :bytes, -newh[:size] 

      # okay now we update the indexes
      RECORD.each do |k|
        index_rm  k, oldh[k], bin if oldh and oldh[k] and oldh[k] != newh[k]
        index_add k, newh[k], bin # will noop on nil
      end

      newh
    end
  end

  def meta_get_stats
    @lmdb.transaction do
      h = %i[ctime mtime objects deleted bytes].map do |k|
        [k, @dbs[:control][k.to_s].unpack1(PACK[k])]
      end.to_h

      # fix the times
      %i[ctime mtime].each { |t| h[t] = Time.at h[t] }

      # get counts on all the countables
      h.merge!(%i[type language charset encoding].map do |d|
        ["#{d}s".to_sym,
          @dbs[d].keys.map { |k| [k, @dbs[d].cardinality(k)] }.to_h]
      end.to_h)

      # would love to do min/max size/dates/etc but that is going to
      # take some lower-level cursor finessing

      h
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
