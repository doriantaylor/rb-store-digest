require 'store/digest/meta'
require 'store/digest/trait'

require 'lmdb'
require 'uri/ni'

# Symas LMDB Metadata driver.
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

  # NOTE these are all internal methods meant to be used inside other
  # transactions so they do not run in transactions themselves


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
      # load up the control database
      @dbs = { control: @lmdb.database('control', create: true) }

      # if control is empty or version is 1, extend V1
      if @dbs[:control].empty?
        # set to v1 for next time
        @dbs[:control]['version'] = ?1
        extend V1
      elsif @dbs[:control]['version'] == ?1
        extend V1
      elsif @dbs[:control]['version'].nil?
        # if version is empty, extend v0
        extend V0
      else
        # otherwise error
        v = @dbs[:control]['version']
        raise CorruptStateError,
          "Control database has unrecognized version #{v}"
      end

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

      setup_dbs
    end

    @lmdb.sync
  end

  public

  # Wrap the block in a transaction. Trying to start a read-write
  # transaction (or do a write operation, as they are wrapped by
  # transactions internally) within a read-only transaction will
  # almost certainly break.
  #
  # @param readonly [false, true] whether the transaction is read-only
  # @param block [Proc] the code to run.
  #
  def transaction readonly: false, &block
    @lmdb.transaction(readonly) do
      # we do not want to transmit
      block.call
    end
  end

  # Return the set of algorithms initialized in the database.
  # @return [Array] the algorithms
  def algorithms
    @algorithms ||= @lmdb.transaction do
      if ret = @dbs[:control]['algorithms']
        ret.strip.downcase.split(/\s*,+\s*/).map(&:to_sym)
      end
    end
  end

  # Return the primary digest algorithm.
  # @return [Symbol] the primary algorithm
  def primary
    @primary ||= @lmdb.transaction do
      if ret = @dbs[:control]['primary']
        ret.strip.downcase.to_sym
      end
    end
  end

  # Return the number of objects in the database.
  # @return [Integer]
  def objects
    @lmdb.transaction do
      if ret = @dbs[:control]['objects']
        ret.unpack1 'Q>' # 64-bit unsigned network-endian integer
      end
    end
  end

  # Return the number of objects whose payloads are deleted but are
  # still on record.
  # @return [Integer]
  def deleted
    @lmdb.transaction do
      if ret = @dbs[:control]['deleted']
        ret.unpack1 'Q>'
      end
    end
  end

  # Return the number of bytes stored in the database (notwithstanding
  # the database itself).
  # @return [Integer]
  def bytes
    @lmdb.transaction do
      if ret = @dbs[:control]['bytes']
        ret.unpack1 'Q>'
      end
    end
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
  # @return [Array] the list

  PARAMS = %i[type charset encoding language
              size ctime mtime ptime dtime].freeze

  def list type: nil, charset: nil, encoding: nil, language: nil,
      size: nil, ctime: nil, mtime: nil, ptime: nil, dtime: nil, sort: nil
    # coerce all the inputs
    params = begin
               b  = binding
               ph = {}
               PARAMS.each do |key|
                 val = b.local_variable_get key
                 val = case val
                       when nil then []
                       when Time then [val]
                       when DateTime then [val.to_time]
                       when -> (v) { v.respond_to? :to_a } then val.to_a
                       else [val]
                       end
                 ph[key] = val unless val.empty?
               end
               ph
             end
    # find the smallest denominator
    index = params.keys.map do |k|
      [k, @dbs[k].size]
    end.sort { |a, b| a[1] <=> b[1] }.map(&:first).first
    out = {}
    @lmdb.transaction do
      if index
        # warn params.inspect
        if INTS[index]
          index_get index, *params[index], range: true do |_, v|
            u = URI("ni:///#{primary};")
            u.digest = v
            out[u] ||= get u
          end
        else
          params[index].each do |val|
            index_get index, val do |_, v|
              u = URI("ni:///#{primary};")
              u.digest = v
              out[u] ||= get u
            end
          end
        end
        rest = params.keys - [index]
        unless rest.empty?
          out.select! do |_, obj|
            rest.map do |param|
              if val = obj.send(param)
                # warn "#{param} #{params[param]} <=> #{val}"
                if INTS[param]
                  min, max = params[param]
                  if min && max
                    val >= min && val <= max
                  elsif min
                    val >= min
                  elsif max
                    val <= max
                  end
                else
                  params[param].include? val
                end
              else
                false
              end
            end.all?(true)
          end
        end
      else
        # if we aren't filtering at all we can just obtain everything
        @dbs[primary].cursor do |c|
          while rec = c.next
            u = URI("ni:///#{primary};")
            u.digest = rec.first
            out[u] ||= get u
          end
        end
      end
    end

    # now we sort
    out.values
  end

  # This is the version zero (original) database layout.
  module V0

    private

    def setup_dbs

      now = Time.now in: ?Z
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
                    [name, @lmdb.database(
                      name.to_s, (flags + [:create]).map { |f| [f, true] }.to_h
                    )]
                  end.to_h).freeze
    end

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
    def index_get index, min, max = nil, range: false, &block
      # min and max will be binary values and the cursor will return a range
      min = index_pack(min)
      max = index_pack(max)
      return unless min || max

      return enum_for :index_get, index, min, max unless block_given?

      body = -> c do
        # lmdb cursors are a pain in the ass because 'set' advances the
        # cursor so you can't just run the whole thing in a loop, you
        # have to do this instead:
        if rec = (min ? c.set_range(min) : c.first)
          return unless range or max or min == rec.first
          block.call(*rec)
          block.call(*rec) while rec = c.next_range(max || min)
        end
      end

      @dbs[index.to_sym].cursor(&body)
      nil
    end

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

    protected

    # Returns a metadata hash or `nil` if no changes have been made. A
    # common scenario is that the caller will attempt to store an object
    # that is already present, with the only distinction being `:ctime`
    # (which is always ignored) and/or `:mtime`. Setting the `:preserve`
    # keyword parameter to a true value will cause any new value for
    # `:mtime` to be ignored as well. In that case, an attempt to store
    # an otherwise identical record overtop of an existing one will
    # return `nil`.
    #
    # @param obj [Store::Digest::Object] the object to store
    # @param preserve [false, true] whether to preserve the mtime
    # @return [nil, Hash] maybe the metadata content of the object
    #
    def set_meta obj, preserve: false
      raise ArgumentError,
        'Object does not have a complete set of digests' unless
        (algorithms - obj.algorithms).empty?

      body = -> do
        # noop if object is present and not deleted and no details have changed
        bin  = obj[primary].digest
        newh = obj.to_h
        now  = Time.now in: ?Z

        change = newh[:dtime] ? -1 : 1 # net change in records
        oldrec = @dbs[primary][bin]
        oldh   = nil
        newh   = if oldrec
                   oldh = inflate bin, oldrec
                   oldh.merge(newh) do |k, ov, nv|
                     case k
                     when :ctime then ov # never overwrite ctime
                     when :mtime # only overwrite the mtime if specified
                       preserve ? (ov || nv || now) : (nv || ov || now)
                     when :ptime then nv || ov || now # XXX derive ptime?
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
        # (ah but we can return from a lambda)
        return if newrec == oldrec
        # anyway a common scenario is a write where nothing is different
        # but the mtime, so thepurpose

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

        # note that actually *removing* a record is  separate process.

        # okay now we update the indexes
        RECORD.each do |k|
          index_rm  k, oldh[k], bin if oldh and oldh[k] and oldh[k] != newh[k]
          index_add k, newh[k], bin # will noop on nil
        end

        # and finally update the mtime
        @dbs[:control]['mtime'] = [now.to_i].pack ?N

        newh
      end

      @lmdb.transaction do
        body.call
      end
    end

    def get_meta obj
      body = -> do
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
          bin = @dbs[algo][bin] or return
        end

        # actually raise maybe? because this should never happen
        rec = @dbs[primary][bin] or return

        # return just a hash of all the elements
        inflate bin, rec
      end

      @lmdb.transaction do
        body.call
      end
    end

    def remove_meta obj
      body = -> do
        hash = get_meta(obj) or return
        bin  = hash[:digests][primary].digest
        now  = Time.now in: ?Z

        RECORD.each { |k| index_rm k, hash[k], bin }
        hash[:digests].each { |algo, uri| @dbs[algo].delete uri.digest }

        # remove counts
        control_add :objects, -1
        if hash[:dtime]
          control_add :deleted, -1
        else
          control_add :bytes, -hash[:size]
          hash[:dtime] = now
        end

        # and finally update the mtime
        @dbs[:control]['mtime'] = [now.to_i].pack ?N

        hash
      end

      @lmdb.transaction do
        body.call
      end
    end

    def mark_meta_deleted obj
      body = -> do
        # the object has to be in here to delete it
        oldh = get_meta(obj) or return
        # if the object is already "deleted" we do nothing
        return if oldh[:dtime]

        bin = oldh[:digests][primary].digest
        now = Time.now in: ?Z

        newh = oldh.merge(obj.to_h) do |k, ov, nv|
          case k
          when :digests then ov  # - old values are guaranteed complete
          when :size    then ov  # - we don't trust the new value
          when :type    then ov  # - this gets set by default
          when :dtime   then now # - what we came here to do
          else nv || ov
          end
        end

        @dbs[primary][bin] = deflate newh
        control_add :deleted, 1
        control_add :bytes, -newh[:size]

        # okay now we update the indexes
        RECORD.each do |k|
          index_rm  k, oldh[k], bin if oldh and oldh[k] and oldh[k] != newh[k]
          index_add k, newh[k], bin # will noop on nil
        end

        # and finally update the mtime
        @dbs[:control]['mtime'] = [now.to_i].pack ?N

        newh
      end

      @lmdb.transaction do
        body.call
      end
    end

  end

  # This is the version 1 database layout.
  module V1

    private

    # import the flags
    Flags = Store::Digest::Object::Flags

    # XXX do we want to introduce dry-types? didn't i try before and
    # it was a huge clusterfuck?

    # i think?? are there others?? lol
    ARCH = [''].pack(?p).size == 8 ? 64 : 32
    LONG = ARCH == 64 ? ?Q : ?L

    ENCODE_NOOP   = -> x { x }
    DECODE_NOOP   = ENCODE_NOOP
    ENCODE_TOKEN  = -> x { x.to_s }
    DECODE_TOKEN  = -> x { x.empty? ? nil : x }
    ENCODE_FLAGS  = -> x { Flags.to_i x }
    DECODE_FLAGS  = -> x { Flags.from x }
    if ARCH == 64
      # you get microsecond resolution
      ENCODE_TIME = -> x { x ? x.to_i * 1_000_000 + x.usec : 0 }
      DECODE_TIME = -> x {
        x == 0 ? nil : Time.at(x / 1_000_000, x % 1_000_000, :usec, in: ?Z)
      }
    else
      # and you do not
      ENCODE_TIME = -> x { x ? x.to_i : 0 }
      DECODE_TIME = -> x { x == 0 ? nil : Time.at(x, in: ?Z) }
    end

    # { Class => [pack, encode, decode] }
    COERCE = {
      Integer => [LONG, ENCODE_NOOP,  DECODE_NOOP ],
      String  => ['Z*', ENCODE_TOKEN, DECODE_TOKEN],
      Time    => [LONG, ENCODE_TIME,  DECODE_TIME ],
      Flags   => [?S,   ENCODE_FLAGS, DECODE_FLAGS],
    }

    # one difference between V0 records and V1 records is we don't
    # force network-endianness, since we can't force it for the
    # integer keys. the other difference is that the flags are now an
    # unsigned short.

    # control records
    CONTROL = {
      version:  String,
      ctime:    Time,
      mtime:    Time,
      expiry:   Integer,
      objects:  Integer,
      deleted:  Integer,
      bytes:    Integer,
    }

    # object records
    RECORD = {
      size:     Integer,
      ctime:    Time,
      mtime:    Time,
      ptime:    Time,
      dtime:    Time,
      flags:    Flags,
      type:     String,
      language: String,
      charset:  String,
      encoding: String,
    }

    # the record string (after the hashes are removed)
    PACKED = RECORD.values.map { |v| COERCE[v].first }.join

    # Set up the V1 database layout.
    #
    # @return [void]
    #
    def setup_dbs
      # in the v1 layout, `primary` is only cosmetic and we have an
      # `entry` database keyed by (native-endian) integer

      now = Time.now in: ?Z

      %i[ctime mtime].each { |k| control_set k, now, maybe: true }

      # clever if i do say so myself
      %i[objects deleted bytes].each { |k| control_set k, 0, maybe: true }

      # default cache expiration
      control_set :expiry, 86400, maybe: true

      # this snarl takes the record layout (popping in a cheeky
      # "etime" index for cache entry expirations) and pairs it with
      # hash algorithm indices to attach them to database flags, which
      # are then shoveled en masse into the LMDB factory method.
      dbs = RECORD.except(:flags).merge({ etime: Time }).transform_values do |type|
        flags = %i[dupsort]
        flags += [Integer, Time].include?(type) ? %i[integerkey integerdup] : []
      end.merge(
        # these are always going to be a fixed length (hash -> size_t)
        algorithms.map { |k| [k, %i[dupsort]] }.to_h, # dupfixed bad?
        { entry: [:integerkey] }
      ).transform_values do |flags|
        (flags + [:create]).map { |flag| [flag, true] }.to_h
      end

      @dbs.merge!(dbs.map { |n, f| [n, @lmdb.database(n.to_s, f)] }.to_h)
    end

    # Encode an individual value.
    #
    # @param value [Object] the value to be encoded
    # @param type [Class] the value's type if not specified
    #
    # @return [String] the raw value for the database
    #
    def db_encode value, type = value.class
      pack, encode, _ = COERCE[type]
      raise ArgumentError, "Unsupported type #{type}" unless pack

      [encode.call(value)].pack pack
    end

    # Decode an individual value.
    #
    # @param raw [String] a raw value from the database
    # @param type [Class] the type to decode it into
    #
    # @return [Object] whatever `type` object was intended
    #
    def db_decode raw, type
      pack, _, decode = COERCE[type]
      raise ArgumentError, "Unsupported type #{type}" unless pack

      decode.call raw.unpack1(pack)
    end

    # Get the "last" (highest-ordinal) key of an integer-keyed database.
    #
    # @param db [LMDB::Database,Symbol]
    # @param raw [false, true] whether to decode the pointer
    #
    # @return [Integer]
    #
    def last_key db, raw: false
      db = @dbs[db] if db.is_a? Symbol
      raise ArgumentError, 'Wrong/malformed database' unless
        db.is_a? ::LMDB::Database and db.flags[:integerkey]

      # the last entry in the database should be the highest number,
      # but also not sure if we want to reserve zero
      out = db.empty? ? 0 : (db.cursor { |c| c.last }.first.unpack1(?J) + 1)

      # return raw pointer
      raw ? [out].pack(?J) : out
    end

    # Retrieve the value of a control field.
    #
    # @param key [Symbol]
    #
    # @return [Object, nil] the value of the key
    #
    def control_get key
      type = CONTROL[key.to_sym] or raise ArgumentError,
        "invalid control key #{key}"

      raw = @dbs[:control][key.to_s]
      db_decode raw, type if raw
    end

    # Set a control field with an explicit value.
    #
    # @param key [Symbol]
    # @param value [Object]
    # @param maybe [false, true] only set if uninitialized
    #
    # @return [Object] the original value passed through
    #
    def control_set key, value, maybe: false
      type = CONTROL[key] or raise ArgumentError, "invalid control key #{key}"
      raise ArgumentError,
        "value should be instance of #{type}" unless value.is_a? type

      @dbs[:control][key.to_s] = db_encode value, type unless
        maybe && @dbs[:control].has?(key.to_s)
    end

    # Increment an existing ({Integer}) control field by a value.
    #
    # @param key [Symbol]
    # @param value [Numeric]
    #
    # @raise [RuntimeError] if the field is uninitialized
    #
    # @return [Integer, Time] the new value
    #
    def control_add key, value
      raise "value must be numeric" unless value.is_a? Numeric
      type = CONTROL[key] or raise ArgumentError, "invalid control key #{key}"

      # value may be uninitialized
      raise "Attempted to change an uninitialized value" unless
        old = control_get(key)

      # early bailout
      return value if value == 0

      # overwrite the value
      control_set key, old + value
    end

    # Add an entry to an index.
    #
    # @note The indexes point to the integer keys in v1 rather than hashes in v0
    #
    # @param index [Symbol] the index table name
    # @param key [Object] the datum to become the index key
    # @param ptr [Integer] the key for the entry
    #
    # @return [void]
    #
    def index_add index, key, ptr
      # XXX just add etime here for now
      cls = RECORD.merge({etime: Time})[index] or raise ArgumentError,
        "No record for #{index}"

      # warn "#{index}, #{key.inspect}"

      key = db_encode key, cls
      ptr = ptr.is_a?(String) ? ptr : [ptr].pack(?J)


      @dbs[index.to_sym].put? key, ptr
    end

    # Remove an entry from an index.
    #
    # @param index [Symbol] the index table name
    # @param key [Object] the datum to become the index key
    # @param ptr [Integer] the key for the entry
    #
    # @return [void]
    #
    def index_rm index, key, ptr
      # XXX etime lol
      cls = RECORD.merge({etime: Time})[index] or raise ArgumentError,
        "No record for #{index}"
      key = db_encode key, cls
      ptr = ptr.is_a?(String) ? ptr : [ptr].pack(?J)

      @dbs[index.to_sym].delete? key, ptr
    end

    # the v1 record is substantively different from v0; also all the
    # hashes are in the v1 record whereas the primary hash is used as
    # the key in v0 and so is not duplicated. this also means we only
    # need the one argument because we don't need the information from
    # the key.

    # Return a hash of a record.
    #
    # @param raw [String] the raw record from the database
    #
    # @return [Hash]
    #
    def inflate raw
      # we're about to chomp through this
      raw = raw.dup

      # get the digest algos
      ds = algorithms.map do |a|
        uri = URI::NI.build(scheme: 'ni', path: "/#{a}")
        uri.digest = raw.slice!(0, DIGESTS[a])
        [a, uri]
      end.to_h

      # love this for me
      { digests: ds }.merge(RECORD.keys.zip(raw.unpack(PACKED)).map do |k, v|
        [k, COERCE[RECORD[k]].last.call(v)]
      end.to_h)
    end

    # Return a packed string suitable to store as a record.
    #
    # @param obj [Store::Digest::Object, Hash]
    #
    # @return [String]
    #
    def deflate obj
      obj   = obj.to_h
      algos = algorithms.map { |a| obj[:digests][a].digest }.join
      rec   = RECORD.map { |k, cls| COERCE[cls][1].call obj[k] }

      algos + rec.pack(PACKED)
    end

    # Get an integer entry key from a {Store::Digest::Object} or
    # {Hash} representation thereof, or hash of digests to {URI::NI}
    # objects.
    #
    # @param obj [Store::Digest::Object, Hash]
    # @param raw [false, true] whether to return the raw bytes
    #
    # @return [Integer, nil]
    #
    def get_ptr obj, raw: false
      # normalize the object and obtain a workable hash algorithm
      obj  = obj.to_h
      obj  = obj[:digests] if obj.key? :digests

      algo = if obj.key? primary
               primary
             else
               DIGESTS.sort do |b, a|
                 cmp = b.last <=> a.last
                 cmp == 0 ? a.first <=> b.first : cmp
               end.detect { |x| obj.key? x.first }.first
             end or return

      # warn "algo: #{algo} #{obj[algo.to_sym]} -> #{obj[algo.to_sym].hexdigest}"

      # wat = {}
      # @dbs[algo.to_sym].each { |k, v| wat[k.unpack1 'H*'] = v.unpack1 ?J }

      # warn wat.inspect

      # this is a private method so we can control what its inputs are
      # but it *should* map to a URI::NI; string hashes are too ambiguous
      uri = obj[algo.to_sym]
      raise ArgumentError, "Unexpected #{uri.class}" unless uri.is_a? URI::NI

      # now return the pointer (or nil)
      out = @dbs[algo.to_sym][uri.digest] or return
      raw ? out : out.unpack1(?J)
    end

    # Retrieve a record from the database.
    #
    # @param obj [Store::Digest::Object, Hash, URI::NI, Integer] the
    #  entry's key, or an object from which it can be resolved
    # @param raw [false, true] whether to leave the result as raw bytes
    #
    # @return [Hash, String, nil] inflated or raw record, if present
    #
    def get_meta obj, raw: false
      @lmdb.transaction(true) do
        # get the pointer
        ptr = case obj
              when String then obj
              when Hash, Store::Digest::Object then get_ptr obj, raw: true
              when Integer then [obj].pack ?J
              when URI::NI then @dbs[obj.algorithm.to_sym][obj.digest]
              else
                raise ArgumentError, "Cannot process an #{obj.class}"
              end

        # get the entry (or not)
        break unless ptr && out = @dbs[:entry][ptr]

        # conditionally inflate the result
        raw ? out : inflate(out)
      end
    end

    # Persist the metadata for a {Store::Digest::Object}.
    #
    # @param obj [Store::Digest::Object]
    #
    # @return [void]
    #
    def set_meta obj, preserve: false
      # check if the object has all the hashes
      raise ArgumentError,
        'Object does not have a complete set of digests' unless
        (algorithms - obj.algorithms).empty?

      # since nothing changes in a content-addressable store by
      # definition, the only meaningful changes involve adding
      # information like `type`, `language`, `charset`, `encoding`,
      # and their concomitant checked/valid flags. `size` and `ctime`
      # should never change. `ptime` should be set automatically to
      # `now`, and only if anything else changes. `mtime` should only
      # be changed if `preserve` is false. `dtime`, if present, should
      # be no greater than `now` unless the object is cache. an object
      # with a `dtime` in the past is assumed to be deleted.

      @lmdb.transaction do |txn|
        # initial information
        now   = Time.now in: ?Z
        ptr   = get_ptr(obj, raw: true) || last_key(:entry, raw: true)
        newh  = obj.to_h
        oldh  = nil

        # warn ptr.inspect

        # other things we reuse
        delta    = 0 # whether we are adding or removing a record
        deleted  = newh[:dtime] && newh[:dtime] <= now
        is_cache = !!(newh[:flags] || [])[8] # may not be present

        # check if the entry already exists
        if oldrec = @dbs[:entry][ptr]
          oldh = inflate oldrec

          # the size and ctime should not change
          newh[:size]  = oldh[:size]
          newh[:ctime] = oldh[:ctime]
          newh[:ptime] ||= oldh[:ptime]
          newh[:mtime] = (preserve ? (oldh[:mtime] || newh[:mtime]) :
                          (newh[:mtime] || oldh[:mtime])) || now

          # only the old value if the new one isn't specified
          %i[type language charset encoding].each do |key|
            newh[key] ||= oldh[key]
          end

          # determine if the old record is a tombstone
          tombstone = oldh[:dtime] && oldh[:dtime] <= now

          # OKAY HERE IS THE ALL-IMPORTANT CACHE LOGIC:
          #
          # we want it so that a cache object can be "solidified"
          # (turned into a non-cache object), but a non-cache object
          # can't be turned into a cache object. `dtime` is punned for
          # cache objects as an expiration time and is likely (but not
          # guaranteed) to be in the future.
          #
          if was_cache = oldh[:flags][8]
            # we get here if there is no change in the state of the
            # cache, but we could be overwriting a tombstone, so we
            # want to make sure there is an expiration time.
            if is_cache && !newh[:dtime]
              oexp = oldh[:dtime] && oldh[:dtime] > now
              newh[:dtime] = oexp || now + control_get(:expiry)
            end
          elsif is_cache
            # the record is not cache but it could be a tombstone. we
            # can overwrite it with cache if it is, but not if it
            # isn't, because the implication is something is using it.
            if tombstone
              newh[:dtime] ||= now + control_get(:expiry)
              delta = 1
            else
              newh[:dtime] = nil
              is_cache = newh[:flags][8] = false
              delta = 0
            end
          else
            # neither is cache; we are updating something else.
            # this is whatever the old one was
            newh[:dtime] ||= oldh[:dtime] if deleted
          end

          # accumulate which parts of the record got changed
          changed = RECORD.keys.select { |k| newh[k] != oldh[k] }

          # changed.each do |change|
          #   warn "#{change}: #{oldh[change]} -> #{newh[change]}"
          # end

          # if this is empty there is nothing to do
          break if changed.empty?

          # *now* we can set the ptime
          newh[:ptime] = now if newh[:ptime] == oldh[:ptime]
          changed << :ptime unless changed.include? :ptime

          # we don't index the flags
          (changed - [:flags]).each do |k|
            index_rm k, oldh[k], ptr if oldh[k]
            if k == :dtime
              index_rm  :etime, oldh[:dtime], ptr if was_cache
              index_add :etime, newh[:dtime], ptr if is_cache
            else
              index_add k, newh[k], ptr if newh[k]
            end
          end
        else

          # we are unambiguously adding a thing
          delta = deleted ? 0 : 1

          newh[:ctime] ||= now
          newh[:mtime] ||= now
          newh[:ptime] ||= now
          newh[:type]  ||= 'application/octet-stream'

          # set the algo mappings
          algorithms.each do |algo|
            # warn "setting #{algo} -> #{obj[algo].hexdigest}"
            @dbs[algo].put? obj[algo].digest, ptr
          end

          # set the indices
          RECORD.except(:flags).keys.each do |k|
            if newh[k]
              # special case for non-deleted cache
              kk = k == :dtime ? (is_cache && !deleted) ? :etime : :dtime : k
              index_add kk, newh[k], ptr
            end
          end
        end

        # okay now we actually set the entry
        @dbs[:entry][ptr] = deflate newh

        # now we handle the counts
        if oldrec
          # here we are replacing a record that could be a tombstone,
          # potentially with another tombstone, so we could be adding,
          # removing, or neither.
          control_add :objects, delta
          control_add :deleted, -delta
          control_add :bytes, newh[:size] * delta
        else
          # here we are unconditionally adding a new record, but the
          # record we could be adding could itself be a tombstone.
          control_add :objects, 1

          if delta > 0
            # it's an ordinary entry
            control_add :bytes, newh[:size]
          else
            # it's a tombstone
            control_add :deleted, 1
          end
        end

        # and finally update the mtime
        control_set :mtime, now

        txn.commit

        newh
      end
    end

    # Set `dtime` to the current timestamp and update the indices and stats.
    #
    # @param obj [Store::Digest::Object, Hash, URI::NI, Integer] the
    #  entry's key, or an object from which it can be resolved
    #
    # @return [Hash, nil] the record, if it exists
    #
    def mark_meta_deleted obj
      @lmdb.transaction do
        # nothing to do if there's no entry
        ptr = get_ptr(obj, raw: true) or break
        rec = get_meta ptr
        now = Time.now in: ?Z

        # it's already deleted and we don't need to do anything
        break if rec[:dtime] and rec[:dtime] < now

        # grab this to get the index
        old = rec[:dtime]

        # set the new dtime
        rec[:dtime] = now

        # update the entry
        @dbs[:entry][ptr] = deflate rec

        # deal with the indices
        %i[dtime etime].each { |k| index_rm k, old, ptr } if old
        index_add :dtime, now, ptr

        # deal with the stats/mtime
        control_add :deleted, 1
        control_add :bytes, -rec[:size]
        control_set :mtime, now

        rec
      end
    end

    # Purge the metadata entry from the database and remove it from
    # the indices.
    #
    # @param obj [Store::Digest::Object, Hash, URI::NI, Integer] the
    #  entry's key, or an object from which it can be resolved
    #
    # @return [Hash, nil] the record, if it exists
    #
    def remove_meta obj
      @lmdb.transaction do
        # nothing to do if there's no entry
        ptr = get_ptr(obj) or break
        rec = get_meta ptr
        now = Time.now in: ?Z

        # overwrite the dtime
        deleted = rec[:dtime] and rec[:dtime] < now
        rec[:dtime] = now

        # deal with indices
        RECORD.merge({etime: nil}).except(:flags).keys.each do |key|
          index_rm key, rec[key], ptr
        end

        # deal with the hashes
        algorithms.each do |algo|
          # XXX this *should* match?
          uri = rec[:digests][algo]
          @dbs[algo].delete? uri.digest, ptr
        end

        # deal with stats
        control_add :objects, -1
        if deleted
          control_add :deleted, -1
        else
          control_add :bytes, -rec[:size]
        end

        # the deleted record
        rec
      end
    end
  end

end
