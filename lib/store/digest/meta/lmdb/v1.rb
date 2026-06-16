require 'store/digest/meta'

module Store::Digest::Meta::LMDB
  # This is the version 1 database layout.
  module V1

    private

    # import the flags
    Flags = Store::Digest::Entry::Flags

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
      type = CONTROL[type] || RECORD[type] if type.is_a? Symbol

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
      type = CONTROL[type] || RECORD[type] if type.is_a? Symbol
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
    # @param obj [Store::Digest::Entry, Hash]
    #
    # @return [String]
    #
    def deflate obj
      obj   = obj.to_h
      algos = algorithms.map { |a| obj[:digests][a].digest }.join
      rec   = RECORD.map { |k, cls| COERCE[cls][1].call obj[k] }

      algos + rec.pack(PACKED)
    end

    # Get an integer entry key from a {Store::Digest::Entry} or
    # {Hash} representation thereof, or hash of digests to {URI::NI}
    # objects.
    #
    # @param obj [Store::Digest::Entry, Hash]
    # @param raw [false, true] whether to return the raw bytes
    #
    # @return [Integer, nil]
    #
    def get_ptr obj, raw: false
      uri = coerce_uri(obj) or return

      # now return the pointer (or nil)
      out = @dbs[uri.algorithm][uri.digest] or return
      raw ? out : out.unpack1(?J)
    end

    # Returns a comparator function suitable for picking the right mtime.
    #
    # @example
    #  cmp = mtime_cmp
    #  newh[:mtime] = [oldh[:mtime], newh[:mtime]].sort(&cmp).first
    #
    # @return [Proc] the comparator
    #
    def mtime_cmp
      policy = {
        preserve: -> a, b { -1 },
        replace:  -> a, b { 1 },
        oldest:   -> a, b { a <=> b },
        newest:   -> a, b { b <=> a },
      }[mtimes]

      raise Store::Digest::Error::Configuration,
        "Can't find modification time comparator for #{mtimes}" unless policy

      return -> a, b do
        raise ArgumentError, 'both comparands are nil' if a.nil? && b.nil?
        return -1 if b.nil?
        return  1 if a.nil?

        policy.call a, b
      end
    end

    protected

    # Retrieve a record from the database.
    #
    # @param obj [Store::Digest::Entry, Hash, URI::NI, Integer] the
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
              when Hash, Store::Digest::Entry then get_ptr obj, raw: true
              when Integer then [obj].pack ?J
              when URI::NI then @dbs[obj.algorithm.to_sym][obj.digest]
              else
                raise ArgumentError, "Cannot process an #{obj.class}"
              end

        if ptr && out = @dbs[:entry][ptr]
          raw ? out : inflate(out)
        end
      end
    end

    # Persist the metadata for a {Store::Digest::Entry}.
    #
    # @param obj [Store::Digest::Entry, Hash]
    #
    # @return [Hash] the updated metadata hash.
    #
    def set_meta2 obj
      # * create a new entry
      # * update metadata of an existing entry
      #   * update fields (no change to status)
      #     * can't update a tombstone
      #     * can't turn non-cache into cache
      #   * turn a cache entry to non-cache
      #     * (remove from etime index and clear out dtime)
      #   * undelete a tombstone
      #     * (remove from dtime index and clear out dtime)
      #   * mark an entry deleted
      #     * note you need the whole record here instead of just the
      #       hash, but we'll support it for parity so the stat counts
      #       don't get messed up
      #
      # deltas:
      # * if new:
      #   * entries + 1
      #   * bytes + N
      # * if undeleting tombstone:
      #   * entries + 0
      #   * deleted - 1
      #   * bytes + N
      # * if marking deleted
      #   * entries - 0
      #   * deleted + 1
      #   * bytes - N
      #

      # check if the object has all the hashes
      raise ArgumentError,
        'Object does not have a complete set of digests' unless
        (algorithms - obj[:digests].keys).empty?

      now     = Time.now(in: ?Z)
      newh    = obj.to_h.dup
      oldh    = nil
      changes = Set[]

      # warn newh.inspect

      # determine if newh is cache
      if is_cache = (newh[:flags] ||= Flags.from(0)).cache
        raise ArgumentError,
          'Cache flag set but expiry is not' unless newh[:dtime]
        if newh[:dtime].is_a? Numeric
          raise ArgumentError,
            'Cache expiry offset must be non-negative' if newh[:dtime] < 0
          newh[:dtime] = now + newh[:dtime]
        elsif !newh[:dtime].is_a?(Time)
          newh[:dtime] = now + cache_ttl
        end
      end

      # determine if newh is a tombstone
      if is_ts = newh[:dtime] && newh[:dtime] <= now
        # warn "#{coerce_uri obj} is tombstone: #{newh[:dtime]}"
        newh[:dtime] = now # normalize dtime to now
      end

      # if newh[:flags].cache is true:
      # * newh[:dtime] must be truthy
      # * newh[:dtime] can be a Time, a positive Numeric, or coercible to `true`
      # * if newh[:dtime] is a Time it must be in the future
      # * if newh[:dtime] is a number it is added to `ptime` (`Time.now`)
      # * otherwise newh[:dtime] is set to now + CACHE_TTL

      # * if it turns out that oldh[:flags].cache is falsy:
      #   * unless oldh is a tombstone (has a dtime in the past):
      #     * newh[:flags].cache is cleared
      #     * newh[:dtime] is cleared
      #     * (unless newh is also a tombstone in which case oldh[:dtime] is used)
      # if newh is a tombstone it doesn't matter whether it's cache, however:
      # * can't update a non-cache entry to cache, even if it's a tombstone
      # * can't update the dtime on a tombstone (it's already dead)
      # * the only legal moves are to change the inevitable expiry
      #   date of an existing cache entry, including into the past
      #   (clipped at Time.now).

      @lmdb.transaction do |txn|
        # `last_key` gives us a new pointer if one does not exist
        ptr = get_ptr(obj, raw: true) || last_key(:entry, raw: true)

        # warn ptr.unpack1(?J)

        added  = false
        was_ts = nil

        if oldrec = @dbs[:entry][ptr]
          # there are only three legal operations with an existing record:
          #
          # * mark the record as a tombstone
          # * reinstate a tombstone as a live record
          # * change some other metadata on a live record:
          #   * change a cache record to non-cache
          #   * update some other metadata that doesn't touch the
          #     cache flag or dtime field (unless changing it)
          #
          # these all basically reduce to "change some metadata" with
          # a handful of rules attached:
          #
          # ctime is minted once and never changes
          # ptime is always set to now if there is something to update
          # mtime goes according to policy:
          # * :preserve keeps the original mtime and never updates it
          # * :update always picks the replacement mtime
          # * :oldest always picks the older of the two
          # * :newest always picks the younger of the two
          # dtime meaning changes depending on whether the entry is cache
          # * if non-nil and not cache, it represents a tombstone (and
          #   MUST be in the past, or actually overwritten to now)
          # * if non-nil and cache, it represents an expiry date

          oldh = inflate oldrec


          # a change in size should blow up
          raise Store::Digest::Error::Integrity,
            "attempt to overwrite size #{oldh[:size]} with #{newh[:size]}" if
            newh[:size] && newh[:size] != oldh[:size]

          # do all the assignments/folding/merging

          # these are always going to be whatever they were
          newh[:size]  = oldh[:size]
          newh[:ctime] = oldh[:ctime]

          # warn "#{oldh[:type]} -> #{newh[:type] || oldh[:type]}"

          %i[type charset encoding language].each do |key|
            newh[key] ||= oldh[key]
          end

          cmp = mtime_cmp
          newh[:mtime] = [oldh[:mtime], newh[:mtime]].sort(&cmp).first

          # determine if oldh is cache
          was_cache = oldh[:flags].cache
          # deterimine if oldh is a tombstone
          was_ts = oldh[:dtime] && oldh[:dtime] <= now
          added  = was_ts && !is_ts

          if was_ts
            # noop because the only way to change a tombstone is to reinstate it
            newh = oldh.dup if is_ts
          elsif !was_cache && is_cache
            # wipe out cache flag and clear dtime on newh
            is_cache = newh[:flags].cache = false
            newh[:dtime] = is_ts ? now : nil
          elsif is_ts
            newh[:dtime] = now
          end

          %i[mtime dtime flags type charset encoding language].each do |key|
            changes << key unless oldh[key] == newh[key]
          end

          # now we know there is a change so set ptime
          unless changes.empty?
            newh[:ptime] = now
            changes << :ptime
          end
        else
          # always a new entry
          newh[:ctime] = now
          newh[:ptime] = now
          newh[:mtime] ||= now
          newh[:type]  ||= MimeMagic['application/octet-stream']

          # set the algo mappings
          algorithms.each do |algo|
            # warn "setting #{algo} -> #{obj[algo].hexdigest}"
            @dbs[algo].put? obj[:digests][algo].digest, ptr
          end

          added = true
          changes |= RECORD.keys
        end

        # warn "got here with #{coerce_uri obj}: #{changes}"

        # update indices and control
        unless changes.empty?
          # update the record
          @dbs[:entry][ptr] = deflate newh
          # dummy oldh
          oldh ||= {}

          # do the indices
          (changes - [:flags]).each do |key|
            # delete old and index entry and add new
            if key == :dtime
              oldk = was_cache ? :etime : :dtime
              newk = is_cache  ? :etime : :dtime

              index_rm  oldk, oldh[:dtime], ptr if oldh[:dtime]
              index_add newk, newh[:dtime], ptr
            else
              index_rm  key, oldh[key], ptr if oldh[key]
              index_add key, newh[key], ptr if newh[key]
            end
          end

          # do the stats
          if oldrec
            if !was_ts && is_ts
              control_add :deleted, 1
              control_add :bytes, -oldh[:size]
            elsif was_ts && !is_ts
              control_add :deleted, -1
              control_add :bytes, oldh[:size]
            end
          else
            control_add :objects, 1
            control_add :bytes, newh[:size]
          end

          # set the global modification time
          control_set :mtime, now
        end

        # txn.commit
      end

      newh
    end

    # Set `dtime` to the current timestamp and update the indices and stats.
    #
    # @param obj [Store::Digest::Entry, Hash, URI::NI, Integer] the
    #  entry's key, or an object from which it can be resolved
    #
    # @return [Hash, nil] the record, if it exists
    #
    def mark_meta_deleted obj
      @lmdb.transaction do |txn|
        # nothing to do if there's no entry
        if ptr = get_ptr(obj, raw: true)
          rec = get_meta ptr
          now = Time.now in: ?Z

          # it's already deleted and we don't need to do anything
          unless rec[:dtime] and rec[:dtime] <= now

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
      end
    end

    # Purge the metadata entry from the database and remove it from
    # the indices.
    #
    # @param obj [Store::Digest::Entry, Hash, URI::NI, Integer] the
    #  entry's key, or an object from which it can be resolved
    #
    # @return [Hash, nil] the record, if it exists
    #
    def remove_meta obj
      @lmdb.transaction do
        # nothing to do if there's no entry
        if ptr = get_ptr(obj)
          rec = get_meta ptr
          now = Time.now in: ?Z

          # overwrite the dtime
          tombstone = rec[:dtime] && rec[:dtime] <= now
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

    public

    # Return the default time-to-live on cache entries.
    #
    # @return [Integer] the TTL in seconds
    #
    def cache_ttl
      control_get :expiry
    end

    # Set a new default time-to-live on cache entries.
    #
    # @param ttl [
    #
    # @return [Integer] the new TTL in seconds
    #
    def cache_ttl= ttl
      control_set :expiry, ttl
    end
  end
end
