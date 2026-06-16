require 'store/digest/meta'

module Store::Digest::Meta::LMDB
  # This is the version zero (original) database layout.
  module V0

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

    private

    def db_decode raw, type
      raw.unpack1 PACK[type]
    end

    def db_encode value, type
      [value].pack PACK[type]
    end

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
      end.to_h.merge(algorithms.map { |k| [k, []] }.to_h)

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
    # @param obj [Store::Digest::Entry] the object to store
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

      @lmdb.transaction(false, &body)
      # body.call
      # end
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

      @lmdb.transaction(true, &body)
      #   body.call
      # end
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
end
