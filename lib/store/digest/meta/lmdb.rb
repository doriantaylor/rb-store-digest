require 'store/digest/meta'
require 'store/digest/trait'

require 'lmdb'
require 'uri/ni'

# Symas LMDB Metadata driver.
module Store::Digest::Meta::LMDB
  include Store::Digest::Meta
  include Store::Digest::Trait::RootDir

  autoload :V0, 'store/digest/meta/lmdb/v0'
  autoload :V1, 'store/digest/meta/lmdb/v1'

  private

  PRIMARY = :"sha-256"
  DIGESTS = {
    md5:       16,
    "sha-1":   20,
    "sha-256": 32,
    "sha-384": 48,
    "sha-512": 64,
  }.freeze

  LMDB_OPTS = %i[mode maxreaders maxdbs mapsize]

  LMDB_FLAGS =
    %i[fixedmap nosubdir nosync rdonly nometasync writemap mapasync notls]

  def meta_get_stats
    # XXX this should be a read transaction
    lmdb.transaction? true do |txn|
      control = lmdb[:control]
      h = %i[ctime mtime objects deleted bytes].map do |k|
        [k, db_decode(control[k.to_s], k)]
      end.to_h

      # fix the times
      %i[ctime mtime].each { |t| h[t] = Time.at h[t] }

      # get counts on all the countables
      h.merge!(
        %i[type language charset encoding].map do |d|
          db = lmdb[d]
          ["#{d}s".to_sym, db.keys.map { |k| [k, db.cardinality(k)] }.to_h]
        end.to_h)

      # would love to do min/max size/dates/etc but that is going to
      # take some lower-level cursor finessing
      # txn.commit
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

    @lmdb_opts = {
      mode: 0666 & ~umask,
      mapsize: mapsize,
    }.merge(options.slice(*(LMDB_OPTS + LMDB_FLAGS)))

    algos = options[:algorithms] || DIGESTS.keys
    raise ArgumentError, "Invalid algorithm specification #{algos}" unless
      algos.is_a? Array and (algos - DIGESTS.keys).empty?

    popt = options[:primary] || PRIMARY
    raise ArgumentError, "Invalid primary algorithm #{popt}" unless
      popt.is_a? Symbol and DIGESTS[popt]

    lmdb.transaction? do
      # load up the control database
      control = lmdb.database('control', create: true)

      # if control is empty or version is 1, extend V1
      if control.empty?
        # set to v1 for next time
        control['version'] = ?1
        extend V1
      elsif control['version'] == ?1
        extend V1
      elsif control['version'].nil?
        # if version is empty, extend v0
        @dbs = { control: control }
        extend V0
      else
        # otherwise error
        v = control['version']
        raise CorruptStateError, "Control database has unrecognized version #{v}"
      end

      if a = algorithms
        raise ArgumentError,
        "Supplied algorithms #{algos.sort} do not match instantiated #{a}" if
          algos.sort != a
      else
        a = algos.sort
        control['algorithms'] = a.join ?,
      end

      if pri = primary
        raise ArgumentError,
        "Supplied algorithm #{popt} does not match instantiated #{pri}" if
          popt != pri
      else
        pri = popt
        control['primary'] = popt.to_s
      end
      setup_dbs
    end

    lmdb.sync
  end

  public

  # Return the LMDB handle for the given process.
  #
  # @return [LMDB::Environment]
  #
  def lmdb
    (@lmdb ||= {})[Process.pid] ||= ::LMDB.new dir, @lmdb_opts
  end

  # Wrap the block in a transaction. Trying to start a read-write
  # transaction (or do a write operation, as they are wrapped by
  # transactions internally) within a read-only transaction will
  # almost certainly break.
  #
  # @param readonly [false, true] whether the transaction is read-only
  # @param block [Proc] the code to run.
  #
  def transaction readonly: false, &block
    lmdb.transaction?(readonly) do
      # we do not want to transmit the transaction
      block.call
    end
  end

  # Return the set of algorithms initialized in the database.
  # @return [Array] the algorithms
  def algorithms
    @algorithms ||= lmdb.transaction? true do
      if ret = lmdb[:control]['algorithms']
        ret.strip.downcase.split(/\s*,+\s*/).map(&:to_sym)
      end
    end
  end

  # Return the primary digest algorithm.
  # @return [Symbol] the primary algorithm
  def primary
    @primary ||= lmdb.transaction? true do
      if ret = lmdb[:control]['primary']
        ret.strip.downcase.to_sym
      end
    end
  end

  # Return the number of objects in the database.
  # @return [Integer]
  def objects
    lmdb.transaction? true do
      if ret = lmdb[:control]['objects']
        db_decode ret, :objects
      end
    end
  end

  # Return the number of objects whose payloads are deleted but are
  # still on record.
  # @return [Integer]
  def deleted
    lmdb.transaction? true do
      if ret = lmdb[:control]['deleted']
        db_decode ret, :deleted
      end
    end
  end

  # Return the number of bytes stored in the database (notwithstanding
  # the database itself).
  # @return [Integer]
  def bytes
    lmdb.transaction? true do
      if ret = lmdb[:control]['bytes']
        db_decode ret, :bytes
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
      [k, lmdb[k].size]
    end.sort { |a, b| a[1] <=> b[1] }.map(&:first).first
    out = {}
    lmdb.transaction? true do
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
        lmdb[primary].cursor do |c|
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
end
