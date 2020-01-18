require 'store/digest/blob'
require 'store/digest/trait'

require 'time'
require 'pathname'
require 'base32'
require 'tempfile'

module Store::Digest::Blob::FileSystem
  include Store::Digest::Trait::RootDir

  private

  STORE = 'store'.freeze
  TMP   = 'tmp'.freeze

  # The location of the store
  # @return [Pathname]
  def store
    dir + STORE
  end

  # The location of the temp directory
  # @return [Pathname]
  def tmp
    dir + TMP
  end

  # Return a hash-pathed location of the blob, suitable for
  # case-insensitive file systems.
  # @param bin [String] The binary representation of the keying digest
  # @return [Pathname] The absolute path for the blob
  def path_for bin
    parts = Base32.encode(bin).tr('=', '').downcase.unpack 'a4a4a4a*'
    store + parts.join('/')
  end

  protected

  def setup **options
    super

    [STORE, TMP].each do |d|
      d = dir + d
      if d.exist?
        raise "#{d} exists and is not a directory!" unless d.directory?
        raise "#{d} is not readable!"   unless d.readable?
        raise "#{d} is not writable!"   unless d.writable?
        raise "#{d} cannot be entered!" unless d.executable?
      else
        # wtf Pathname#mkdir takes no args
        Dir.mkdir d, 0777 & ~umask
      end
    end
  end

  # Return an open tempfile in the designated temp directory
  # @return [Tempfile]
  def temp_blob
    Tempfile.new 'blob', tmp
  end

  # Settle a blob from its temporary location to its permanent location.
  # @param bin [String] The binary representation of the keying digest
  # @param fh  [File] An open filehandle, presumably a temp file
  # @param mtime [nil, Time, DateTime, Integer] the modification time
  #  (defaults to now)
  # @param overwrite [false, true] whether to overwrite the target
  # @return [true] a throwaway return value
  # @raise [SystemCallError] as we are mucking with the file system
  def settle_blob bin, fh, mtime: nil, overwrite: false
    # get the mtimes
    mtime ||= Time.now
    mtime = case mtime
            when Time    then mtime.to_i
            when Integer then mtime
            when -> x { x.respond_to? :to_time }
              mtime.to_time.to_i
            else
              raise ArgumentError,
                "mtime must be a Time, DateTime, or Integer, not #{mtime.class}"
            end

    # get the filenames
    source = fh.path
    target = path_for bin

    # make sure this thing is flushed
    unless fh.closed?
      fh.flush
      fh.close
    end

    # these can all raise, of course
    FileUtils.mkpath(target.dirname, mode: 0777 & ~umask)

    if !target.exist? || overwrite
      FileUtils.mv source, target
      target.chmod 0444 & ~umask
      target.utime mtime, mtime
    end

    true
  end

  # Return a blob filehandle (or closure that will return said blob).
  # @param bin [String] The binary representation of the keying digest
  # @param direct [false, true] whether to open the filehandle directly
  # @return [Proc, IO] Either a closure or the blob itself
  # @raise  [RuntimeError] blows up if the blob is not what is expected
  # @raise  [SystemCallError] if there's trouble opening the blob
  def get_blob bin, direct: false
    path = path_for bin
    return unless path.exist?
    hex = bin.unpack1 'H*'
    raise "Blob #{hex} is not a file!"   unless path.file?
    raise "Blob #{hex} is not readable!" unless path.readable?

    # return a closure (maybe)
    direct ? path.open('rb') : -> { path.open('rb') }
  end

  # Remove a blob based on its binary digest value.
  # @param bin [String] The binary representation of the keying digest
  # @return [File] reutnr
  # @raise  [SystemCallError] since it's mucking with the file system
  def remove_blob bin
    # XXX we should really flock the directory stack
    path = path_for bin
    ret  = if path.exist?
             fh = path.open 'rb'
             path.unlink
             fh
           end

    # XXX we should really flock the directory stack
    dn = path.dirname.relative_path_from(store).to_s.split ?/
    dn.each_index.reverse_each do |i|
      subpath = store + dn.slice(0..i).join(?/)
      subpath.rmdir if subpath.exist? and subpath.empty?
    end

    ret
  end

end
