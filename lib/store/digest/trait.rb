require 'store/digest/version'

require 'pathname'
require 'fileutils'

module Store::Digest::Trait
  module RootDir
    attr_reader :umask, :dir

    protected

    def setup **options
      # deal with umask
      @umask = options[:umask] || 0077
      raise ArgumentError, 'umask must be a non-negative integer' unless
        @umask.is_a? Integer and @umask >= 0
      @umask &= 0777

      # deal with root
      raise ArgumentError,
        'Must specify a working directory' unless options[:dir]
      @dir = Pathname(options[:dir]).expand_path
      if @dir.exist?
        raise "#{dir} already exists and is not a directory" unless
          @dir.directory?
      else
        FileUtils.mkpath @dir, mode: (0777 & ~@umask | 02000)
      end
      raise "Specified directory #{@dir} must be writable" unless @dir.writable?
    end
  end
end
