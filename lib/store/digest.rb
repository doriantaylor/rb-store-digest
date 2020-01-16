require "store/digest/version"

require 'base32' # for the file
require 'uri'
require 'uri/ni'
require 'lmdb'
require 'mimemagic'

require 'store/digest/driver'

class Store::Digest
  private

  # flag constants
  TYPE_CHECKED     = 1 << 0
  TYPE_VALID       = 1 << 1
  CHARSET_CHECKED  = 1 << 2
  CHARSET_VALID    = 1 << 3
  ENCODING_CHECKED = 1 << 4
  ENCODING_VALID   = 1 << 5
  SYNTAX_CHECKED   = 1 << 6
  SYNTAX_VALID     = 1 << 7

  LABELS = {
    size:     'Size (Bytes)',
    ctime:    'Added to Store',
    mtime:    'Last Modified',
    ptime:    'Properties Modified',
    dtime:    'Deleted',
    type:     'Content Type',
    language: '(Natural) Language',
    charset:  'Character Set',
    encoding: 'Content Encoding',
  }.freeze

  FLAG  = %i[content-type charset content-encoding syntax].freeze
  STATE = %i[unverified invalid recheck valid].freeze

  public

  # Initialize a storage
  def initialize **options
    driver = options.delete(:driver) || Store::Digest::Driver::LMDB
    warn driver.ancestors.inspect
    raise ArgumentError,
      "Driver #{driver} is not a Store::Digest::Driver" unless
      driver.ancestors.include? Store::Digest::Driver
    extend driver

    # load a driver
    # initialize driver stuff
    #super(**options)
  end

  #
  def add obj
    transaction do
      obj = coerce_object obj
    end
  end

  #
  def get obj
  end

  #
  def remove obj, forget: false
    # remove blob and mark metadata entry as deleted
    transaction do
      obj = coerce_object obj
      forget ? remove_meta(obj) : mark_deleted(obj)
      remove_blob
    end

    obj
  end

  # Remove an object from the store and "forget" it ever existed,
  # i.e., purge it from the metadata.
  # 
  def forget obj
    remove obj, forget: true
  end

  # Return statistics on the store
  def stats
  end

  class Object
    #

    #
    def initialize content = nil, size: 0, type: 'application/octet-stream',
        charset: nil, language: nil, encoding: :identity,
        ctime: nil, mtime: nil, ptime: nil, dtime: nil, flags: 0
    end

    attr_reader :digests, :size, :type, :charset, :language, :encoding,
      :ctime, :mtime, :ptime, :dtime, :flags

    def digest symbol
      digests[symbol]
    end

    alias_method :"[]", :digest

    # Returns true if the content type has been checked.
    def type_checked?
      @flags & TYPE_CHECKED
    end

    # Returns true if the content type has been checked _and_ is valid.
    def type_valid?
      @flags & (TYPE_CHECKED|TYPE_VALID)
    end

    # Returns true if the character set has been checked.
    def charset_checked?
      @flags & CHARSET_CHECKED
    end

    # Returns true if the character set has been checked _and_ is valid.
    def charset_valid?
      @flags & (CHARSET_CHECKED|CHARSET_VALID)
    end

    # Returns true if the content encoding (e.g. gzip, deflate) has
    # been checked.
    def encoding_checked?
      @flags & ENCODING_CHECKED
    end

    # Returns true if the content encoding has been checked _and_ is valid.
    def encoding_valid?
      @flags & (ENCODING_CHECKED|ENCODING_VALID)
    end

    # Returns true if the blob's syntax has been checked.
    def syntax_checked?
      @flags & SYNTAX_CHECKED
    end

    # Returns true if the blob's syntax has been checked _and_ is valid.
    def syntax_valid?
      @flags & (SYNTAX_CHECKED|SYNTAX_VALID)
    end

    # Outputs a human-readable string representation of the object.
    def to_s
      out = "#{self.class}\n  Digests:\n"

      # disgorge the digests
      digests.each { |d| out << "    #{digest d}\n" }

      # now the fields
      MANDATORY.each { |m| out << "  #{LABELS[m]}: #{call m}\n" }
      OPTIONAL.each do |o|
        val = call o
        out << "  #{LABELS[o]}: #{val}\n" if val
      end

      # now the validatio
      out << "Validation:\n"
      FLAG.each_index do |i|
        x = f >> (3 - i) & 3
        out << ("    %-16s: %s\n" % [FLAG[i], STATE[x]])
      end

      out
    end
  end

  class Stats
  end
end
