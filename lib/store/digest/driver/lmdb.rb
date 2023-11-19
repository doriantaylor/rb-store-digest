require 'store/digest/driver'
require 'store/digest/blob/filesystem'
require 'store/digest/meta/lmdb'

module Store::Digest::Driver::LMDB
  include Store::Digest::Driver
  include Store::Digest::Blob::FileSystem
  include Store::Digest::Meta::LMDB

  protected

  def setup **options
    options[:mapsize] = int_bytes options[:mapsize] if options[:mapsize]

    super
  end
end
