require 'store/digest/driver'
require 'store/digest/blob/filesystem'
require 'store/digest/meta/lmdb'

module Store::Digest::Driver::LMDB
  include Store::Digest::Driver
  include Store::Digest::Blob::FileSystem
  include Store::Digest::Meta::LMDB
end
