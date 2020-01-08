require 'store/digest/driver'
require 'store/digest/blob/filesystem'
require 'store/digest/meta/lmdb'

class Store::Digest::Driver::LMDB < Store::Digest::Driver
  include Blob::FileSystem
  include Meta::LMDB

end
