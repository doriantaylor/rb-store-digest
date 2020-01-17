require 'store/digest/version'

module Store::Digest::Driver
  # This is an abstract class for drivers.

  # this is the only implementation we have so far
  autoload :LMDB, 'store/digest/driver/lmdb'

  protected

  def setup **options
    raise NotImplementedError, 'gotta roll your own, holmes'
  end
end
