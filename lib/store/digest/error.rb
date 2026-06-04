# again for the symbol
require 'store/digest/version'

class Store::Digest::Error < RuntimeError

  # Raised when you try to add a deleted entry to a store.
  #
  class Deleted < self
  end

  # Raised when a scanned property fails to match an asserted property.
  #
  # @note Use this when e.g. the size or type don't match.
  #
  class Integrity < self
  end

  # Raised specificially when a scanned hash doesn't match an asserted one.
  #
  class CryptographicIntegrity < Integrity
  end

end
