require 'store/digest/version'

module Store::Digest::Driver
  # This is an abstract class for drivers.

  # this is the only implementation we have so far
  autoload :LMDB, 'store/digest/driver/lmdb'

  private

  # for the mapsize parameter
  UNITS = { nil => 1 }
  'kmgtpe'.split('').each_with_index do |x, i|
    j = i + 1
    UNITS[x] = 1000 ** j
    UNITS[x.upcase] = 1024 ** j
  end
  UNITS.freeze

  def int_bytes bytes
    m = /\A\s*(\d+)([kmgtpeKMGTPE])?\s*\Z/s.match bytes.to_s
    raise ArgumentError, "#{bytes} not a viable byte size" unless m

    factor, unit = m.captures
    factor.to_i * UNITS[unit]
  end

  protected

  def setup **options
    raise NotImplementedError, 'gotta roll your own, holmes'
  end
end
