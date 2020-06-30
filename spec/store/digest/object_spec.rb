RSpec.describe Store::Digest::Object do
  context 'creating an object' do
    obj = Store::Digest::Object.new

    it 'initializes with empty content' do 
      # object initializes blank
      expect(obj.content).to be_nil
    end

    it 'has the most basic content type' do
      # object defaults to application/octet-stream
      expect(obj.type).to eql 'application/octet-stream'
    end

    it 'has zero size' do
      # object defaults to size 0
      expect(obj.size).to be 0
    end

    it 'is not fresh' do
      expect(obj.fresh?).to be false
    end
  end

  context 'scanning data' do
    it 'can scan a String' do
      # object can scan a String
      obj = Store::Digest::Object.scan 'string lol'
      expect(obj.size).to be 10
      expect(obj.type).to eql 'text/plain'
    end

    it 'can scan a File' do
      # object can scan a File
      fh  = File.open __FILE__
      obj = Store::Digest::Object.scan fh

      expect(obj.size).to be fh.size
      expect(obj.type).to eql 'application/x-ruby'
      expect(obj.fresh?).to be false
    end

    it 'can scan a Pathname' do
      # object can scan a Pathname
      pn = Pathname(__FILE__)

      obj = Store::Digest::Object.scan pn

      expect(obj.size).to be pn.size
      expect(obj.type).to eql 'application/x-ruby'
    end

    it 'can scan an IO' do
      # object can scan an IO

      # uhh now wondering if this makes any sense
    end
    
    it 'can scan a Proc (that returns an IO)' do
    # object can scan a Proc (that returns an IO)
      proc = Proc.new { StringIO.new 'lol' }
      obj  = Store::Digest::Object.scan(proc)
      expect(obj.size).to be 3
    end

    it 'complains if the coerced IO can\'t seek/tell' do
      # object complains if the coerced IO can't seek/tell (ie no pipes/sockets)
      io = IO.popen ['ping', '-?'], err: %i[child out]
      expect { Store::Digest::Object.scan io }.to raise_error(Errno::ESPIPE)
    end

    # object correctly sets blob size from scan
    # object correctly gleans mtime from content (if file)
    # object correctly gleans content-type from path if input is a file
    # object otherwise obtains content-type by sampling input
    # object complains if specified digests are not in the inventory
    # object digests of course match the content
    # user should not be able to modify content or digests after a scan
    # user should not be able to overwrite size, ctime, ptime, or dtime
    # user should not be able to set type/charset/language/encoding to garbage
    # input for mtime/type/charset/language/encoding should be normalized
  end
end
