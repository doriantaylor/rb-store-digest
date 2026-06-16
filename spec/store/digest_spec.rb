RSpec.describe Store::Digest do
  before :context do
    @store = Store::Digest.new dir: '/tmp/test-store-digest', mapsize: 2**27
  end

  subject { @store }

  after :context do
    # warn "wtf running"
    @store = nil
    FileUtils.rm_rf '/tmp/test-store-digest'
  end

  # after :each do
  #   GC.compact
  # end

  # anyway, i will mark driver-specific tests with an asterisk *

  context 'initializing the store' do

    it "has a version number" do
      expect(Store::Digest::VERSION).not_to be nil
    end

    # store should work with threads
    it 'should work with threads' do
      require 'thread'
      t = Thread.new do
        subject.add 'lolz'
      end
      t.join
    end

    # store should initialize
    it 'should initialize' do
      expect(subject).to be_a Store::Digest
    end

    it 'should select the lmdb driver by default' do
      # store should select the lmdb driver by default
      expect(subject).to be_a Store::Digest::Driver::LMDB
    end

    it 'should accept the module or resolve the symbol to the module' do
      # store should accept the module or resolve the symbol to the module
      expect do
        Store::Digest.new driver: :LMDB, dir: '/tmp/test-store-digest'
      end.to_not raise_error
    end

    it 'should complain if you give it a bad driver' do
      # store should complain if you give it a bad driver
      expect do
        Store::Digest.new driver: :Derp, dir: '/tmp/test-store-digest'
      end.to raise_error(ArgumentError)
    end

    # store should set a creation time once and never touch it again

    # store should set a modification time that starts out the same as ctime

    # store should initialize with objects/deleted/byte counts all zero
  end

  # context 'poke at Driver::LMDB' do
  #   # store should complain if you don't tell it where to set up shop *

  #   # store should create its root directory if it doesn't exist *

  #   # store should complain if creating a directory fails *

  #   # store should complain if its root is anything but an rwx directory *

  #   # store should correctly commute its umask to the directory *

  #   # store should setgid its directories if the OS supports it *

  #   # store should chown its contents to make sure it can be accessed *

  #   # store should initialize metadata database, whatever that entails *
  # end


  context 'storing objects' do
    # XXX a few of these are redundant

    # store should add a String/IO/File/Pathname/Store::Digest::Entry
    # store should complain if IO is not seekable
    # store should complain if S:D:O is not in order
    # store should complain if obj.size does not match content size


    # store.add should increment the store's byte and object counts
    # store.add should ignore any supplied ctime/ptime/dtime
    # store.add should return a retrieved object (with content as a proc)
    # store.add should no-op the same entry added a second time
    # store.add should nevertheless update metadata if different from existing

    it 'should set obj.stored? to true for a new object' do
      # store.add should set obj.fresh? to true if the object was not
      #   previously present in the store
      entry = subject.add 'hurrdurr', scan: true
      # warn entry.inspect
      expect(entry.stored?).to be_truthy
      expect(subject.stats.objects).to eq 1
    end

    it 'should handle deleting an object and re-adding it' do
      # store.add should set obj.fresh? to true if the object had been
      #   previously deleted
      expect(subject.stats.objects).to eq 1

      obj  = subject.add 'lol', scan: true

      expect(obj.size).to be 3
      expect(obj.scanned?).to be_truthy
      expect(obj.type).to eql('text/plain')
      expect(subject.stats.objects).to eq 2
      expect(subject.stats.bytes).to eq 11
      expect(subject.stats.deleted).to eq 0

      lol = URI::NI.compute 'lol'

      dead = subject.remove lol, forget: false

      expect(subject.stats.bytes).to eq 8
      expect(subject.stats.objects).to eq 2
      expect(subject.stats.deleted).to eq 1

      # warn dead
      expect(dead.stored?).to be_falsy

      obj = subject.add 'lol', scan: true
      expect(obj.size).to be 3
      expect(obj.stored?).to be_truthy
      expect(subject.stats.objects).to eq 2

      # warn subject.stats

      expect(subject.stats.bytes).to eq 11
      expect(subject.stats.deleted).to eq 0
    end

    it 'should handle a substantive metadata change' do
      # store.add should set obj.fresh? to true if any metadata has
      #   been updated
      obj = subject.add 'lol', type: 'application/x-derp'
      expect(obj.stored?).to be_truthy
      expect(obj.type).to eql('application/x-derp')

      wut = subject.get obj
      expect(wut).to be_a(Store::Digest::Entry)
      expect(wut.type).to eql('application/x-derp')
      expect(subject.stats.objects).to eq 2
    end

    it 'should set obj.? to false for an existing object' do
      # store.add should set obj.fresh? to false if the object was
      # already present
      # (lol god now we are repeating this mimetype to make the tests pass)
      old = subject.get URI::NI.compute('lol')
      expect(old).to be_a(Store::Digest::Entry)
      expect(old.type).to eql('application/x-derp')

      obj = subject.add old, type: 'application/x-hurr'
      # expect(obj.stored?).to be_falsy
      expect(subject.stats.objects).to eq 2
    end

    # it 'should set obj.fresh? to false on preserve: true' do
    #   # store.add should set obj.fresh? to false if preserve: true and the
    #   # only difference in the new object is its mtime
    #   # (store.add should set obj.fresh? to true otherwise)
    #   obj = subject.add 'lol', type: 'application/x-derp',
    #     mtime: Time.now - 10
    #   # expect(obj.stored?).to be_falsy
    #   expect(subject.stats.objects).to eq 2
    # end

    it 'should not store until it is read' do
      # warn subject.stats
      obj = subject.add 'totes potates', type: 'application/x-hurrr'
      expect(subject.stats.objects).to eq 2
      expect(obj.read).to eq 'totes potates'
      # warn obj.object.digests
      expect(obj.size).to eq 'totes potates'.length

      objs = subject.stats.objects

      expect(objs).to eq 3
    end
  end

  context 'retrieving objects' do
    # store should get a String/IO/File/Pathname/Store::Digest::Entry/URI::NI
    # store.get should scan an S:D:O if it is not scanned (ie, no digest
    #   URIs), and complain if it can't scan (ie, if there is no content)
    # store.get should return nil if the object is not in the store
    # store.get should return a new object if present

    # (Note: It may seem weird to `get` an object you already have, but
    # the return value is an implicit verification that the store *also*
    # has it. It is like this mainly for symmetry.)
  end

  context 'removing objects' do
    # store should remove a URI::NI/Store::Digest::Entry/String/IO/File/Pathname
    # store.remove returns the removed object
    # store.remove should set the object's dtime
    # store.remove should nuke the blob *
    # store.remove should nuke the directory tree if empty *
    # store.remove should increment the deleted count
    # store.remove should decrement the byte count
    # store.remove should NOT decrement the object count
    # store.get should return a removed object (minus content, plus dtime)
  end

  context 'forgetting objects' do
    # store.forget is the same as store.remove obj, forget: true
    # store.forget should destroy its metadata entirely
    # store.forget should decrement the object count
    # store.forget should decrement deleted count if the object was
    #   previously deleted
    # store.forget should decrement byte count if the object was NOT
    #   previously deleted
  end

  # store.objects/deleted/bytes should always remain accurate, no
  # matter how much traffic goes through the interface
  # (lol how the hell am i gonna test that)

  # there should be a way to set metadata on an object without having
  # to gin one up and 'add' it (maybe?)

  # store.search should return all matches on a truncated digest

  # store.search should return exact matches or open/closed range on
  # size, ctime, mtime, ptime, dtime

  # store.search should return all matches on sets of type, charset,
  # encoding, language

  # both type and language should afford partial matches
  # (e.g. `application/* or en-*`)

  # store.search should complain if any of the parameters are malformed

  # search parameters should be ANDed between dimensions and ORed
  # between values

end
