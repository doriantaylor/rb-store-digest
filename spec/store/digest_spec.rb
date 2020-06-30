RSpec.describe Store::Digest do
  it "has a version number" do
    expect(Store::Digest::VERSION).not_to be nil
  end

  # can't really test this thing other than as a system, but here goes

  # actually no that's not true, we can test the objects in isolation:

  context 'creating objects' do
  # object initializes blank
  # object defaults to application/octet-stream
  # object defaults to size 0
  # object can scan a String
  # object can scan a File
  # object can scan a Pathname
  # object can scan an IO
  # object can scan a Proc (that returns an IO)
  # object complains if the coerced IO can't seek/tell (ie no pipes/sockets)
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

  # anyway, i will mark driver-specific tests with an asterisk *

  context 'initializing the store' do
  # store should initialize
  # store should select the lmdb driver by default
  # store should accept the module or resolve the symbol to the module
  # store should complain if you give it a bad driver
  # store should complain if you don't tell it where to set up shop *
  # store should create its root directory if it doesn't exist *
  # store should complain if creating a directory fails *
  # store should complain if its root is anything but an rwx directory *
  # store should correctly commute its umask to the directory *
  # store should setgid its directories if the OS supports it *
  # store should chown its contents to make sure it can be accessed *
  # store should initialize metadata database, whatever that entails *
  # store should set a creation time once and never touch it again
  # store should set a modification time that starts out the same as ctime
  # store should initialize with objects/deleted/byte counts all zero
  end

  context 'storing objects' do
  # store should add a String/IO/File/Pathname/Store::Digest::Object
  # store should complain if IO is not seekable
  # store should complain if S:D:O is not in order
  # store should complain if obj.size does not match content size
  # store.add should increment the store's byte and object counts
  # store.add should ignore any supplied ctime/ptime/dtime
  # store.add should return a retrieved object (with content as a proc)
  # store.add should no-op the same entry added a second time
  # store.add should nevertheless update metadata if different from existing

  # store.add should set obj.fresh? to true if the object was not
  #   previously present in the store
  # store.add should set obj.fresh? to true if the object had been
  #   previously deleted
  # store.add should set obj.fresh? to true if any metadata has been
  #   updated
  # store.add should set obj.fresh? to false if the object was already
  #   present
  # store.add should set obj.fresh? to false if preserve: true and the
  #   only difference in the new object is its mtime
  # (store.add should set obj.fresh? to true otherwise)
  end

  context 'retrieving objects' do
  # store should get a String/IO/File/Pathname/Store::Digest::Object/URI::NI
  # store.get should scan an S:D:O if it is not scanned (ie, no digest
  #   URIs), and complain if it can't scan (ie, if there is no content)
  # store.get should return nil if the object is not in the store
  # store.get should return a new object if present

  # (Note: It may seem weird to `get` an object you already have, but
  # the return value is an implicit verification that the store *also*
  # has it. It is like this mainly for symmetry.)
  end

  context 'removing objects' do
  # store should remove a URI::NI/Store::Digest::Object/String/IO/File/Pathname
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

  # store should work with threads
  subject do
    Store::Digest.new dir: '/tmp/test-store-digest', mapsize: 2**27
  end

  it 'should work with threads' do
    require 'thread'
    t = Thread.new do
      subject.add 'lolz'
    end
    t.join
  end

end
