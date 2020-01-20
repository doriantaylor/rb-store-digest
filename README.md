# Store::Digest - An RFC6920-compliant content-addressable store

There are a number of content-addressable stores out there. [Git is
one](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects),
[Perkeep](https://perkeep.org/) (née CamliStore) is another. Why
another one? Well:

1. **[RFC6920](https://tools.ietf.org/html/rfc6920) URI interface:** the
   primary way you talk to this thing is through `ni:///…` URIs.
2. **Multiple digest algorithms:** every object in the store is
  identified by more than one cryptographic digest, as cryptographic
  digest algorithms tend to come and go.
3. **Network-optional:** don't run another network service when an
   embedded solution will suffice.
4. **Rudimentary metadata:** Aside from internal bookeeping
   information, store _only_ the handful of facts that would enable a
   blob to be properly rendered in a Web browser without integrating
   another metadata source, _and nothing else_.
5. **Organizational memory:** Retain a record, not only of every
   object _currently_ in the store, but also every object that has
   _ever been_ in the store.

## How to use Store::Digest

```ruby
require 'store/digest'
require 'pathname'
require 'uri/ni'

store = Store::Digest.new driver: :LMDB, dir: '/var/lib/store-digest'

objs = Pathname('~/Desktop').expand_path.glob(?*).map do |f|
  store.add f if f.file?
end
# congratulations, you have just copied all the stuff on your desktop.

# let's make an identifer

uri = URI::NI.compute 'some data'
# => #<URI::NI ni:///sha-256;EweZDmulyhRes16ZGCqb7EZTG8VN32VqYCx4D6AkDe4>

store.get uri
# => nil

# of course not because we didn't put the content in there, so let's do that:

store.add 'some data'
# => #<Store::Digest::Object:0x00007fa00d5ee3e0
#  @content=
#   #<Proc:0x00007fa00d5ee430:1 (lambda)>,
#  @ctime=2020-01-18 14:05:56 -0800,
#  @digests=
#   {:md5=>#<URI::NI ni:///md5;HlAhCgICSX-3m8OLat5sNA>,
#    :"sha-1"=>#<URI::NI ni:///sha-1;uvNFUf7LSKzD2oaOuF4bbayd41Y>,
#    :"sha-256"=>
#     #<URI::NI ni:///sha-256;EweZDmulyhRes16ZGCqb7EZTG8VN32VqYCx4D6AkDe4>,
#    :"sha-384"=>
#     #<URI::NI ni:///sha-384;qcYaFi9LVypj5rDitFrvRztzAn1ZBVWWakwJGFg3_3KhAZHBNuw_RhTXkU0dqCPw>,
#    :"sha-512"=>
#     #<URI::NI ni:///sha-512;4WRedJLwMvtixnTbdVAL57Jgv8DaqWWCHds_ikm10zeI7j8EZ0TiuVr7XD2PJQDFScqJ15_GiQiF0o4FUAdCTw>},
#  @dtime=nil,
#  @flags=0,
#  @mtime=2020-01-18 14:05:56 -0800,
#  @ptime=2020-01-18 14:05:56 -0800,
#  @size=9,
#  @type="text/plain">

store.get uri
# ...same thing...
```

The main operations are, of course, `add`, `get`, `remove`, and
`forget`. I am currently working on a `search` which will match
partial digests and metadata values.

For each object, immutable bookkeeping metadata include:

* Size (bytes)
* Added to store (timestamp)
* Metadata modified (timestamp)
* Blob deleted (timestamp if present)

User-manipulable metadata consists of:

* Modification time (timestamp)
* Content-type (MIME identifier, e.g. `text/html`)
* Language (optional [RFC5646](https://tools.ietf.org/html/rfc5646)
  token, e.g. `en-ca`)
* Character set (optional token, e.g. `utf-8`, `iso-8859-1`, `windows-1252`)
* Content-encoding (optional token, e.g. `gzip`, `deflate`)
* Flags (8-bit unsigned integer)

There are four flags, each with two bits of information:

* Content-type
* Character set
* Content-encoding
* Syntax

For each of these flags, the values 0 to 3 signify:

0. Unverified
1. Invalid
2. Recheck validation
3. Verified valid

These metadata fields are "user-manipulable" for an _extremely_ loose
definition of "user". The idea, in particular for the fields that have
associated flags, is that any initial value is a _claim_ that may be
subsequently verified, by, for example, a separate maintenance daemon
that scans newly-inserted objects and supplants their metadata with
whatever it finds. For example, one could insert a compressed file of
type `application/gzip`, and some other maintenance process could
come along and realize that in fact the file is `image/svg+xml` with
an _encoding_ of `gzip`, but there is also a syntax error, so it
should not be served without first attempting to repair it. Because of
the enormous combination of types, encodings, and syntaxes, such a
maintenance daemon is way out of scope for this project, despite being
a desirable and likely future addition to it.

## API Documentation

Generated and deposited
[in the usual place](http://www.rubydoc.info/github/doriantaylor/rb-store-digest/master).

## Design

This package is intended to be the simplest possible substrate for
managing opaque data segments in terms of their contents. The central
ambition is to solidify a consistent interface for all the desired
behaviour, and then subsequently expand that interface to different
languages and platforms, including, when possible, adapters to
existing content-addressable storage platforms.

This version is written in Ruby, and is the maturation of [an earlier
version written in Perl](https://metacpan.org/pod/Store::Digest).
Whereas the Perl implementation uses [Berkeley
DB](https://www.oracle.com/database/berkeley-db/) to store its
metadata, this version uses [LMDB](https://symas.com/lmdb/). Indeed, a
subsidiary goal of this project is to define a _pattern_ for
lower-level database storage, and key-value stores in particular, such
that complying digest store interfaces running on different
programming languages can attach to the _same_ storage repository.

The current implementation uses the file system store its blobs. It
does so by taking the primary digest algorithm (`sha-256` by default)
of the given blob, encoding it as base-32 (to be case-insensitive),
and transforming the first few bits into a hashed directory structure
(similar to Git, though it uses hexadecimal encoding). Internally, the
blob and metadata subsystems are decoupled, such that the two can be
mixed and matched.

### Basic Architecture

`Store::Digest` proper is a unified interface over a `Driver` which
provides the concrete implementation. A driver may be further
decoupled (as is the case with `LMDB`) into `Blob` and `Meta`
subcomponents, which themselves may share `Trait`s (like storing its
state in a `RootDir`). We can imagine this bifurcation not being
universal, e.g. a prospective PostgreSQL driver could handle both
blobs _and_ metadata within its own confines.

> I have yet to decide on the final layout of this system, so don't
> get too used to it.

### A note on storage efficiency

Unlike Git, which uses `pigz` (or rather, `deflate`) to compress its
contents (along with a small amount of embedded metadata), objects in
this system are stored as-is. This is deliberate: if you want to
compress your objects, you should compress them _before_ adding them
to the store, and signal the fact that they are compressed in the
metadata.

I have also elected to have the system designate a "primary" digest
algorithm, which defaults to `sha-256`. Its binary representation is
32 bytes long. By all accounts, this is way too big for an internal
identifier. This is how the original Perl version worked, and likely
how I plan to keep it until I can work out the additional complexity
of a shorter identifier (like an unsigned integer) which is guaranteed
to be unique (namely, recycling discarded identifiers). For the time
being however I am not worried, as storage is large and computers are
fast.

### Issues with hash collisions

MD5 and SHA-1 are both considered unsafe for the purposes of
cryptographic integrity, since collision attacks have been
demonstrated against both algorithms. (This is why neither is
recommended as a primary key.)

Currently, this system does not support duplicate mappings from one
digest algorithm to another. If two different blobs with the same hash
are entered into the store, the second one will probably be ignored. I
will probably change this eventually so smaller hashes can accommodate
duplicate entries, as dumping crafted data into this system would
likely be a convenient way to identify collision targets.

### Future directions

The first order of business is to create a search function for the
various metadata elements. Then, probably a Rack app to put this whole
business online (much like the analogous one I wrote in Perl).
Following that, probably start poking around at that maintenance
daemon I mentioned in the other section, and whatever else I think is
a good idea and not too much effort.

Afterward, I will probably take the show on the road: write a version
in Python and/or JavaScript, for example. Maybe look at other
back-ends. We'll see.

## Installation

When it is ready, you know how to do this:

    $ gem install store-digest

You will know when it's far enough along when you can [download it off
rubygems.org](https://rubygems.org/gems/store-digest).

## Contributing

Bug reports and pull requests are welcome at
[the GitHub repository](https://github.com/doriantaylor/rb-store-digest/issues).

## Copyright & License

©2019 [Dorian Taylor](https://doriantaylor.com/)

This software is provided under
the [Apache License, 2.0](https://www.apache.org/licenses/LICENSE-2.0).
