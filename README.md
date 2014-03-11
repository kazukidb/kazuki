# Kazuki : a harmonious data store

Kazuki is a set of data store interfaces that encapsulate
well-known persistence patterns for high performance
and/or scalable distributed solutions.

Developers often choose a NoSQL data store at the beginning
of a project because they believe that it is the only way
to acheive scalability and fault-tolerance.

Kazuki is built with the belief that the *interface* is more
important than the implementation, so developers may choose
an initial implementation that is the friendliest to *their*
environment, and migrate to other implementations as needs
arise.


# Currently Supported

Persistence Interfaces:

* Sequence Service: support for generating identifiers
* Key-Value/Document Store: a persistent KV store with schema support and optional iterators
* Journal Store: a partitioned store

Supported Backends:

* H2 database (via jdbi): Sequence, KV Store, Journal Store
* MySQL (via jdbi, in progress)

Notable Features:

* Schema Extraction: KV types may have an associated schema to reduce duplication
* Compact Binary Encoding: using SMILE (binary JSON format)
* Compression: using LZF


# Roadmap

Persistence Interfaces:

* Secondary Index
* Full-Text Search
* KV Cache (for KV and unique indexes)
* Counters

Upcoming Backends:

* LevelDB: Sequence, KV Store, Journal Store
* Java Chronicle: Journal Store
* Sqlite3 (via jdbi): Sequence, KV Store, Journal Store
* MS SQL: Sequence, KV Store, Journal Store
* Riak: KV Store
* RocksDB: Sequence, KV Store, Journal Store
* Amazon DynamoDB: Sequence, KV Store, Journal Store


