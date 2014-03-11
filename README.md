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

* Schema Support: including structured and primitive types, JSON-compatible
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


# Sequence Service

The sequence store is an interface around identifier allocation. In a
traditional database, this would be AUTOINCREMENT. As NoSQL data stores
become more popular, folks are starting to use techniques like UUIDs
to generate globally unique identifiers.

The current Sequence Service is based on JDBI/JDBC for a single instance.
In the future, we would like to support interleaved sequences (each
sequence server gets one or more values modulo the cluster size), as well
as more natural distributed solutions (such as Twitter snowflake).

Implementors take note - since the current JDBI KV and Journal Store
have grown up "in the same neighborhood" as the sequence service, there
are bound to be some couplings (such as long id representation) that need
to be revisited over the next few releases.


# Key-Value Document Store

The Key-Value Document store is a "bare-bones as possible" document
store implementation on top of a key-value store (initially, a JDBI-based
implementation). The KV abstraction consists of 4 main operations (CREATE,
RETRIEVE, UPDATE, and DELETE), plus iteration (which will be optional
or at least more restricted as we implement more distributed data stores).

On top of this KV base we implement Schema Extraction, which is just a
fancy way of saying that developers can specify a subset of fields and
types on their JSON documents, and the data store will store those fields
in a more compact way. So if you say a field is of type DATETIME, we'll
turn it into an 8-byte long millis instead of a 20-byte timestamp string.

In addition to Schema Extraction, we also provide Binary Encoding using
SMILE (a binary representation of JSON). This means that JSON structures
are natively supported, and primitive types such as integers are even more
efficiently encoded.

Lastly, we include Compression using LZF so that larger documents enjoy
up to 30-40% compression transparently in addition to the benefits of
schema extraction and binary encoding.


# Journal Store

The Journal store is a persistence pattern that emerged as we considered
time-series data. Often time, it's important to keep a fixed amount of
this data, removing large chunks of data from the beginning of time. It
is inspired by Java Chronicle, although we haven't implemented the Java
Chronicle Journal Store just yet.

The Journal Store consists of a single operation (APPEND), plus iteration
(over entries in sequential order), plus the ability to drop a partition
or close the currently active partition.

The Journal Store is efficient for JDBI-based stores because it does not
have to support a fine-grained DELETE operation (we could actually implement
an efficient in-place soft delete, just not yet). We can support partition
DROP using TRUNCATE TABLE or DROP TABLE, allowing the underlying database
to reclaim storage efficiently. With distributed backends in the future,
this will use the most efficient bulk deletion mechanism available (KV
stores like AWS S3 and Riak don't have efficient bucket destruction APIs
just yet).








