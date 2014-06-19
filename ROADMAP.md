
# Roadmap: Coming Soon

* Schema Evolution : how to migrate entities and indexes across schema versions (lazy/eager)
* Backup & Recovery : bulk import & export of store content 
* Materialized Aggregations : ability to declare counters/aggregations across entities, similar to secondary indexes
* SequenceService: distributed SequenceService (a la Twitter Snowflake)


# Roadmap: Soon After...

Persistence Interfaces:

* Secondary Index (including Unique index support)
* Full-Text Search
* KV Cache (for KV and unique indexes)
* Counters

Upcoming Backends:

* ElasticSearch: KV Store, Journal Store, Full-Text indexes
* LevelDB: Sequence, KV Store, Journal Store
* Java Chronicle: Journal Store
* Sqlite3 (via jdbi): Sequence, KV Store, Journal Store
* MS SQL: Sequence, KV Store, Journal Store
* Riak: KV Store
* RocksDB: Sequence, KV Store, Journal Store
* Amazon DynamoDB: Sequence, KV Store, Journal Store

