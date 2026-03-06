Create system to limit max concurrent tables for compaction
  - Try to constrain memory and disk to some user configurable threshold

Create table stats that get persisted in the manifest

Comprehensive internal metrics, discuss if we should store in a special internal BisqueLance raft group
  - TableEngine metrics including ingestion rate, compaction rate, query rate, total size and size time-series, 
  - Otel ingestion metrics, query metrics, etc
  - Otel metrics for each of the connector / API types

Design overall bisque model around multi-tenant. It is important to point out that bisque-lance is just one module built on top of bisque-raft. There will be more modules like bisque-mq for message queues and kafka like commit logs. There will be bisque-libsql for SQLite databases and more. Any module may have 1 or more raft groups. Each raft group has 1 module.
  - Tenant may own any number of raft groups
  - Limits for local filesystem space across all raft groups owned by a tenant
  - Limits for concurrent queries / RAM used across all raft groups owned by a tenant
  - Allow Datafusion SessionContext to be scoped to a tenant including access across all raft groups owned by a tenant

  We need to specify terms that make sense. A tenant can own multiple raft groups and each raft group runs 1 bisque-raft module like bisque-lance, bisque-mq, bisque-libsql, etc. We should name the raft group something meaningful like a catalog or database or something else?
What do you think of this overall design?
  
Dictionary for multi-tenant
  - Tenant = Logical owner of multiple raft groups
  - Catalog = Single raft group

  
Version pinning for bisque-lance must be cluster-wide since HTTP may route to any node in the cluster. In addition the bisque client websocket may connect to any node in the cluster including follower nodes and not just the leader. Otherwise, a tenant could be pinned to a specific node and not have access to other nodes in the cluster but that makes S3 http requests more difficult I think. What do you think?
