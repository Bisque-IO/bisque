Create system to limit max concurrent tables for compaction
  - Try to constrain memory and disk to some user configurable threshold

Create table stats that get persisted in the manifest

Comprehensive internal metrics, discuss if we should store in a special internal BisqueLance raft group
  - TableEngine metrics including ingestion rate, compaction rate, query rate, total size and size time-series, 
  - Otel ingestion metrics, query metrics, etc
  - Otel metrics for each of the connector / API types

Design manifest model around multi-tenant
  - Tenant may own any number of raft groups
  - Limits for local filesystem space across all raft groups owned by a tenant
  - Limits for concurrent queries / RAM used across all raft groups owned by a tenant
  - Allow Datafusion SessionContext to be scoped to a tenant including access across all raft groups owned by a tenan
  
Dictionary for multi-tenant
  - Tenant = Logical owner of multiple raft groups
  - Catalog = Single raft group
