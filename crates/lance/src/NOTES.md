Create system to limit max concurrent tables for compaction
  - Try to constrain memory and disk to some user configurable threshold

Metrics Specialized Tables
  - Histogram
  - Counter
  - Gauge

Logging Table Type
  - Store application logs
  - Flexible schema to support any log fields
  - Full-text search support
  
Aggressive Batching for ingestion via crossfire queues
  - This allows for high throughput ingestion that could support telemetry data
  
Add datafusion-postgres to allow postgres wire protocol access
  - Support basic DDL
  - Full support for SQL queries including both read and write operations
