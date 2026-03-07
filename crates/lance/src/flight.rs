//! Arrow Flight SQL service layer for bisque-lance.
//!
//! Implements both **Arrow Flight** and **Arrow Flight SQL** protocols,
//! giving clients multiple ways to interact with the storage engine:
//!
//! | Client | Protocol | Example |
//! |--------|----------|---------|
//! | Python `pyarrow.flight` | Plain Flight | `do_put`, `do_get` |
//! | Python `adbc_driver_flightsql` | Flight SQL / ADBC | Automatic |
//! | DBeaver / JDBC | Flight SQL | SQL queries |
//! | Go / C++ `flightsql` | Flight SQL | SQL queries |
//! | Custom app | Plain Flight | Bulk writes |
//!
//! # Flight SQL operations
//!
//! | Flight SQL command | bisque-lance operation |
//! |--------------------|------------------------|
//! | `CommandStatementQuery` | DataFusion SQL → stream results |
//! | `CommandStatementUpdate` | DELETE/UPDATE via Raft; DDL via DataFusion |
//! | `CommandStatementIngest` | Bulk write batches to a table |
//! | `CommandGetTables` | List tables with schemas |
//! | `CommandGetCatalogs` | Returns single "bisque" catalog |
//! | `CommandGetDbSchemas` | Returns single "public" schema |
//!
//! # Custom actions (plain Flight)
//!
//! | Action type | Body | Description |
//! |-------------|------|-------------|
//! | `create_table` | `u16 name_len + name + IPC schema` | Create a table |
//! | `drop_table` | UTF-8 table name | Drop a table |

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::GetTablesBuilder;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandStatementIngest,
    CommandStatementQuery, CommandStatementUpdate, DoPutPreparedStatementResult,
    ProstMessageExt as _, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use futures::{StreamExt, TryStreamExt};
use prost::Message as _;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::ipc;
use crate::postgres::BisqueLanceCatalogProvider;
use crate::raft::{LanceRaftNode, WriteError};

/// Default timeout for waiting on the read fence.
const READ_FENCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// gRPC metadata key for read-after-write fencing.
///
/// Clients pass the `log_index` from their [`WriteResult`] in this header.
/// The query handler waits until Lance has materialized at least this far
/// before executing the query.
const READ_FENCE_HEADER: &str = "x-bisque-min-log-id";

/// Arrow Flight SQL service backed by a bisque-lance Raft node.
///
/// Implements [`FlightSqlService`] which provides a blanket [`FlightService`]
/// implementation. This means the service supports both:
/// - **Flight SQL** — standardized SQL protocol (ADBC, JDBC, DBeaver)
/// - **Plain Flight** — custom actions for DDL, bulk writes via `do_put_fallback`
///
/// Write operations are routed through Raft consensus via [`LanceRaftNode`].
/// Read operations are served directly from the local engine.
pub struct BisqueFlightService {
    raft_node: Arc<LanceRaftNode>,
    /// Cached session context with a dynamic catalog that resolves tables
    /// from the live engine — no per-query rebuild needed.
    ctx: Arc<SessionContext>,
}

impl BisqueFlightService {
    /// Create a new Flight SQL service wrapping the given Raft node.
    pub fn new(raft_node: Arc<LanceRaftNode>) -> Self {
        let engine = raft_node.engine().clone();

        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema("bisque", "public")
            .with_create_default_catalog_and_schema(false);
        let ctx = SessionContext::new_with_config(session_config);

        let catalog = Arc::new(BisqueLanceCatalogProvider::new(engine));
        ctx.register_catalog("bisque", catalog);

        Self {
            raft_node,
            ctx: Arc::new(ctx),
        }
    }

    /// Get a reference to the underlying Raft node.
    pub fn raft_node(&self) -> &Arc<LanceRaftNode> {
        &self.raft_node
    }

    /// Build a [`FlightServiceServer`] from this service, ready to be added to a tonic router.
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Wait for the Lance materialization watermark to reach the specified
    /// log index (read-after-write fence). No-op if async apply is not enabled
    /// or if no fence header is present.
    async fn wait_for_read_fence<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let min_log_id = match request.metadata().get(READ_FENCE_HEADER) {
            Some(val) => {
                let s = val
                    .to_str()
                    .map_err(|_| Status::invalid_argument("x-bisque-min-log-id must be ASCII"))?;
                s.parse::<u64>()
                    .map_err(|_| Status::invalid_argument("x-bisque-min-log-id must be a u64"))?
            }
            None => return Ok(()),
        };

        if min_log_id == 0 {
            return Ok(());
        }

        if let Some(watermark) = self.raft_node.applied_watermark() {
            if !watermark.wait_for(min_log_id, READ_FENCE_TIMEOUT).await {
                warn!(
                    min_log_id,
                    current = watermark.current_index(),
                    "read fence timeout waiting for Lance materialization"
                );
                return Err(Status::deadline_exceeded(format!(
                    "timed out waiting for log index {} to be materialized (current: {})",
                    min_log_id,
                    watermark.current_index()
                )));
            }
            debug!(min_log_id, "read fence satisfied");
        }
        // If no watermark (sync mode), data is immediately visible.
        Ok(())
    }

    /// Execute a SQL query and return the result schema + batches.
    async fn execute_sql(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {e}")))?;

        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("execution error: {e}")))?;

        Ok((schema, batches))
    }
}

/// Convert a [`WriteError`] to a tonic [`Status`].
fn write_error_to_status(err: WriteError) -> Status {
    match err {
        WriteError::NotLeader {
            leader_id,
            leader_node: _,
        } => Status::unavailable(format!(
            "not leader; leader_id={}",
            leader_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )),
        WriteError::Encode(msg) => Status::invalid_argument(format!("encode error: {msg}")),
        WriteError::Raft(msg) => Status::internal(format!("raft error: {msg}")),
        WriteError::Fatal(msg) => Status::internal(format!("fatal raft error: {msg}")),
    }
}

/// Decode the body of a `create_table` custom action.
///
/// Format: `u16 BE name_len` + `name bytes` + `IPC schema bytes`
fn decode_create_table_action(body: &[u8]) -> Result<(String, Schema), Status> {
    if body.len() < 2 {
        return Err(Status::invalid_argument(
            "create_table action body too short",
        ));
    }
    let name_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + name_len {
        return Err(Status::invalid_argument(
            "create_table action body truncated (name)",
        ));
    }
    let name = std::str::from_utf8(&body[2..2 + name_len])
        .map_err(|_| Status::invalid_argument("table name is not valid UTF-8"))?
        .to_string();
    let schema_bytes = &body[2 + name_len..];
    let schema = ipc::schema_from_ipc(schema_bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid schema IPC: {e}")))?;
    Ok((name, schema))
}

/// Decode the body of a `delete_records` custom action.
///
/// Format: `u16 BE name_len` + `name bytes` + `UTF-8 filter string`
fn decode_delete_action(body: &[u8]) -> Result<(String, String), Status> {
    if body.len() < 2 {
        return Err(Status::invalid_argument(
            "delete_records action body too short",
        ));
    }
    let name_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + name_len {
        return Err(Status::invalid_argument(
            "delete_records action body truncated (name)",
        ));
    }
    let name = std::str::from_utf8(&body[2..2 + name_len])
        .map_err(|_| Status::invalid_argument("table name is not valid UTF-8"))?
        .to_string();
    let filter = std::str::from_utf8(&body[2 + name_len..])
        .map_err(|_| Status::invalid_argument("filter is not valid UTF-8"))?
        .to_string();
    Ok((name, filter))
}

/// Decode the body of an `update_records` custom action.
///
/// Format: `u16 BE name_len` + `name bytes` + `u32 BE filter_len` + `filter bytes` + `IPC data`
fn decode_update_action(body: &[u8]) -> Result<(String, String, bytes::Bytes), Status> {
    if body.len() < 2 {
        return Err(Status::invalid_argument(
            "update_records action body too short",
        ));
    }
    let name_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + name_len + 4 {
        return Err(Status::invalid_argument(
            "update_records action body truncated (name or filter_len)",
        ));
    }
    let name = std::str::from_utf8(&body[2..2 + name_len])
        .map_err(|_| Status::invalid_argument("table name is not valid UTF-8"))?
        .to_string();
    let filter_offset = 2 + name_len;
    let filter_len = u32::from_be_bytes([
        body[filter_offset],
        body[filter_offset + 1],
        body[filter_offset + 2],
        body[filter_offset + 3],
    ]) as usize;
    let filter_start = filter_offset + 4;
    if body.len() < filter_start + filter_len {
        return Err(Status::invalid_argument(
            "update_records action body truncated (filter)",
        ));
    }
    let filter = std::str::from_utf8(&body[filter_start..filter_start + filter_len])
        .map_err(|_| Status::invalid_argument("filter is not valid UTF-8"))?
        .to_string();
    let data = bytes::Bytes::copy_from_slice(&body[filter_start + filter_len..]);
    Ok((name, filter, data))
}

// =============================================================================
// SQL DELETE / UPDATE parsing helpers
// =============================================================================

/// Parse a SQL DELETE statement into (table_name, filter_predicate).
///
/// Supports: `DELETE FROM [schema.]table WHERE predicate`
///
/// Returns `None` if the SQL is not a DELETE statement.
fn parse_delete_sql(sql: &str) -> Option<(String, String)> {
    // Case-insensitive check for DELETE FROM
    let upper = sql.to_uppercase();
    if !upper.starts_with("DELETE") {
        return None;
    }

    // Skip "DELETE" and optional whitespace
    let rest = sql["DELETE".len()..].trim_start();

    // Expect "FROM"
    let upper_rest = rest.to_uppercase();
    if !upper_rest.starts_with("FROM") {
        return None;
    }
    let rest = rest["FROM".len()..].trim_start();

    // Find WHERE clause
    let upper_rest = rest.to_uppercase();
    let where_pos = upper_rest.find("WHERE")?;

    let table = rest[..where_pos].trim().trim_matches('"');
    // Strip optional schema prefix (e.g. "public.my_table" → "my_table")
    let table = table.rsplit('.').next().unwrap_or(table).trim_matches('"');
    let filter = rest[where_pos + "WHERE".len()..].trim();

    // Strip trailing semicolon
    let filter = filter.strip_suffix(';').unwrap_or(filter).trim();

    if table.is_empty() || filter.is_empty() {
        return None;
    }

    Some((table.to_string(), filter.to_string()))
}

/// Parse a SQL UPDATE statement into (table_name, set_assignments, filter_predicate).
///
/// Supports: `UPDATE [schema.]table SET col1=val1, col2=val2 WHERE predicate`
///
/// Returns `None` if the SQL is not an UPDATE statement.
fn parse_update_sql(sql: &str) -> Option<(String, String, String)> {
    let upper = sql.to_uppercase();
    if !upper.starts_with("UPDATE") {
        return None;
    }

    let rest = sql["UPDATE".len()..].trim_start();

    // Find SET keyword
    let upper_rest = rest.to_uppercase();
    let set_pos = upper_rest.find("SET")?;

    let table = rest[..set_pos].trim().trim_matches('"');
    let table = table.rsplit('.').next().unwrap_or(table).trim_matches('"');
    let rest = rest[set_pos + "SET".len()..].trim_start();

    // Find WHERE keyword
    let upper_rest = rest.to_uppercase();
    let where_pos = upper_rest.find("WHERE")?;

    let assignments = rest[..where_pos].trim();
    let filter = rest[where_pos + "WHERE".len()..].trim();

    // Strip trailing semicolon
    let filter = filter.strip_suffix(';').unwrap_or(filter).trim();
    let assignments = assignments.strip_suffix(',').unwrap_or(assignments).trim();

    if table.is_empty() || assignments.is_empty() || filter.is_empty() {
        return None;
    }

    Some((
        table.to_string(),
        assignments.to_string(),
        filter.to_string(),
    ))
}

/// Build a SELECT query that produces updated rows for an UPDATE statement.
///
/// Given `UPDATE t SET col1=expr1, col2=expr2 WHERE pred`, produces:
/// `SELECT expr1 AS col1, expr2 AS col2, <other_cols> FROM t WHERE pred`
///
/// This lets DataFusion evaluate the SET expressions and produce replacement
/// RecordBatches that can be written back through Raft.
fn build_update_select_sql(table: &str, assignments: &str, filter: &str) -> String {
    // Parse assignments into (col, expr) pairs
    let mut set_cols: Vec<(&str, &str)> = Vec::new();
    for assignment in assignments.split(',') {
        if let Some((col, expr)) = assignment.split_once('=') {
            set_cols.push((col.trim(), expr.trim()));
        }
    }

    // Build SELECT with SET expressions aliased to their column names,
    // plus a wildcard EXCLUDE for untouched columns.
    // DataFusion supports EXCLUDE syntax for this pattern.
    let set_exprs: Vec<String> = set_cols
        .iter()
        .map(|(col, expr)| format!("{expr} AS {col}"))
        .collect();
    let exclude_cols: Vec<&str> = set_cols.iter().map(|(col, _)| *col).collect();

    // Use EXCLUDE to get all non-modified columns, then add modified ones
    format!(
        "SELECT * EXCLUDE ({excludes}), {sets} FROM {table} WHERE {filter}",
        excludes = exclude_cols.join(", "),
        sets = set_exprs.join(", "),
        table = table,
        filter = filter,
    )
}

// =============================================================================
// FlightSqlService implementation
// =============================================================================

#[tonic::async_trait]
impl FlightSqlService for BisqueFlightService {
    type FlightService = BisqueFlightService;

    // =========================================================================
    // Handshake — accept without auth for now
    // =========================================================================

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::new(),
        };
        let stream = futures::stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    // =========================================================================
    // SQL Queries — get_flight_info_statement + do_get_statement
    // =========================================================================

    /// Handle `CommandStatementQuery` — return FlightInfo describing the query result.
    ///
    /// We embed the SQL string directly in the statement handle so that
    /// `do_get_statement` can execute it without server-side state.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(sql = %query.query, "get_flight_info_statement");

        self.wait_for_read_fence(&request).await?;
        let (schema, _batches) = self.execute_sql(&query.query).await?;

        // Create a ticket that embeds the SQL query for do_get_statement.
        let ticket = TicketStatementQuery {
            statement_handle: query.query.clone().into(),
        };
        let ticket_bytes = ticket.as_any().encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes)))
            .with_descriptor(FlightDescriptor::new_cmd(query.query));

        Ok(Response::new(info))
    }

    /// Handle `TicketStatementQuery` — execute the SQL and stream results.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let query_start = std::time::Instant::now();
        let sql = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|_| Status::invalid_argument("statement handle is not valid UTF-8 SQL"))?;

        debug!(sql = %sql, "do_get_statement");

        self.wait_for_read_fence(&request).await?;

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {e}")))?;

        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

        let batch_stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("execution error: {e}")))?
            .map_err(|e| arrow_flight::error::FlightError::Arrow(e.into()));

        metrics::counter!("bisque_requests_total", "protocol" => "flight", "op" => "query")
            .increment(1);
        metrics::histogram!("bisque_request_latency_seconds", "protocol" => "flight", "op" => "query").record(query_start.elapsed().as_secs_f64());

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        let response_stream = flight_stream.map(|result| {
            result.map_err(|e| Status::internal(format!("flight encode error: {e}")))
        });

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // SQL DML/DDL — do_put_statement_update
    // =========================================================================

    /// Handle `CommandStatementUpdate` — execute DDL/DML SQL statements.
    ///
    /// DELETE and UPDATE statements are routed through Raft consensus so that
    /// mutations are replicated to all nodes. Other DDL/DML (CREATE TABLE,
    /// DROP TABLE, etc.) is executed directly via DataFusion.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = &ticket.query;
        debug!(sql = %sql, "do_put_statement_update");

        let trimmed = sql.trim();

        // Route DELETE statements through Raft consensus.
        if let Some((table, filter)) = parse_delete_sql(trimmed) {
            let result = self
                .raft_node
                .delete_records(&table, &filter)
                .await
                .map_err(write_error_to_status)?;
            return match result.response {
                crate::types::LanceResponse::RowsAffected(n) => Ok(n as i64),
                crate::types::LanceResponse::Error(e) => {
                    Err(Status::internal(format!("delete failed: {e}")))
                }
                _ => Ok(0),
            };
        }

        // Route UPDATE statements through Raft consensus.
        //
        // SQL UPDATE requires evaluating SET expressions which needs query
        // planning. We use DataFusion to plan a SELECT of the replacement
        // rows (applying the SET transformations), then route the result
        // through Raft as a delete + append.
        if let Some((table, assignments, filter)) = parse_update_sql(trimmed) {
            // Build a SELECT that produces the updated rows.
            let select_sql = build_update_select_sql(&table, &assignments, &filter);
            let (_schema, batches) = self.execute_sql(&select_sql).await?;

            if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
                return Ok(0);
            }

            let result = self
                .raft_node
                .update_records(&table, &filter, &batches)
                .await
                .map_err(write_error_to_status)?;
            return match result.response {
                crate::types::LanceResponse::RowsAffected(n) => Ok(n as i64),
                crate::types::LanceResponse::Error(e) => {
                    Err(Status::internal(format!("update failed: {e}")))
                }
                _ => Ok(0),
            };
        }

        // All other DDL/DML — execute directly via DataFusion.
        let (_schema, batches) = self.execute_sql(sql).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(rows as i64)
    }

    // =========================================================================
    // Bulk Ingestion — do_put_statement_ingest (Flight SQL standard)
    // =========================================================================

    /// Handle `CommandStatementIngest` — bulk write batches to a table.
    ///
    /// This is the Flight SQL standard way to ingest data. The `table` field
    /// specifies the target table name. Record batches are decoded from the
    /// streaming FlightData and written through Raft consensus.
    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let table_name = &ticket.table;
        debug!(table = %table_name, "do_put_statement_ingest");

        // Verify the table exists.
        let engine = self.raft_node.engine();
        if !engine.has_table(table_name) {
            return Err(Status::not_found(format!("table not found: {table_name}")));
        }

        // Decode the incoming FlightData stream into RecordBatches.
        let stream = request.into_inner();
        let record_stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let batches: Vec<RecordBatch> = record_stream
            .try_collect()
            .await
            .map_err(|e| Status::invalid_argument(format!("failed to decode batches: {e}")))?;

        if batches.is_empty() {
            return Ok(0);
        }

        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        self.raft_node
            .write_records(table_name, &batches)
            .await
            .map_err(write_error_to_status)?;

        metrics::counter!("bisque_requests_total", "protocol" => "flight", "op" => "ingest")
            .increment(1);
        debug!(table = %table_name, rows = num_rows, "do_put_statement_ingest: complete");
        Ok(num_rows as i64)
    }

    // =========================================================================
    // Metadata: GetTables
    // =========================================================================

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let builder = GetTablesBuilder::new(
            query.catalog.as_deref(),
            query.db_schema_filter_pattern.as_deref(),
            query.table_name_filter_pattern.as_deref(),
            query.table_types.iter().map(|s| s.as_str()),
            query.include_schema,
        );

        let schema = builder.schema();
        let flight_descriptor = request.into_inner();

        let ticket = Ticket::new(query.as_any().encode_to_vec());

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(flight_descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let engine = self.raft_node.engine();

        let mut builder = GetTablesBuilder::new(
            query.catalog.as_deref(),
            query.db_schema_filter_pattern.as_deref(),
            query.table_name_filter_pattern.as_deref(),
            query.table_types.iter().map(|s| s.as_str()),
            query.include_schema,
        );

        for table_name in engine.list_tables() {
            let table = match engine.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };
            let schema = match table.schema().await {
                Some(s) => s,
                None => continue,
            };

            builder
                .append("bisque", "public", &table_name, "TABLE", &schema)
                .map_err(|e| Status::internal(format!("table builder error: {e}")))?;
        }

        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("table builder build error: {e}")))?;
        let schema = batch.schema();

        let batch_stream =
            futures::stream::once(async { Ok::<_, arrow_flight::error::FlightError>(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);
        let response_stream = flight_stream
            .map(|r| r.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // Metadata: GetCatalogs
    // =========================================================================

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(flight_descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["bisque"]))],
        )
        .map_err(|e| Status::internal(format!("batch error: {e}")))?;

        let batch_stream =
            futures::stream::once(async { Ok::<_, arrow_flight::error::FlightError>(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);
        let response_stream = flight_stream
            .map(|r| r.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // Metadata: GetDbSchemas
    // =========================================================================

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]);
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(flight_descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["bisque"])),
                Arc::new(arrow_array::StringArray::from(vec!["public"])),
            ],
        )
        .map_err(|e| Status::internal(format!("batch error: {e}")))?;

        let batch_stream =
            futures::stream::once(async { Ok::<_, arrow_flight::error::FlightError>(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);
        let response_stream = flight_stream
            .map(|r| r.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // Metadata: GetTableTypes
    // =========================================================================

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(flight_descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["TABLE"]))],
        )
        .map_err(|e| Status::internal(format!("batch error: {e}")))?;

        let batch_stream =
            futures::stream::once(async { Ok::<_, arrow_flight::error::FlightError>(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);
        let response_stream = flight_stream
            .map(|r| r.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // Metadata: GetSqlInfo
    // =========================================================================

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Build a minimal FlightInfo with the sql info schema.
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());

        // SqlInfo responses use a specific schema defined by the protocol.
        let schema = Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new("value", DataType::Utf8, true),
        ]);

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(flight_descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Return minimal SQL info.
        let schema = Arc::new(Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::UInt32Array::from(vec![
                    SqlInfo::FlightSqlServerName as u32,
                    SqlInfo::FlightSqlServerVersion as u32,
                ])),
                Arc::new(arrow_array::StringArray::from(vec![
                    "bisque-lance",
                    env!("CARGO_PKG_VERSION"),
                ])),
            ],
        )
        .map_err(|e| Status::internal(format!("batch error: {e}")))?;

        let batch_stream =
            futures::stream::once(async { Ok::<_, arrow_flight::error::FlightError>(batch) });
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);
        let response_stream = flight_stream
            .map(|r| r.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // Prepared statements (minimal support)
    // =========================================================================

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        debug!(sql = %query.query, "creating prepared statement");

        // Execute the query to get the result schema, but don't persist results.
        let (schema, _batches) = self.execute_sql(&query.query).await?;

        let options = IpcWriteOptions::default();
        let ipc_msg: arrow_flight::IpcMessage =
            SchemaAsIpc::new(&schema, &options).try_into().map_err(
                |e: arrow_schema::ArrowError| Status::internal(format!("schema encode error: {e}")),
            )?;

        Ok(ActionCreatePreparedStatementResult {
            // Embed the SQL query as the handle — stateless prepared statements.
            prepared_statement_handle: query.query.into(),
            dataset_schema: ipc_msg.0,
            parameter_schema: bytes::Bytes::new(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        // Stateless — nothing to close.
        Ok(())
    }

    async fn do_get_prepared_statement(
        &self,
        query: arrow_flight::sql::CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = std::str::from_utf8(&query.prepared_statement_handle).map_err(|_| {
            Status::invalid_argument("prepared statement handle is not valid UTF-8")
        })?;

        debug!(sql = %sql, "do_get_prepared_statement");

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {e}")))?;

        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

        let batch_stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("execution error: {e}")))?
            .map_err(|e| arrow_flight::error::FlightError::Arrow(e.into()));

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        let response_stream = flight_stream
            .map(|r| r.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: arrow_flight::sql::CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = std::str::from_utf8(&query.prepared_statement_handle).map_err(|_| {
            Status::invalid_argument("prepared statement handle is not valid UTF-8")
        })?;

        let (schema, _) = self.execute_sql(sql).await?;

        let ticket_bytes = query.as_any().encode_to_vec();

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes)));

        Ok(Response::new(info))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: arrow_flight::sql::CommandPreparedStatementQuery,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // Parameter binding — return handle unchanged (stateless).
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: None,
        })
    }

    // =========================================================================
    // Custom actions: create_table, drop_table (plain Flight)
    // =========================================================================

    /// Handle custom action types that aren't part of the Flight SQL standard.
    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<
        Response<Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>>,
        Status,
    > {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "create_table" => {
                let (name, schema) = decode_create_table_action(&action.body)?;
                debug!(table = %name, "do_action: creating table");

                self.raft_node
                    .create_table(&name, &schema)
                    .await
                    .map_err(write_error_to_status)?;

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "drop_table" => {
                let name = std::str::from_utf8(&action.body)
                    .map_err(|_| Status::invalid_argument("table name is not valid UTF-8"))?;
                debug!(table = %name, "do_action: dropping table");

                self.raft_node
                    .drop_table(name)
                    .await
                    .map_err(write_error_to_status)?;

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "delete_records" => {
                let (name, filter) = decode_delete_action(&action.body)?;
                debug!(table = %name, filter = %filter, "do_action: deleting records");

                self.raft_node
                    .delete_records(&name, &filter)
                    .await
                    .map_err(write_error_to_status)?;

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "update_records" => {
                let (name, filter, ipc_data) = decode_update_action(&action.body)?;
                debug!(table = %name, filter = %filter, "do_action: updating records");

                let batches = ipc::decode_record_batches(&ipc_data).map_err(|e| {
                    Status::invalid_argument(format!("failed to decode IPC data: {e}"))
                })?;

                self.raft_node
                    .update_records(&name, &filter, &batches)
                    .await
                    .map_err(write_error_to_status)?;

                let result = arrow_flight::Result {
                    body: bytes::Bytes::from_static(b"ok"),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            other => Err(Status::invalid_argument(format!(
                "unknown action type: {other}"
            ))),
        }
    }

    /// Advertise custom actions alongside the standard Flight SQL ones.
    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        Some(vec![
            Ok(ActionType {
                r#type: "create_table".to_string(),
                description: "Create a new table. Body: u16 name_len + name + IPC schema"
                    .to_string(),
            }),
            Ok(ActionType {
                r#type: "drop_table".to_string(),
                description: "Drop a table. Body: UTF-8 table name".to_string(),
            }),
            Ok(ActionType {
                r#type: "delete_records".to_string(),
                description:
                    "Delete rows matching a filter. Body: u16 name_len + name + UTF-8 filter"
                        .to_string(),
            }),
            Ok(ActionType {
                r#type: "update_records".to_string(),
                description: "Update rows: delete matching + append replacements. Body: u16 name_len + name + u32 filter_len + filter + IPC data".to_string(),
            }),
        ])
    }

    // =========================================================================
    // SqlInfo registration (required by trait)
    // =========================================================================

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

// =============================================================================
// Server startup helper
// =============================================================================

/// Start a Flight SQL gRPC server.
///
/// The server supports both Flight SQL (for ADBC/JDBC/DBeaver clients) and
/// plain Flight (for custom `do_put` writes and DDL actions).
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use bisque_lance::flight::serve_flight;
/// # async fn run(raft_node: Arc<bisque_lance::LanceRaftNode>) {
/// let addr = "0.0.0.0:50051".parse().unwrap();
/// serve_flight(raft_node, addr).await.unwrap();
/// # }
/// ```
pub async fn serve_flight(
    raft_node: Arc<LanceRaftNode>,
    addr: std::net::SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let service = BisqueFlightService::new(raft_node);
    Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    /// Build a valid `create_table` action body: u16 BE name_len + name + IPC schema.
    fn build_create_table_body(name: &str, schema: &Schema) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len() as u16;
        let schema_ipc = ipc::schema_to_ipc(schema).expect("schema_to_ipc");

        let mut body = Vec::new();
        body.extend_from_slice(&name_len.to_be_bytes());
        body.extend_from_slice(name_bytes);
        body.extend_from_slice(&schema_ipc);
        body
    }

    #[test]
    fn test_decode_create_table_action_valid() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]);
        let body = build_create_table_body("my_table", &schema);

        let (name, decoded_schema) = decode_create_table_action(&body).unwrap();
        assert_eq!(name, "my_table");
        assert_eq!(decoded_schema.fields().len(), 2);
        assert_eq!(decoded_schema.field(0).name(), "id");
        assert_eq!(decoded_schema.field(1).name(), "value");
    }

    #[test]
    fn test_decode_create_table_action_body_too_short() {
        // Empty body
        let result = decode_create_table_action(&[]);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("too short"));

        // Single byte
        let result = decode_create_table_action(&[0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_create_table_action_truncated_name() {
        // name_len = 100 but only 2 bytes of body after the length
        let mut body = Vec::new();
        body.extend_from_slice(&100u16.to_be_bytes());
        body.extend_from_slice(b"ab");

        let result = decode_create_table_action(&body);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("truncated"));
    }

    #[test]
    fn test_decode_create_table_action_invalid_utf8_name() {
        // name_len = 2, name bytes are invalid UTF-8
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8
        // Add some dummy schema bytes (will never be reached)
        body.extend_from_slice(&[0x00; 10]);

        let result = decode_create_table_action(&body);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("UTF-8"));
    }

    #[test]
    fn test_write_error_to_status_not_leader() {
        let err = WriteError::NotLeader {
            leader_id: Some(5),
            leader_node: None,
        };
        let status = write_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("not leader"));
        assert!(status.message().contains("5"));

        // With unknown leader
        let err_unknown = WriteError::NotLeader {
            leader_id: None,
            leader_node: None,
        };
        let status_unknown = write_error_to_status(err_unknown);
        assert_eq!(status_unknown.code(), tonic::Code::Unavailable);
        assert!(status_unknown.message().contains("unknown"));
    }

    #[test]
    fn test_write_error_to_status_encode() {
        let err = WriteError::Encode("bad data".to_string());
        let status = write_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("encode error"));
    }

    #[test]
    fn test_write_error_to_status_raft() {
        let err = WriteError::Raft("consensus failed".to_string());
        let status = write_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("raft error"));
    }

    #[test]
    fn test_write_error_to_status_fatal() {
        let err = WriteError::Fatal("node crashed".to_string());
        let status = write_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("fatal raft error"));
    }

    // ---- decode_delete_action tests ----

    #[test]
    fn test_decode_delete_action_valid() {
        let name = "events";
        let filter = "id > 100";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(filter.as_bytes());

        let (decoded_name, decoded_filter) = decode_delete_action(&body).unwrap();
        assert_eq!(decoded_name, "events");
        assert_eq!(decoded_filter, "id > 100");
    }

    #[test]
    fn test_decode_delete_action_empty_filter() {
        let name = "t";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        // No filter bytes = empty filter

        let (decoded_name, decoded_filter) = decode_delete_action(&body).unwrap();
        assert_eq!(decoded_name, "t");
        assert_eq!(decoded_filter, "");
    }

    #[test]
    fn test_decode_delete_action_body_too_short() {
        let result = decode_delete_action(&[]);
        assert!(result.is_err());
        let result = decode_delete_action(&[0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_delete_action_truncated_name() {
        // Says name is 10 bytes but body only has 3 bytes after the length prefix
        let body = vec![0x00, 0x0A, b'a', b'b', b'c'];
        let result = decode_delete_action(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_delete_action_invalid_utf8_name() {
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        body.extend_from_slice(b"filter");
        let result = decode_delete_action(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_delete_action_invalid_utf8_filter() {
        let name = "t";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8 filter
        let result = decode_delete_action(&body);
        assert!(result.is_err());
    }

    // ---- decode_update_action tests ----

    #[test]
    fn test_decode_update_action_valid() {
        let name = "metrics";
        let filter = "x > 0";
        let ipc_data = vec![1u8, 2, 3, 4, 5];

        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&(filter.len() as u32).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        body.extend_from_slice(&ipc_data);

        let (decoded_name, decoded_filter, decoded_data) = decode_update_action(&body).unwrap();
        assert_eq!(decoded_name, "metrics");
        assert_eq!(decoded_filter, "x > 0");
        assert_eq!(&decoded_data[..], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_decode_update_action_empty_data() {
        let name = "t";
        let filter = "id = 1";

        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&(filter.len() as u32).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        // No additional data

        let (decoded_name, decoded_filter, decoded_data) = decode_update_action(&body).unwrap();
        assert_eq!(decoded_name, "t");
        assert_eq!(decoded_filter, "id = 1");
        assert!(decoded_data.is_empty());
    }

    #[test]
    fn test_decode_update_action_body_too_short() {
        let result = decode_update_action(&[]);
        assert!(result.is_err());
        let result = decode_update_action(&[0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_update_action_truncated_name() {
        // Says name is 10 bytes but only has 2
        let body = vec![0x00, 0x0A, b'a', b'b'];
        let result = decode_update_action(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_update_action_truncated_filter() {
        let name = "t";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&100u32.to_be_bytes()); // says filter is 100 bytes
        body.extend_from_slice(b"short"); // but only 5 bytes of filter
        let result = decode_update_action(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_update_action_missing_filter_len() {
        // Name is valid but no room for filter_len u32
        let name = "t";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        // No filter_len bytes
        let result = decode_update_action(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_update_action_invalid_utf8_filter() {
        let name = "t";
        let mut body = Vec::new();
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8 filter
        let result = decode_update_action(&body);
        assert!(result.is_err());
    }

    // ---- parse_delete_sql tests ----

    #[test]
    fn test_parse_delete_sql_valid_basic() {
        let result = parse_delete_sql("DELETE FROM users WHERE id = 5");
        assert_eq!(result, Some(("users".to_string(), "id = 5".to_string())));
    }

    #[test]
    fn test_parse_delete_sql_case_insensitive() {
        let result = parse_delete_sql("delete from Users where id > 10");
        assert_eq!(result, Some(("Users".to_string(), "id > 10".to_string())));
    }

    #[test]
    fn test_parse_delete_sql_schema_qualified() {
        let result = parse_delete_sql("DELETE FROM public.users WHERE id = 1");
        assert_eq!(result, Some(("users".to_string(), "id = 1".to_string())));
    }

    #[test]
    fn test_parse_delete_sql_quoted_table() {
        let result = parse_delete_sql("DELETE FROM \"my_table\" WHERE x = 1");
        assert_eq!(result, Some(("my_table".to_string(), "x = 1".to_string())));
    }

    #[test]
    fn test_parse_delete_sql_trailing_semicolon() {
        let result = parse_delete_sql("DELETE FROM t WHERE id = 1;");
        assert_eq!(result, Some(("t".to_string(), "id = 1".to_string())));
    }

    #[test]
    fn test_parse_delete_sql_extra_whitespace() {
        let result = parse_delete_sql("DELETE   FROM   users   WHERE   id = 5");
        assert_eq!(result, Some(("users".to_string(), "id = 5".to_string())));
    }

    #[test]
    fn test_parse_delete_sql_complex_filter() {
        let result = parse_delete_sql("DELETE FROM t WHERE id > 5 AND name = 'foo'");
        assert_eq!(
            result,
            Some(("t".to_string(), "id > 5 AND name = 'foo'".to_string()))
        );
    }

    #[test]
    fn test_parse_delete_sql_no_where_clause() {
        assert_eq!(parse_delete_sql("DELETE FROM users"), None);
    }

    #[test]
    fn test_parse_delete_sql_not_a_delete() {
        assert_eq!(parse_delete_sql("SELECT * FROM users"), None);
    }

    #[test]
    fn test_parse_delete_sql_missing_from() {
        assert_eq!(parse_delete_sql("DELETE users WHERE id = 1"), None);
    }

    #[test]
    fn test_parse_delete_sql_empty_input() {
        assert_eq!(parse_delete_sql(""), None);
    }

    #[test]
    fn test_parse_delete_sql_just_delete() {
        assert_eq!(parse_delete_sql("DELETE"), None);
    }

    #[test]
    fn test_parse_delete_sql_empty_table() {
        assert_eq!(parse_delete_sql("DELETE FROM WHERE x = 1"), None);
    }

    // ---- parse_update_sql tests ----

    #[test]
    fn test_parse_update_sql_valid_basic() {
        let result = parse_update_sql("UPDATE users SET name = 'foo' WHERE id = 1");
        assert_eq!(
            result,
            Some((
                "users".to_string(),
                "name = 'foo'".to_string(),
                "id = 1".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_update_sql_multiple_assignments() {
        let result = parse_update_sql("UPDATE t SET a = 1, b = 2 WHERE id = 3");
        assert_eq!(
            result,
            Some((
                "t".to_string(),
                "a = 1, b = 2".to_string(),
                "id = 3".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_update_sql_case_insensitive() {
        let result = parse_update_sql("update Users set name='test' where id=1");
        assert_eq!(
            result,
            Some((
                "Users".to_string(),
                "name='test'".to_string(),
                "id=1".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_update_sql_schema_qualified() {
        let result = parse_update_sql("UPDATE public.users SET x = 1 WHERE y = 2");
        assert_eq!(
            result,
            Some((
                "users".to_string(),
                "x = 1".to_string(),
                "y = 2".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_update_sql_trailing_semicolon() {
        let result = parse_update_sql("UPDATE t SET x = 1 WHERE id = 1;");
        assert_eq!(
            result,
            Some(("t".to_string(), "x = 1".to_string(), "id = 1".to_string()))
        );
    }

    #[test]
    fn test_parse_update_sql_expression_in_set() {
        let result = parse_update_sql("UPDATE t SET price = price * 1.1 WHERE id = 1");
        assert_eq!(
            result,
            Some((
                "t".to_string(),
                "price = price * 1.1".to_string(),
                "id = 1".to_string()
            ))
        );
    }

    #[test]
    fn test_parse_update_sql_no_where() {
        assert_eq!(parse_update_sql("UPDATE users SET name = 'foo'"), None);
    }

    #[test]
    fn test_parse_update_sql_no_set() {
        assert_eq!(parse_update_sql("UPDATE users WHERE id = 1"), None);
    }

    #[test]
    fn test_parse_update_sql_not_an_update() {
        assert_eq!(parse_update_sql("INSERT INTO users VALUES (1)"), None);
    }

    #[test]
    fn test_parse_update_sql_empty_input() {
        assert_eq!(parse_update_sql(""), None);
    }

    #[test]
    fn test_parse_update_sql_empty_parts() {
        assert_eq!(parse_update_sql("UPDATE SET WHERE"), None);
    }

    // ---- build_update_select_sql tests ----

    #[test]
    fn test_build_update_select_sql_single_assignment() {
        let result = build_update_select_sql("users", "name = 'foo'", "id = 1");
        assert_eq!(
            result,
            "SELECT * EXCLUDE (name), 'foo' AS name FROM users WHERE id = 1"
        );
    }

    #[test]
    fn test_build_update_select_sql_multiple_assignments() {
        let result = build_update_select_sql("t", "a = 1, b = 2", "id = 3");
        assert_eq!(
            result,
            "SELECT * EXCLUDE (a, b), 1 AS a, 2 AS b FROM t WHERE id = 3"
        );
    }

    #[test]
    fn test_build_update_select_sql_expression_assignment() {
        let result = build_update_select_sql("t", "price = price * 1.1", "id = 1");
        assert_eq!(
            result,
            "SELECT * EXCLUDE (price), price * 1.1 AS price FROM t WHERE id = 1"
        );
    }
}
