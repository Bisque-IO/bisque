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
//! | `CommandStatementUpdate` | DataFusion DDL (CREATE TABLE, DROP TABLE) |
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
    CommandGetSqlInfo, CommandGetTables, CommandGetTableTypes, CommandStatementIngest,
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
use futures::{StreamExt, TryStreamExt};
use prost::Message as _;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use crate::ipc;
use crate::query::BisqueLanceTableProvider;
use crate::raft::{LanceRaftNode, WriteError};

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
}

impl BisqueFlightService {
    /// Create a new Flight SQL service wrapping the given Raft node.
    pub fn new(raft_node: Arc<LanceRaftNode>) -> Self {
        Self { raft_node }
    }

    /// Get a reference to the underlying Raft node.
    pub fn raft_node(&self) -> &Arc<LanceRaftNode> {
        &self.raft_node
    }

    /// Build a [`FlightServiceServer`] from this service, ready to be added to a tonic router.
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Build a DataFusion `SessionContext` with all current tables registered.
    async fn build_session_context(&self) -> Result<SessionContext, Status> {
        let engine = self.raft_node.engine();
        let ctx = SessionContext::new();

        for table_name in engine.list_tables() {
            let table = match engine.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };

            let schema = match table.schema().await {
                Some(s) => s,
                None => continue,
            };

            let provider = BisqueLanceTableProvider::new(table, schema);
            ctx.register_table(&table_name, Arc::new(provider))
                .map_err(|e| Status::internal(format!("failed to register table: {e}")))?;
        }

        Ok(ctx)
    }

    /// Execute a SQL query and return the result schema + batches.
    async fn execute_sql(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        let ctx = self.build_session_context().await?;

        let df = ctx
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(sql = %query.query, "get_flight_info_statement");

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
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|_| Status::invalid_argument("statement handle is not valid UTF-8 SQL"))?;

        debug!(sql = %sql, "do_get_statement");

        let ctx = self.build_session_context().await?;

        let df = ctx
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
            .map(|result| result.map_err(|e| Status::internal(format!("flight encode error: {e}"))));

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // SQL DML/DDL — do_put_statement_update
    // =========================================================================

    /// Handle `CommandStatementUpdate` — execute DDL/DML SQL statements.
    ///
    /// Supports `CREATE TABLE ... AS SELECT ...`, `DROP TABLE`, etc.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = &ticket.query;
        debug!(sql = %sql, "do_put_statement_update");

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

        let batch_stream = futures::stream::once(async {
            Ok::<_, arrow_flight::error::FlightError>(batch)
        });
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

        let batch_stream = futures::stream::once(async {
            Ok::<_, arrow_flight::error::FlightError>(batch)
        });
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

        let batch_stream = futures::stream::once(async {
            Ok::<_, arrow_flight::error::FlightError>(batch)
        });
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

        let batch_stream = futures::stream::once(async {
            Ok::<_, arrow_flight::error::FlightError>(batch)
        });
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

        let batch_stream = futures::stream::once(async {
            Ok::<_, arrow_flight::error::FlightError>(batch)
        });
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
        let ipc_msg: arrow_flight::IpcMessage = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e: arrow_schema::ArrowError| {
                Status::internal(format!("schema encode error: {e}"))
            })?;

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
        let sql = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|_| Status::invalid_argument("prepared statement handle is not valid UTF-8"))?;

        debug!(sql = %sql, "do_get_prepared_statement");

        let ctx = self.build_session_context().await?;
        let df = ctx
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
        let sql = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|_| Status::invalid_argument("prepared statement handle is not valid UTF-8"))?;

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
