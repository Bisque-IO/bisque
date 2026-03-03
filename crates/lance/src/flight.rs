//! Arrow Flight service layer for bisque-lance.
//!
//! Exposes the bisque-lance storage engine over the Arrow Flight gRPC protocol,
//! enabling language-agnostic clients (Python, Java, Go, C++, etc.) to:
//!
//! - **Write** record batches via `do_put`
//! - **Query** via SQL through `do_get`
//! - **DDL** (create/drop tables) via `do_action`
//! - **Introspect** schemas and available tables via `get_schema`, `get_flight_info`, `list_flights`
//!
//! # Wire format conventions
//!
//! | RPC | Descriptor / Ticket | Body |
//! |-----|---------------------|------|
//! | `do_put` | `FlightDescriptor::path = [table_name]` | Streaming `FlightData` (RecordBatches) |
//! | `do_get` | `Ticket` = UTF-8 SQL string | — |
//! | `get_flight_info` | `FlightDescriptor::path = [table_name]` | — |
//! | `get_schema` | `FlightDescriptor::path = [table_name]` | — |
//! | `do_action("create_table")` | — | `u16 name_len ++ name ++ IPC schema` |
//! | `do_action("drop_table")` | — | UTF-8 table name |
//! | `list_flights` | — | — |

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::SchemaRef;
use datafusion::execution::context::SessionContext;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use crate::ipc;
use crate::query::BisqueLanceTableProvider;
use crate::raft::{LanceRaftNode, WriteError};

/// Arrow Flight service backed by a bisque-lance Raft node.
///
/// All write operations (do_put, do_action for DDL) are routed through Raft
/// consensus via [`LanceRaftNode`]. Read operations (do_get, get_schema,
/// get_flight_info, list_flights) are served directly from the local engine.
pub struct BisqueFlightService {
    raft_node: Arc<LanceRaftNode>,
}

impl BisqueFlightService {
    /// Create a new Flight service wrapping the given Raft node.
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
            let table = engine.get_table(&table_name).ok_or_else(|| {
                Status::internal(format!("table disappeared during query setup: {table_name}"))
            })?;

            let schema = table.schema().await.ok_or_else(|| {
                Status::internal(format!("table {table_name} has no schema"))
            })?;

            let provider = BisqueLanceTableProvider::new(table, schema);
            ctx.register_table(&table_name, Arc::new(provider))
                .map_err(|e| Status::internal(format!("failed to register table: {e}")))?;
        }

        Ok(ctx)
    }
}

/// Extract the table name from a FlightDescriptor's path.
fn table_name_from_descriptor(descriptor: &FlightDescriptor) -> Result<&str, Status> {
    descriptor
        .path
        .first()
        .map(|s| s.as_str())
        .ok_or_else(|| {
            Status::invalid_argument("FlightDescriptor.path must contain at least one element (the table name)")
        })
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

/// Encode an Arrow schema as IPC bytes suitable for `FlightInfo.schema` / `SchemaResult.schema`.
fn schema_to_ipc_bytes(schema: &arrow_schema::Schema) -> Result<bytes::Bytes, Status> {
    let options = IpcWriteOptions::default();
    let pair: arrow_flight::IpcMessage = SchemaAsIpc::new(schema, &options)
        .try_into()
        .map_err(|e: arrow_schema::ArrowError| {
            Status::internal(format!("failed to encode schema: {e}"))
        })?;
    Ok(pair.0)
}

/// Decode the body of a `create_table` action.
///
/// Format: `u16 BE name_len` + `name bytes` + `IPC schema bytes`
fn decode_create_table_action(body: &[u8]) -> Result<(String, arrow_schema::Schema), Status> {
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

type BoxedFlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for BisqueFlightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;

    // =========================================================================
    // Handshake — not implemented (no auth required yet)
    // =========================================================================

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    // =========================================================================
    // list_flights — return FlightInfo for each table
    // =========================================================================

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let engine = self.raft_node.engine();
        let mut infos = Vec::new();

        for table_name in engine.list_tables() {
            let table = match engine.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };

            let schema = match table.schema().await {
                Some(s) => s,
                None => continue,
            };

            let info = FlightInfo::new()
                .try_with_schema(&schema)
                .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
                .with_descriptor(FlightDescriptor::new_path(vec![table_name.clone()]))
                .with_endpoint(
                    FlightEndpoint::new().with_ticket(Ticket::new(format!(
                        "SELECT * FROM \"{table_name}\""
                    ))),
                );

            infos.push(Ok(info));
        }

        let stream = futures::stream::iter(infos);
        Ok(Response::new(Box::pin(stream)))
    }

    // =========================================================================
    // get_flight_info — return FlightInfo for a specific table
    // =========================================================================

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let table_name = table_name_from_descriptor(&descriptor)?.to_string();

        let engine = self.raft_node.engine();
        let table = engine
            .get_table(&table_name)
            .ok_or_else(|| Status::not_found(format!("table not found: {table_name}")))?;

        let schema = table
            .schema()
            .await
            .ok_or_else(|| Status::internal(format!("table {table_name} has no schema")))?;

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode error: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(format!(
                    "SELECT * FROM \"{table_name}\""
                ))),
            );

        Ok(Response::new(info))
    }

    // =========================================================================
    // poll_flight_info — not implemented
    // =========================================================================

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    // =========================================================================
    // get_schema — return the Arrow schema for a table
    // =========================================================================

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let table_name = table_name_from_descriptor(&descriptor)?;

        let engine = self.raft_node.engine();
        let table = engine
            .get_table(table_name)
            .ok_or_else(|| Status::not_found(format!("table not found: {table_name}")))?;

        let schema = table
            .schema()
            .await
            .ok_or_else(|| Status::internal(format!("table {table_name} has no schema")))?;

        let schema_bytes = schema_to_ipc_bytes(&schema)?;

        Ok(Response::new(arrow_flight::SchemaResult {
            schema: schema_bytes,
        }))
    }

    // =========================================================================
    // do_get — execute a SQL query and stream results
    // =========================================================================

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = std::str::from_utf8(&ticket.ticket)
            .map_err(|_| Status::invalid_argument("ticket is not valid UTF-8 SQL"))?;

        debug!(sql = %sql, "do_get: executing SQL query");

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

        let response_stream = flight_stream.map(|result| {
            result.map_err(|e| Status::internal(format!("flight encode error: {e}")))
        });

        Ok(Response::new(Box::pin(response_stream)))
    }

    // =========================================================================
    // do_put — write record batches to a table
    // =========================================================================

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();

        // Collect all FlightData messages. The first message carries the
        // FlightDescriptor (with the table name) and the schema header.
        let mut messages: Vec<FlightData> = Vec::new();
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("stream error: {e}")))?
        {
            messages.push(msg);
        }

        if messages.is_empty() {
            return Err(Status::invalid_argument("empty do_put stream"));
        }

        // Extract table name from the first message's FlightDescriptor.
        let descriptor = messages[0]
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| {
                Status::invalid_argument(
                    "first FlightData must include a FlightDescriptor with the table name",
                )
            })?;
        let table_name = table_name_from_descriptor(descriptor)?.to_string();

        debug!(table = %table_name, num_messages = messages.len(), "do_put: writing batches");

        // Decode FlightData messages into RecordBatches using FlightRecordBatchStream.
        let data_stream = futures::stream::iter(
            messages
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );
        let mut record_stream = FlightRecordBatchStream::new_from_flight_data(data_stream);

        let mut batches = Vec::new();
        while let Some(batch) = record_stream
            .next()
            .await
        {
            let batch = batch.map_err(|e| {
                Status::invalid_argument(format!("failed to decode RecordBatch: {e}"))
            })?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(Status::invalid_argument(
                "do_put stream contained no record batches",
            ));
        }

        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Write through Raft consensus.
        self.raft_node
            .write_records(&table_name, &batches)
            .await
            .map_err(write_error_to_status)?;

        debug!(table = %table_name, rows = num_rows, "do_put: write complete");

        let result = PutResult {
            app_metadata: bytes::Bytes::from(format!("{num_rows}")),
        };
        let stream = futures::stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    // =========================================================================
    // do_exchange — not implemented
    // =========================================================================

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    // =========================================================================
    // do_action — DDL operations (create_table, drop_table)
    // =========================================================================

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
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

    // =========================================================================
    // list_actions — enumerate available actions
    // =========================================================================

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "create_table".to_string(),
                description: "Create a new table. Body: u16 name_len + name + IPC schema"
                    .to_string(),
            }),
            Ok(ActionType {
                r#type: "drop_table".to_string(),
                description: "Drop a table. Body: UTF-8 table name".to_string(),
            }),
        ];

        let stream = futures::stream::iter(actions);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Start a Flight gRPC server.
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
