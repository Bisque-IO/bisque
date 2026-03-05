//! OTel-Arrow protocol types and gRPC service definitions.
//!
//! Defines the messages and services from the OTel-Arrow `arrow_service.proto`
//! using prost derives (no build.rs / proto files needed). The protocol enables
//! efficient telemetry transport using Apache Arrow IPC encoding over
//! bidirectional gRPC streams.
//!
//! See: <https://github.com/open-telemetry/otel-arrow>

#[allow(unused_imports)]
use std::pin::Pin;

#[allow(unused_imports)]
use futures::Stream;
use tonic::codegen::http;

// ---------------------------------------------------------------------------
// Protocol Messages
// ---------------------------------------------------------------------------

/// A batch of Arrow-encoded telemetry records sent from exporter to collector.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchArrowRecords {
    /// Unique batch identifier within the stream context.
    #[prost(int64, tag = "1")]
    pub batch_id: i64,

    /// Collection of Arrow-encoded payloads.
    #[prost(message, repeated, tag = "2")]
    pub arrow_payloads: ::prost::alloc::vec::Vec<ArrowPayload>,

    /// Optional headers, hpack-encoded.
    #[prost(bytes = "vec", tag = "3")]
    pub headers: ::prost::alloc::vec::Vec<u8>,
}

/// A single Arrow-encoded payload within a batch.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowPayload {
    /// Canonical ID representing the Arrow Record schema. When a new schema_id
    /// is seen, the consumer must create a new IPC reader.
    #[prost(string, tag = "1")]
    pub schema_id: ::prost::alloc::string::String,

    /// Type of OTel Arrow payload.
    #[prost(enumeration = "ArrowPayloadType", tag = "2")]
    pub r#type: i32,

    /// Serialized Arrow Record Batch (IPC stream format).
    #[prost(bytes = "vec", tag = "3")]
    pub record: ::prost::alloc::vec::Vec<u8>,
}

/// Collector acknowledgment for a batch.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchStatus {
    #[prost(int64, tag = "1")]
    pub batch_id: i64,

    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status_code: i32,

    #[prost(string, tag = "3")]
    pub status_message: ::prost::alloc::string::String,
}

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

/// All OTel Arrow payload types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ArrowPayloadType {
    Unknown = 0,
    ResourceAttrs = 1,
    ScopeAttrs = 2,
    UnivariateMetrics = 10,
    NumberDataPoints = 11,
    SummaryDataPoints = 12,
    HistogramDataPoints = 13,
    ExpHistogramDataPoints = 14,
    NumberDpAttrs = 15,
    SummaryDpAttrs = 16,
    HistogramDpAttrs = 17,
    ExpHistogramDpAttrs = 18,
    NumberDpExemplars = 19,
    HistogramDpExemplars = 20,
    ExpHistogramDpExemplars = 21,
    NumberDpExemplarAttrs = 22,
    HistogramDpExemplarAttrs = 23,
    ExpHistogramDpExemplarAttrs = 24,
    MultivariateMetrics = 25,
    MetricAttrs = 26,
    Logs = 30,
    LogAttrs = 31,
    Spans = 40,
    SpanAttrs = 41,
    SpanEvents = 42,
    SpanLinks = 43,
    SpanEventAttrs = 44,
    SpanLinkAttrs = 45,
}

/// Status codes aligned with gRPC status codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StatusCode {
    Ok = 0,
    Canceled = 1,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    Aborted = 10,
    Internal = 13,
    Unavailable = 14,
    Unauthenticated = 16,
}

// ---------------------------------------------------------------------------
// Service Traits
// ---------------------------------------------------------------------------

/// ArrowTracesService — traces-only OTel-Arrow bidirectional stream.
#[tonic::async_trait]
pub trait ArrowTracesService: Send + Sync + 'static {
    type ArrowTracesStream: Stream<Item = Result<BatchStatus, tonic::Status>> + Send + 'static;

    async fn arrow_traces(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowTracesStream>, tonic::Status>;
}

/// ArrowLogsService — logs-only OTel-Arrow bidirectional stream.
#[tonic::async_trait]
pub trait ArrowLogsService: Send + Sync + 'static {
    type ArrowLogsStream: Stream<Item = Result<BatchStatus, tonic::Status>> + Send + 'static;

    async fn arrow_logs(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowLogsStream>, tonic::Status>;
}

/// ArrowMetricsService — metrics-only OTel-Arrow bidirectional stream.
#[tonic::async_trait]
pub trait ArrowMetricsService: Send + Sync + 'static {
    type ArrowMetricsStream: Stream<Item = Result<BatchStatus, tonic::Status>> + Send + 'static;

    async fn arrow_metrics(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowMetricsStream>, tonic::Status>;
}

// ---------------------------------------------------------------------------
// Server implementations (matching tonic 0.14 generated code pattern)
// ---------------------------------------------------------------------------

macro_rules! impl_arrow_service_server {
    ($server:ident, $trait:ident, $method:ident, $stream_type:ident, $service_name:expr, $rpc_path:expr) => {
        #[derive(Debug)]
        pub struct $server<T> {
            inner: std::sync::Arc<T>,
            accept_compression_encodings: tonic::codec::EnabledCompressionEncodings,
            send_compression_encodings: tonic::codec::EnabledCompressionEncodings,
            max_decoding_message_size: Option<usize>,
            max_encoding_message_size: Option<usize>,
        }

        impl<T> $server<T> {
            pub fn new(inner: T) -> Self {
                Self::from_arc(std::sync::Arc::new(inner))
            }

            pub fn from_arc(inner: std::sync::Arc<T>) -> Self {
                Self {
                    inner,
                    accept_compression_encodings: Default::default(),
                    send_compression_encodings: Default::default(),
                    max_decoding_message_size: None,
                    max_encoding_message_size: None,
                }
            }

            #[must_use]
            pub fn accept_compressed(
                mut self,
                encoding: tonic::codec::CompressionEncoding,
            ) -> Self {
                self.accept_compression_encodings.enable(encoding);
                self
            }

            #[must_use]
            pub fn send_compressed(
                mut self,
                encoding: tonic::codec::CompressionEncoding,
            ) -> Self {
                self.send_compression_encodings.enable(encoding);
                self
            }
        }

        impl<T> Clone for $server<T> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                    accept_compression_encodings: self.accept_compression_encodings,
                    send_compression_encodings: self.send_compression_encodings,
                    max_decoding_message_size: self.max_decoding_message_size,
                    max_encoding_message_size: self.max_encoding_message_size,
                }
            }
        }

        impl<T> tonic::server::NamedService for $server<T> {
            const NAME: &'static str = $service_name;
        }

        impl<T, B> tonic::codegen::Service<http::Request<B>> for $server<T>
        where
            T: $trait,
            B: tonic::codegen::Body + Send + 'static,
            B::Error: Into<tonic::codegen::StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

            fn poll_ready(
                &mut self,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                match req.uri().path() {
                    $rpc_path => {
                        #[allow(non_camel_case_types)]
                        struct Svc<T: $trait>(pub std::sync::Arc<T>);

                        impl<T: $trait>
                            tonic::server::StreamingService<BatchArrowRecords>
                            for Svc<T>
                        {
                            type Response = BatchStatus;
                            type ResponseStream = T::$stream_type;
                            type Future = tonic::codegen::BoxFuture<
                                tonic::Response<Self::ResponseStream>,
                                tonic::Status,
                            >;

                            fn call(
                                &mut self,
                                request: tonic::Request<
                                    tonic::Streaming<BatchArrowRecords>,
                                >,
                            ) -> Self::Future {
                                let inner = std::sync::Arc::clone(&self.0);
                                let fut = async move {
                                    <T as $trait>::$method(&inner, request).await
                                };
                                Box::pin(fut)
                            }
                        }

                        let accept_compression_encodings =
                            self.accept_compression_encodings;
                        let send_compression_encodings =
                            self.send_compression_encodings;
                        let max_decoding_message_size =
                            self.max_decoding_message_size;
                        let max_encoding_message_size =
                            self.max_encoding_message_size;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let method = Svc(inner);
                            let codec = tonic_prost::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                )
                                .apply_max_message_size_config(
                                    max_decoding_message_size,
                                    max_encoding_message_size,
                                );
                            let res = grpc.streaming(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    _ => Box::pin(async move {
                        let mut response = http::Response::new(
                            tonic::body::Body::default(),
                        );
                        let headers = response.headers_mut();
                        headers.insert(
                            tonic::Status::GRPC_STATUS,
                            (tonic::Code::Unimplemented as i32).into(),
                        );
                        headers.insert(
                            http::header::CONTENT_TYPE,
                            tonic::metadata::GRPC_CONTENT_TYPE,
                        );
                        Ok(response)
                    }),
                }
            }
        }
    };
}

impl_arrow_service_server!(
    ArrowTracesServiceServer,
    ArrowTracesService,
    arrow_traces,
    ArrowTracesStream,
    "opentelemetry.proto.experimental.arrow.v1.ArrowTracesService",
    "/opentelemetry.proto.experimental.arrow.v1.ArrowTracesService/ArrowTraces"
);

impl_arrow_service_server!(
    ArrowLogsServiceServer,
    ArrowLogsService,
    arrow_logs,
    ArrowLogsStream,
    "opentelemetry.proto.experimental.arrow.v1.ArrowLogsService",
    "/opentelemetry.proto.experimental.arrow.v1.ArrowLogsService/ArrowLogs"
);

impl_arrow_service_server!(
    ArrowMetricsServiceServer,
    ArrowMetricsService,
    arrow_metrics,
    ArrowMetricsStream,
    "opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService",
    "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService/ArrowMetrics"
);
