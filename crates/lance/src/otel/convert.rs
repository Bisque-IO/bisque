//! Protobuf → JSON conversion helpers for OTEL attributes.
//!
//! Converts OTEL `KeyValue` attribute lists and `AnyValue` types into
//! JSON strings for storage in Utf8 Arrow columns.

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use serde_json::json;

/// Convert a slice of OTEL `KeyValue` pairs into a JSON object string.
///
/// Example output: `{"service.name":"my-svc","http.method":"GET"}`
/// Returns `"{}"` for empty attribute lists.
pub fn key_values_to_json(attrs: &[KeyValue]) -> String {
    if attrs.is_empty() {
        return "{}".to_string();
    }
    let map: serde_json::Map<String, serde_json::Value> = attrs
        .iter()
        .map(|kv| {
            let val = kv
                .value
                .as_ref()
                .map(any_value_to_json)
                .unwrap_or(serde_json::Value::Null);
            (kv.key.clone(), val)
        })
        .collect();
    serde_json::Value::Object(map).to_string()
}

/// Convert an OTEL `AnyValue` to a `serde_json::Value`.
pub fn any_value_to_json(av: &AnyValue) -> serde_json::Value {
    match &av.value {
        Some(Value::StringValue(s)) => json!(s),
        Some(Value::BoolValue(b)) => json!(b),
        Some(Value::IntValue(i)) => json!(i),
        Some(Value::DoubleValue(d)) => json!(d),
        Some(Value::BytesValue(b)) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            json!(hex)
        }
        Some(Value::ArrayValue(arr)) => {
            let items: Vec<serde_json::Value> =
                arr.values.iter().map(any_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(Value::KvlistValue(kvl)) => {
            let map: serde_json::Map<String, serde_json::Value> = kvl
                .values
                .iter()
                .map(|kv| {
                    let val = kv
                        .value
                        .as_ref()
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    (kv.key.clone(), val)
                })
                .collect();
            serde_json::Value::Object(map)
        }
        None => serde_json::Value::Null,
    }
}

/// Convert an optional `AnyValue` body (e.g., log body) to a string.
///
/// String values are returned as-is. Other types are JSON-encoded.
pub fn any_value_to_string(av: Option<&AnyValue>) -> String {
    match av.and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => s.clone(),
        Some(_) => any_value_to_json(av.unwrap()).to_string(),
        None => String::new(),
    }
}

/// Pad with zeros or truncate to exactly `len` bytes.
///
/// Returns the input slice directly when it's already the correct length,
/// avoiding allocation in the common case.
pub fn pad_or_truncate(data: &[u8], len: usize) -> std::borrow::Cow<'_, [u8]> {
    if data.len() == len {
        std::borrow::Cow::Borrowed(data)
    } else {
        let mut buf = vec![0u8; len];
        let copy_len = data.len().min(len);
        buf[..copy_len].copy_from_slice(&data[..copy_len]);
        std::borrow::Cow::Owned(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(value),
            }),
        }
    }

    #[test]
    fn empty_attributes() {
        assert_eq!(key_values_to_json(&[]), "{}");
    }

    #[test]
    fn string_attributes() {
        let attrs = vec![kv("service.name", Value::StringValue("my-svc".into()))];
        let json = key_values_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["service.name"], "my-svc");
    }

    #[test]
    fn mixed_attributes() {
        let attrs = vec![
            kv("count", Value::IntValue(42)),
            kv("ratio", Value::DoubleValue(3.14)),
            kv("enabled", Value::BoolValue(true)),
        ];
        let json = key_values_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["count"], 42);
        assert_eq!(parsed["ratio"], 3.14);
        assert_eq!(parsed["enabled"], true);
    }

    #[test]
    fn array_attribute() {
        let arr = ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(Value::StringValue("a".into())),
                },
                AnyValue {
                    value: Some(Value::StringValue("b".into())),
                },
            ],
        };
        let attrs = vec![kv("tags", Value::ArrayValue(arr))];
        let json = key_values_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["tags"], json!(["a", "b"]));
    }

    #[test]
    fn kvlist_attribute() {
        let kvl = KeyValueList {
            values: vec![kv("nested_key", Value::StringValue("nested_val".into()))],
        };
        let attrs = vec![kv("meta", Value::KvlistValue(kvl))];
        let json = key_values_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["meta"]["nested_key"], "nested_val");
    }

    #[test]
    fn body_string_passthrough() {
        let av = AnyValue {
            value: Some(Value::StringValue("hello world".into())),
        };
        assert_eq!(any_value_to_string(Some(&av)), "hello world");
    }

    #[test]
    fn body_none() {
        assert_eq!(any_value_to_string(None), "");
    }
}
