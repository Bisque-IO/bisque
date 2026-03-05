//! Protobuf ↔ JSON conversion helpers for OTEL attributes.
//!
//! Converts OTEL `KeyValue` attribute lists and `AnyValue` types into
//! JSON strings for storage in Utf8 Arrow columns, and back again
//! for query-time reconstruction of OTLP protobuf responses.

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
            let items: Vec<serde_json::Value> = arr.values.iter().map(any_value_to_json).collect();
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

/// Convert an optional `AnyValue` body (e.g., log body) to a string
/// together with a type tag preserving the original `AnyValue` variant.
///
/// Returns `(body_string, body_type)`.
/// - String values are returned as-is with type `"string"`.
/// - Other types are JSON-encoded with their variant name.
/// - `None` / empty returns `("", "")`.
pub fn any_value_to_string_typed(av: Option<&AnyValue>) -> (String, &'static str) {
    match av.and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => (s.clone(), "string"),
        Some(Value::BoolValue(_)) => (any_value_to_json(av.unwrap()).to_string(), "bool"),
        Some(Value::IntValue(_)) => (any_value_to_json(av.unwrap()).to_string(), "int"),
        Some(Value::DoubleValue(_)) => (any_value_to_json(av.unwrap()).to_string(), "double"),
        Some(Value::BytesValue(_)) => (any_value_to_json(av.unwrap()).to_string(), "bytes"),
        Some(Value::ArrayValue(_)) => (any_value_to_json(av.unwrap()).to_string(), "array"),
        Some(Value::KvlistValue(_)) => (any_value_to_json(av.unwrap()).to_string(), "kvlist"),
        None => (String::new(), ""),
    }
}

/// Convert a JSON object string back into a `Vec<KeyValue>`.
///
/// This is the inverse of [`key_values_to_json`]. JSON types are mapped
/// back to OTLP `AnyValue` variants:
/// - JSON string → `StringValue`
/// - JSON bool → `BoolValue`
/// - JSON integer → `IntValue`
/// - JSON float → `DoubleValue`
/// - JSON array → `ArrayValue`
/// - JSON object → `KvlistValue`
/// - JSON null → no value
///
/// Returns an empty `Vec` if the input is not a valid JSON object.
pub fn json_to_key_values(json_str: &str) -> Vec<KeyValue> {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str(json_str) else {
        return Vec::new();
    };
    map.into_iter()
        .map(|(key, val)| KeyValue {
            key,
            value: Some(json_to_any_value(&val)),
        })
        .collect()
}

/// Convert a `serde_json::Value` to an OTLP `AnyValue`.
pub fn json_to_any_value(val: &serde_json::Value) -> AnyValue {
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    let value = match val {
        serde_json::Value::String(s) => Some(Value::StringValue(s.clone())),
        serde_json::Value::Bool(b) => Some(Value::BoolValue(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::IntValue(i))
            } else {
                Some(Value::DoubleValue(n.as_f64().unwrap_or(0.0)))
            }
        }
        serde_json::Value::Array(arr) => {
            let values = arr.iter().map(json_to_any_value).collect();
            Some(Value::ArrayValue(ArrayValue { values }))
        }
        serde_json::Value::Object(map) => {
            let values = map
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: Some(json_to_any_value(v)),
                })
                .collect();
            Some(Value::KvlistValue(KeyValueList { values }))
        }
        serde_json::Value::Null => None,
    };
    AnyValue { value }
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
            value: Some(AnyValue { value: Some(value) }),
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
        let (body, ty) = any_value_to_string_typed(Some(&av));
        assert_eq!(body, "hello world");
        assert_eq!(ty, "string");
    }

    #[test]
    fn body_none() {
        let (body, ty) = any_value_to_string_typed(None);
        assert_eq!(body, "");
        assert_eq!(ty, "");
    }

    #[test]
    fn body_int_preserves_type() {
        let av = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        let (body, ty) = any_value_to_string_typed(Some(&av));
        assert_eq!(body, "42");
        assert_eq!(ty, "int");
    }

    #[test]
    fn json_to_key_values_roundtrip() {
        let original = vec![
            kv("service.name", Value::StringValue("my-svc".into())),
            kv("count", Value::IntValue(42)),
            kv("enabled", Value::BoolValue(true)),
        ];
        let json = key_values_to_json(&original);
        let recovered = json_to_key_values(&json);
        assert_eq!(recovered.len(), 3);
        // Verify by re-serializing
        let json2 = key_values_to_json(&recovered);
        let p1: serde_json::Value = serde_json::from_str(&json).unwrap();
        let p2: serde_json::Value = serde_json::from_str(&json2).unwrap();
        assert_eq!(p1, p2);
    }

    #[test]
    fn json_to_key_values_empty() {
        assert!(json_to_key_values("{}").is_empty());
        assert!(json_to_key_values("invalid").is_empty());
    }

    #[test]
    fn body_kvlist_preserves_type() {
        let kvl = KeyValueList {
            values: vec![kv("k", Value::StringValue("v".into()))],
        };
        let av = AnyValue {
            value: Some(Value::KvlistValue(kvl)),
        };
        let (_, ty) = any_value_to_string_typed(Some(&av));
        assert_eq!(ty, "kvlist");
    }
}
