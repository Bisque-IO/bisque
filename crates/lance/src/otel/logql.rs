//! Minimal LogQL parser.
//!
//! Hand-rolled recursive descent parser for a practical subset of LogQL:
//!
//! ```text
//! query           = log_query | metric_query
//! log_query       = stream_selector pipeline*
//! metric_query    = metric_func "(" log_query "[" duration "]" ")"
//!                 | agg_func ("by" "(" labels ")")? "(" metric_query ")"
//! stream_selector = "{" matcher ("," matcher)* "}"
//! matcher         = label op string
//! pipeline        = "|=" string | "!=" string | "|~" string | "!~" string
//! ```
//!
//! This covers the LogQL subset needed for Grafana Loki data source
//! compatibility: stream selectors, line filters, and basic metric queries.

use regex::Regex;

// ---------------------------------------------------------------------------
// AST types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum LogQLExpr {
    LogQuery {
        selector: StreamSelector,
        pipeline: Vec<PipelineStage>,
    },
    MetricQuery {
        func: MetricFunc,
        inner: Box<LogQLExpr>,
        range: std::time::Duration,
    },
    Aggregation {
        op: AggFunc,
        by_labels: Vec<String>,
        inner: Box<LogQLExpr>,
    },
}

#[derive(Debug, Clone)]
pub struct StreamSelector {
    pub matchers: Vec<StreamMatcher>,
}

#[derive(Debug, Clone)]
pub struct StreamMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum MatchOp {
    Eq,
    Neq,
    Re,
    Nre,
}

#[derive(Debug, Clone)]
pub enum PipelineStage {
    LineContains(String),
    LineNotContains(String),
    LineMatchesRegex(String),
    LineNotMatchesRegex(String),
}

#[derive(Debug, Clone, Copy)]
pub enum MetricFunc {
    CountOverTime,
    Rate,
    BytesOverTime,
    BytesRate,
}

#[derive(Debug, Clone, Copy)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

pub fn parse(input: &str) -> Result<LogQLExpr, String> {
    let mut parser = Parser::new(input);
    let expr = parser.parse_expr()?;
    Ok(expr)
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn skip_ws(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.skip_ws();
        self.remaining().chars().next()
    }

    fn expect(&mut self, ch: char) -> Result<(), String> {
        self.skip_ws();
        if self.remaining().starts_with(ch) {
            self.pos += ch.len_utf8();
            Ok(())
        } else {
            Err(format!(
                "expected '{ch}' at position {}, found {:?}",
                self.pos,
                self.remaining().chars().next()
            ))
        }
    }

    fn parse_expr(&mut self) -> Result<LogQLExpr, String> {
        self.skip_ws();

        // Check if this is an aggregation: sum/avg/min/max/count
        if let Some(agg_func) = self.try_parse_agg_func() {
            return self.parse_aggregation(agg_func);
        }

        // Check if this is a metric function: count_over_time/rate/etc
        if let Some(metric_func) = self.try_parse_metric_func() {
            return self.parse_metric_query(metric_func);
        }

        // Otherwise it's a log query
        self.parse_log_query()
    }

    fn try_parse_agg_func(&mut self) -> Option<AggFunc> {
        self.skip_ws();
        let rest = self.remaining();
        let funcs = [
            ("sum", AggFunc::Sum),
            ("avg", AggFunc::Avg),
            ("min", AggFunc::Min),
            ("max", AggFunc::Max),
            ("count", AggFunc::Count),
        ];
        for (name, func) in &funcs {
            if rest.starts_with(name) {
                let after = &rest[name.len()..];
                let trimmed = after.trim_start();
                if trimmed.starts_with('(') || trimmed.starts_with("by") {
                    self.pos += name.len();
                    return Some(*func);
                }
            }
        }
        None
    }

    fn try_parse_metric_func(&mut self) -> Option<MetricFunc> {
        self.skip_ws();
        let rest = self.remaining();
        let funcs = [
            ("count_over_time", MetricFunc::CountOverTime),
            ("rate", MetricFunc::Rate),
            ("bytes_over_time", MetricFunc::BytesOverTime),
            ("bytes_rate", MetricFunc::BytesRate),
        ];
        for (name, func) in &funcs {
            if rest.starts_with(name) {
                let after = &rest[name.len()..];
                if after.trim_start().starts_with('(') {
                    self.pos += name.len();
                    return Some(*func);
                }
            }
        }
        None
    }

    fn parse_aggregation(&mut self, op: AggFunc) -> Result<LogQLExpr, String> {
        self.skip_ws();

        // Optional "by (labels)"
        let by_labels = if self.remaining().starts_with("by") {
            self.pos += 2;
            self.skip_ws();
            self.parse_label_list()?
        } else {
            Vec::new()
        };

        self.expect('(')?;
        let inner = self.parse_expr()?;
        self.expect(')')?;

        Ok(LogQLExpr::Aggregation {
            op,
            by_labels,
            inner: Box::new(inner),
        })
    }

    fn parse_metric_query(&mut self, func: MetricFunc) -> Result<LogQLExpr, String> {
        self.expect('(')?;
        let inner = self.parse_log_query()?;

        // Parse range: [5m]
        self.skip_ws();
        self.expect('[')?;
        let range = self.parse_duration()?;
        self.expect(']')?;

        self.expect(')')?;

        Ok(LogQLExpr::MetricQuery {
            func,
            inner: Box::new(inner),
            range,
        })
    }

    fn parse_log_query(&mut self) -> Result<LogQLExpr, String> {
        let selector = self.parse_stream_selector()?;
        let mut pipeline = Vec::new();

        loop {
            self.skip_ws();
            if let Some(stage) = self.try_parse_pipeline_stage() {
                pipeline.push(stage);
            } else {
                break;
            }
        }

        Ok(LogQLExpr::LogQuery { selector, pipeline })
    }

    fn parse_stream_selector(&mut self) -> Result<StreamSelector, String> {
        self.expect('{')?;
        let mut matchers = Vec::new();

        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                break;
            }
            if !matchers.is_empty() {
                self.expect(',')?;
            }
            matchers.push(self.parse_stream_matcher()?);
        }

        self.expect('}')?;
        Ok(StreamSelector { matchers })
    }

    fn parse_stream_matcher(&mut self) -> Result<StreamMatcher, String> {
        self.skip_ws();
        let name = self.parse_ident()?;
        self.skip_ws();

        let op = if self.remaining().starts_with("=~") {
            self.pos += 2;
            MatchOp::Re
        } else if self.remaining().starts_with("!~") {
            self.pos += 2;
            MatchOp::Nre
        } else if self.remaining().starts_with("!=") {
            self.pos += 2;
            MatchOp::Neq
        } else if self.remaining().starts_with('=') {
            self.pos += 1;
            MatchOp::Eq
        } else {
            return Err(format!(
                "expected matcher operator at position {}",
                self.pos
            ));
        };

        self.skip_ws();
        let value = self.parse_string()?;

        Ok(StreamMatcher { name, op, value })
    }

    fn try_parse_pipeline_stage(&mut self) -> Option<PipelineStage> {
        let rest = self.remaining();
        if rest.starts_with("|=") {
            self.pos += 2;
            self.skip_ws();
            if let Ok(s) = self.parse_string() {
                return Some(PipelineStage::LineContains(s));
            }
        } else if rest.starts_with("!=") {
            self.pos += 2;
            self.skip_ws();
            if let Ok(s) = self.parse_string() {
                return Some(PipelineStage::LineNotContains(s));
            }
        } else if rest.starts_with("|~") {
            self.pos += 2;
            self.skip_ws();
            if let Ok(s) = self.parse_string() {
                return Some(PipelineStage::LineMatchesRegex(s));
            }
        } else if rest.starts_with("!~") {
            self.pos += 2;
            self.skip_ws();
            if let Ok(s) = self.parse_string() {
                return Some(PipelineStage::LineNotMatchesRegex(s));
            }
        }
        None
    }

    fn parse_ident(&mut self) -> Result<String, String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'.' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(format!("expected identifier at position {}", self.pos));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.skip_ws();
        let quote = match self.remaining().chars().next() {
            Some('"') => '"',
            Some('`') => '`',
            Some('\'') => '\'',
            _ => return Err(format!("expected quoted string at position {}", self.pos)),
        };
        self.pos += 1;

        let start = self.pos;
        let mut escaped = false;
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if escaped {
                escaped = false;
                self.pos += 1;
            } else if ch == b'\\' && quote != '`' {
                escaped = true;
                self.pos += 1;
            } else if ch == quote as u8 {
                let result = self.input[start..self.pos].to_string();
                self.pos += 1;
                return Ok(result);
            } else {
                self.pos += 1;
            }
        }
        Err("unterminated string".to_string())
    }

    fn parse_label_list(&mut self) -> Result<Vec<String>, String> {
        self.expect('(')?;
        let mut labels = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                break;
            }
            if !labels.is_empty() {
                self.expect(',')?;
            }
            labels.push(self.parse_ident()?);
        }
        self.expect(')')?;
        Ok(labels)
    }

    fn parse_duration(&mut self) -> Result<std::time::Duration, String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        let num_str = &self.input[start..self.pos];
        let num: u64 = num_str
            .parse()
            .map_err(|_| format!("invalid duration number: {num_str}"))?;

        let unit = self
            .remaining()
            .chars()
            .next()
            .ok_or("expected duration unit")?;
        self.pos += unit.len_utf8();

        let secs = match unit {
            's' => num,
            'm' => num * 60,
            'h' => num * 3600,
            'd' => num * 86400,
            'w' => num * 604800,
            _ => return Err(format!("unknown duration unit: {unit}")),
        };

        Ok(std::time::Duration::from_secs(secs))
    }
}

// ---------------------------------------------------------------------------
// SQL translation helpers
// ---------------------------------------------------------------------------

/// Returns true if the given label name maps to a direct column in otel_logs.
pub fn is_direct_column(label: &str) -> bool {
    matches!(
        label,
        "severity_text" | "severity_number" | "body" | "body_type" | "event_name" | "level"
    )
}

/// Apply pipeline stages as post-filters on log lines.
pub fn matches_pipeline(body: &str, pipeline: &[PipelineStage]) -> bool {
    pipeline.iter().all(|stage| match stage {
        PipelineStage::LineContains(s) => body.contains(s.as_str()),
        PipelineStage::LineNotContains(s) => !body.contains(s.as_str()),
        PipelineStage::LineMatchesRegex(pattern) => Regex::new(pattern)
            .map(|re| re.is_match(body))
            .unwrap_or(false),
        PipelineStage::LineNotMatchesRegex(pattern) => Regex::new(pattern)
            .map(|re| !re.is_match(body))
            .unwrap_or(true),
    })
}

/// Check if a log row matches the stream selector matchers.
pub fn matches_stream_selector(
    labels: &std::collections::BTreeMap<String, String>,
    selector: &StreamSelector,
) -> bool {
    selector.matchers.iter().all(|m| {
        let val = labels.get(&m.name).map(|s| s.as_str()).unwrap_or("");
        match &m.op {
            MatchOp::Eq => val == m.value,
            MatchOp::Neq => val != m.value,
            MatchOp::Re => Regex::new(&m.value)
                .map(|re| re.is_match(val))
                .unwrap_or(false),
            MatchOp::Nre => Regex::new(&m.value)
                .map(|re| !re.is_match(val))
                .unwrap_or(true),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_log_query() {
        let expr = parse(r#"{job="app"}"#).unwrap();
        match expr {
            LogQLExpr::LogQuery { selector, pipeline } => {
                assert_eq!(selector.matchers.len(), 1);
                assert_eq!(selector.matchers[0].name, "job");
                assert!(matches!(selector.matchers[0].op, MatchOp::Eq));
                assert_eq!(selector.matchers[0].value, "app");
                assert!(pipeline.is_empty());
            }
            _ => panic!("expected LogQuery"),
        }
    }

    #[test]
    fn parse_log_query_with_pipeline() {
        let expr = parse(r#"{job="app"} |= "error" != "timeout""#).unwrap();
        match expr {
            LogQLExpr::LogQuery { selector, pipeline } => {
                assert_eq!(selector.matchers.len(), 1);
                assert_eq!(pipeline.len(), 2);
                assert!(matches!(pipeline[0], PipelineStage::LineContains(_)));
                assert!(matches!(pipeline[1], PipelineStage::LineNotContains(_)));
            }
            _ => panic!("expected LogQuery"),
        }
    }

    #[test]
    fn parse_metric_query() {
        let expr = parse(r#"count_over_time({job="app"}[5m])"#).unwrap();
        match expr {
            LogQLExpr::MetricQuery { func, range, .. } => {
                assert!(matches!(func, MetricFunc::CountOverTime));
                assert_eq!(range.as_secs(), 300);
            }
            _ => panic!("expected MetricQuery"),
        }
    }

    #[test]
    fn parse_aggregation() {
        let expr = parse(r#"sum by (job) (count_over_time({job="app"}[5m]))"#).unwrap();
        match expr {
            LogQLExpr::Aggregation { op, by_labels, .. } => {
                assert!(matches!(op, AggFunc::Sum));
                assert_eq!(by_labels, vec!["job"]);
            }
            _ => panic!("expected Aggregation"),
        }
    }
}
