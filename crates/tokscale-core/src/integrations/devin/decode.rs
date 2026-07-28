//! Devin CLI session decoder.
//!
//! Parses assistant token metrics from the Devin CLI SQLite session store:
//! - `~/.local/share/devin/cli/sessions.db`
//!
//! The Devin CLI records conversation turns in the `message_nodes` table as a
//! forest of JSON `chat_message` blobs linked by `parent_node_id`. Tokscale
//! accounts only for token-bearing assistant turns: each `message_nodes` row
//! whose `chat_message.role = "assistant"` carries an optional
//! `metadata.metrics` object with `input_tokens`, `output_tokens`,
//! `cache_read_tokens`, and `cache_creation_tokens`. Those four buckets map
//! directly to Tokscale's `TokenBreakdown`; `total_time_ms` is streaming
//! timing and is dropped.
//!
//! The forest structure (twin child nodes, cross-root forks, rewinds) is not
//! reconstructed. Twin nodes share the same `message_id` and the same
//! metrics, so usage is deduplicated by `(session_id, message_id)` and
//! counted once per session. Rows without `message_id` fall back to
//! `node-<id>` and are kept individually.
//!
//! Session-level `metadata.total_credit_cost` and `total_acu_cost` are
//! ignored: Tokscale derives local usage cost from token buckets and its own
//! pricing table, never from vendor credits or balances (ADR 0001). The
//! session `model` label is preserved verbatim and canonicalized by the
//! shared pricing/identity pipeline. Provider attribution is inferred from
//! the model id (for example `claude-opus-4-8-medium` → `anthropic`);
//! unresolvable providers stay `unknown` rather than being guessed.
//!
//! Rows with all token buckets equal to zero are ignored, so failed
//! generations with no billable usage do not create usage rows. Hidden
//! sessions (`sessions.hidden = 1`) are excluded from discovery.

use crate::input_health::{InputFailure, RecordRejectionReason, ScannedInput};
use crate::records::error::{SessionParseError, SessionParseResult};
use crate::records::utils::open_readonly_sqlite;
use crate::records::{dedup_hash_str, ParsedMessage};
use crate::{provider_identity, TokenBreakdown};
use serde_json::Value;
use std::path::Path;

const DEVIN_AGENT_NAME: &str = "Devin";

pub fn parse_devin_sqlite(db_path: &Path) -> SessionParseResult<ScannedInput> {
    let conn = open_readonly_sqlite(db_path)?;

    let query = r#"
        SELECT
            s.id,
            s.model,
            n.created_at,
            n.chat_message
        FROM message_nodes n
        JOIN sessions s ON s.id = n.session_id
        WHERE s.hidden = 0
        ORDER BY n.row_id
    "#;

    let mut stmt = conn
        .prepare(query)
        .map_err(|error| SessionParseError::new("prepare Devin message query", error))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| SessionParseError::new("execute Devin message query", error))?;

    let mut scanned = ScannedInput::default();
    let mut row_index = 0usize;
    loop {
        let row = match rows.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(error) => {
                scanned.interrupted = Some(InputFailure::new(
                    "iterate Devin message rows",
                    format!("{} after row {row_index}: {error}", db_path.display()),
                ));
                break;
            }
        };
        row_index += 1;

        let session_id: String = match row.get(0) {
            Ok(value) => value,
            Err(_) => {
                scanned
                    .rejections
                    .record(RecordRejectionReason::MalformedRecord);
                continue;
            }
        };
        let model_id: Option<String> = row.get(1).ok();
        let created_at_sec: i64 = match row.get::<_, Option<i64>>(2) {
            Ok(Some(value)) => value,
            _ => {
                scanned
                    .rejections
                    .record(RecordRejectionReason::MissingTimestamp);
                continue;
            }
        };
        let chat_message: String = match row.get(3) {
            Ok(value) => value,
            Err(_) => {
                scanned
                    .rejections
                    .record(RecordRejectionReason::MalformedRecord);
                continue;
            }
        };

        match extract_assistant_usage(&chat_message) {
            Ok(Some(metrics)) => {
                let Some(model_id) = model_id
                    .map(|model| model.trim().to_string())
                    .filter(|model| !model.is_empty())
                else {
                    scanned
                        .rejections
                        .record(RecordRejectionReason::MissingModel);
                    continue;
                };
                let tokens = TokenBreakdown {
                    input: metrics.input,
                    output: metrics.output,
                    cache_read: metrics.cache_read,
                    cache_write: metrics.cache_write,
                    reasoning: 0,
                };
                let Some(token_total) = tokens.checked_total() else {
                    scanned
                        .rejections
                        .record(RecordRejectionReason::MalformedRecord);
                    continue;
                };
                if token_total == 0 {
                    continue;
                }
                let timestamp = created_at_sec.checked_mul(1000).filter(|ms| *ms > 0);
                let Some(timestamp) = timestamp else {
                    scanned
                        .rejections
                        .record(RecordRejectionReason::MissingTimestamp);
                    continue;
                };
                let provider = provider_identity::observed_provider_id("", &model_id);
                let dedup_key = Some(dedup_hash_str(&format!(
                    "devin:{session_id}:{}",
                    metrics
                        .message_id
                        .as_deref()
                        .unwrap_or(&metrics.node_fallback)
                )));
                let mut msg = ParsedMessage::new_with_agent(
                    model_id,
                    provider,
                    session_id,
                    timestamp,
                    tokens,
                    0.0,
                    Some(DEVIN_AGENT_NAME.to_string()),
                );
                msg.dedup_key = dedup_key;
                scanned.messages.push(msg);
            }
            Ok(None) => {}
            Err(DevinMetricError::MalformedJson) => {
                scanned
                    .rejections
                    .record(RecordRejectionReason::MalformedRecord);
            }
        }
    }
    Ok(scanned)
}

struct DevinMetrics {
    input: i64,
    output: i64,
    cache_read: i64,
    cache_write: i64,
    message_id: Option<String>,
    node_fallback: String,
}

enum DevinMetricError {
    MalformedJson,
}

/// Extract token metrics from an assistant `chat_message` JSON blob.
///
/// Returns `Ok(None)` for non-assistant rows, assistant rows without
/// metrics, and rows whose metrics object is null or empty — these are not
/// usage rows and are silently skipped without a rejection. Returns
/// `Err(MalformedJson)` only when the blob cannot be parsed as JSON.
fn extract_assistant_usage(chat_message: &str) -> Result<Option<DevinMetrics>, DevinMetricError> {
    let parsed: Value = match serde_json::from_str(chat_message) {
        Ok(value) => value,
        Err(_) => return Err(DevinMetricError::MalformedJson),
    };

    let role = parsed
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if role != "assistant" {
        return Ok(None);
    }

    let message_id = parsed
        .get("message_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let node_fallback = message_id.clone().unwrap_or_default();

    let metrics = match parsed
        .get("metadata")
        .and_then(|metadata| metadata.get("metrics"))
    {
        Some(Value::Null) | None => return Ok(None),
        Some(metrics) => metrics,
    };

    let input = non_negative_i64(metrics.get("input_tokens"));
    let output = non_negative_i64(metrics.get("output_tokens"));
    let cache_read = non_negative_i64(metrics.get("cache_read_tokens"));
    let cache_write = non_negative_i64(metrics.get("cache_creation_tokens"));

    if input.is_none() && output.is_none() && cache_read.is_none() && cache_write.is_none() {
        return Ok(None);
    }

    Ok(Some(DevinMetrics {
        input: input.unwrap_or(0),
        output: output.unwrap_or(0),
        cache_read: cache_read.unwrap_or(0),
        cache_write: cache_write.unwrap_or(0),
        message_id,
        node_fallback,
    }))
}

/// Map a JSON token count to a non-negative `i64`. Negative values are
/// treated as malformed (returned as `None` so the caller can reject the
/// row); null/absent fields return `None` so the caller can treat the
/// bucket as zero.
fn non_negative_i64(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    if value.is_null() {
        return None;
    }
    let numeric = value
        .as_i64()
        .or_else(|| value.as_u64().map(|v| v as i64))
        .or_else(|| value.as_f64().map(|v| v as i64))?;
    if numeric < 0 {
        return None;
    }
    Some(numeric)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection};

    fn create_devin_db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                working_directory TEXT NOT NULL,
                backend_type TEXT NOT NULL,
                model TEXT NOT NULL,
                agent_mode TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                title TEXT,
                main_chain_id INTEGER,
                shell_last_seen_index INTEGER DEFAULT 0,
                cogs_json TEXT,
                workspace_dirs TEXT,
                hidden INTEGER NOT NULL DEFAULT 0,
                metadata TEXT
            );
            CREATE TABLE message_nodes (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                node_id INTEGER NOT NULL,
                parent_node_id INTEGER,
                chat_message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                metadata TEXT,
                UNIQUE(session_id, node_id)
            );",
        )
        .unwrap();
        conn
    }

    fn insert_session(
        conn: &Connection,
        id: &str,
        model: &str,
        hidden: bool,
        metadata: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO sessions
                (id, working_directory, backend_type, model, agent_mode, created_at,
                 last_activity_at, title, main_chain_id, hidden, metadata)
             VALUES (?1, ?2, 'Windsurf', ?3, 'normal', 1752000000, 1752003600, NULL, NULL, ?4, ?5)",
            params![id, "/work", model, if hidden { 1 } else { 0 }, metadata],
        )
        .unwrap();
    }

    fn insert_node(
        conn: &Connection,
        session_id: &str,
        node_id: i64,
        parent_node_id: Option<i64>,
        chat_message: &str,
        created_at: i64,
    ) {
        conn.execute(
            "INSERT INTO message_nodes
                (session_id, node_id, parent_node_id, chat_message, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session_id,
                node_id,
                parent_node_id,
                chat_message,
                created_at
            ],
        )
        .unwrap();
    }

    fn assistant_message(message_id: &str, metrics: Option<&str>) -> String {
        let metadata = match metrics {
            Some(metrics) => format!(r#","metadata":{{"metrics":{metrics}}}"#),
            None => String::new(),
        };
        format!(r#"{{"message_id":"{message_id}","role":"assistant","content":"hi"{metadata}}}"#)
    }

    #[test]
    fn parse_devin_sqlite_reports_missing_schema_as_input_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        drop(Connection::open(&path).unwrap());

        let error = parse_devin_sqlite(&path).unwrap_err();
        assert_eq!(error.operation(), "prepare Devin message query");
    }

    #[test]
    fn extracts_assistant_metrics_and_ignores_non_assistant_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "sunny-forest", "claude-opus-4-8-medium", false, None);
        insert_node(
            &conn,
            "sunny-forest",
            0,
            None,
            r#"{"message_id":"m-sys","role":"system","content":"intro"}"#,
            1752000000,
        );
        insert_node(
            &conn,
            "sunny-forest",
            1,
            Some(0),
            r#"{"message_id":"m-user","role":"user","content":"hi","metadata":{"is_user_input":true}}"#,
            1752000010,
        );
        insert_node(
            &conn,
            "sunny-forest",
            2,
            Some(1),
            &assistant_message(
                "m-asst-2",
                Some(
                    r#"{"input_tokens":100,"output_tokens":20,"cache_creation_tokens":177,"cache_read_tokens":null,"total_time_ms":1500}"#,
                ),
            ),
            1752000020,
        );
        insert_node(
            &conn,
            "sunny-forest",
            4,
            Some(2),
            r#"{"message_id":"m-tool-4","role":"tool","tool_call_id":"tc-1","content":"ok"}"#,
            1752000030,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 1);
        let msg = &scanned.messages[0];
        assert_eq!(msg.model_id.as_ref(), "claude-opus-4-8-medium");
        assert_eq!(msg.provider_id.as_ref(), "anthropic");
        assert_eq!(msg.session_id.as_ref(), "sunny-forest");
        assert_eq!(msg.timestamp, 1_752_000_020_000);
        assert_eq!(msg.tokens.input, 100);
        assert_eq!(msg.tokens.output, 20);
        assert_eq!(msg.tokens.cache_read, 0);
        assert_eq!(msg.tokens.cache_write, 177);
        assert_eq!(msg.tokens.reasoning, 0);
        assert_eq!(msg.cost, 0.0);
        assert_eq!(msg.agent.as_deref(), Some("Devin"));
        assert!(scanned.rejections.is_empty());
        assert!(scanned.interrupted.is_none());
    }

    #[test]
    fn twin_nodes_sharing_message_id_are_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "sunny-forest", "claude-opus-4-8-medium", false, None);
        let metrics = r#"{"input_tokens":100,"output_tokens":20,"cache_creation_tokens":177}"#;
        // Twin child nodes share the same message_id and metrics.
        insert_node(
            &conn,
            "sunny-forest",
            2,
            Some(1),
            &assistant_message("m-asst-2", Some(metrics)),
            1752000020,
        );
        insert_node(
            &conn,
            "sunny-forest",
            3,
            Some(1),
            &assistant_message("m-asst-2", Some(metrics)),
            1752000021,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 2);
        // The fold layer deduplicates by dedup_key; both rows carry the same key.
        assert_eq!(scanned.messages[0].dedup_key, scanned.messages[1].dedup_key);
        assert!(scanned.messages[0].dedup_key.is_some());
    }

    #[test]
    fn assistant_without_metrics_is_silently_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "quiet-pond", "devin-mini", false, None);
        insert_node(
            &conn,
            "quiet-pond",
            0,
            None,
            &assistant_message("m-asst-0", None),
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert!(scanned.messages.is_empty());
        assert!(scanned.rejections.is_empty());
    }

    #[test]
    fn zero_token_metrics_are_filtered_without_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "quiet-pond", "devin-mini", false, None);
        insert_node(
            &conn,
            "quiet-pond",
            0,
            None,
            &assistant_message("m-asst-0", Some(r#"{"input_tokens":0,"output_tokens":0}"#)),
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert!(scanned.messages.is_empty());
        assert!(scanned.rejections.is_empty());
    }

    #[test]
    fn hidden_sessions_are_excluded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "visible", "claude-opus-4-8-medium", false, None);
        insert_session(&conn, "hidden-one", "claude-opus-4-8-medium", true, None);
        let metrics = r#"{"input_tokens":10,"output_tokens":2}"#;
        insert_node(
            &conn,
            "visible",
            0,
            None,
            &assistant_message("m-v-0", Some(metrics)),
            1752000000,
        );
        insert_node(
            &conn,
            "hidden-one",
            0,
            None,
            &assistant_message("m-h-0", Some(metrics)),
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 1);
        assert_eq!(scanned.messages[0].session_id.as_ref(), "visible");
    }

    #[test]
    fn credit_cost_metadata_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(
            &conn,
            "sunny-forest",
            "claude-opus-4-8-medium",
            false,
            Some(r#"{"total_credit_cost":7,"total_acu_cost":0.0}"#),
        );
        insert_node(
            &conn,
            "sunny-forest",
            0,
            None,
            &assistant_message("m-asst-0", Some(r#"{"input_tokens":10,"output_tokens":2}"#)),
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 1);
        // Cost is token-derived; vendor credits are not mixed in.
        assert_eq!(scanned.messages[0].cost, 0.0);
    }

    #[test]
    fn token_bearing_row_without_model_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        conn.execute(
            "INSERT INTO sessions
                (id, working_directory, backend_type, model, agent_mode, created_at,
                 last_activity_at, title, main_chain_id, hidden, metadata)
             VALUES ('no-model', '/work', 'Windsurf', '', 'normal', 1752000000, 1752003600, NULL, NULL, 0, NULL)",
            [],
        )
        .unwrap();
        insert_node(
            &conn,
            "no-model",
            0,
            None,
            &assistant_message("m-asst-0", Some(r#"{"input_tokens":10,"output_tokens":2}"#)),
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert!(scanned.messages.is_empty());
        let rejection = scanned.rejections.entries().next().unwrap();
        assert_eq!(rejection.key, "missing-model");
    }

    #[test]
    fn unresolvable_provider_stays_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "odd-cove", "devin-mini", false, None);
        insert_node(
            &conn,
            "odd-cove",
            0,
            None,
            &assistant_message("m-asst-0", Some(r#"{"input_tokens":10,"output_tokens":2}"#)),
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 1);
        assert_eq!(scanned.messages[0].provider_id.as_ref(), "unknown");
    }

    #[test]
    fn malformed_chat_message_json_is_rejected_but_scan_continues() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "sunny-forest", "claude-opus-4-8-medium", false, None);
        insert_node(&conn, "sunny-forest", 0, None, "not-json{{", 1752000000);
        insert_node(
            &conn,
            "sunny-forest",
            1,
            Some(0),
            &assistant_message("m-asst-1", Some(r#"{"input_tokens":10,"output_tokens":2}"#)),
            1752000010,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 1);
        let rejection = scanned.rejections.entries().next().unwrap();
        assert_eq!(rejection.key, "malformed-record");
    }

    #[test]
    fn node_without_message_id_uses_node_fallback_for_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = create_devin_db(&path);
        insert_session(&conn, "odd-cove", "claude-opus-4-8-medium", false, None);
        // Assistant message without message_id — dedup falls back to empty
        // string, so each such node is kept individually.
        insert_node(
            &conn,
            "odd-cove",
            0,
            None,
            r#"{"role":"assistant","content":"hi","metadata":{"metrics":{"input_tokens":10,"output_tokens":2}}}"#,
            1752000000,
        );
        drop(conn);

        let scanned = parse_devin_sqlite(&path).unwrap();
        assert_eq!(scanned.messages.len(), 1);
        assert!(scanned.messages[0].dedup_key.is_some());
    }
}
