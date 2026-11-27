//! PostgreSQL Wire Protocol v3.0 TCP server (`Simple Query` + `Extended Query` + Auth).
//!
//! Zero-dependency `std::net` implementation enforcing `unsafe_code = "deny"` and
//! `unwrap_used = "deny"`, compatible with `psql`, DBeaver, Navicat, and `psycopg2`.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use query_engine::{KvError, SqlError, SqlResult, format_value};
use table_codec::CodecError;

use crate::ServerConfig;
use crate::backend::{ServerSession, SqlBackend};
use crate::pg_catalog::{OID_TEXT, try_handle_pg_compat, value_to_pg_oid};

const SSL_REQUEST_CODE: u32 = 80_877_103;
const PROTOCOL_V3_0: u32 = 196_608;
const MAX_MESSAGE_LEN: usize = 16 * 1024 * 1024;

/// Running handle to a bound PostgreSQL wire-protocol server.
pub struct PgServer {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl PgServer {
    /// Binds to `config.listen_addr` and starts accepting PostgreSQL v3.0 TCP connections.
    pub fn start<B: SqlBackend>(backend: Arc<B>, config: ServerConfig) -> io::Result<Self> {
        let listener = TcpListener::bind(&config.listen_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        if !local_addr.ip().is_loopback() && config.password.is_none() {
            eprintln!(
                "[SECURITY WARNING] AnchorDB is listening on non-loopback address {local_addr} without password authentication!"
            );
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_flag = Arc::clone(&shutdown);
        let config_arc = Arc::new(config);

        let handle = thread::spawn(move || {
            while !shutdown_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _peer)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_nodelay(true);
                        let b = Arc::clone(&backend);
                        let c = Arc::clone(&config_arc);
                        thread::spawn(move || {
                            let _ = handle_connection(stream, b.as_ref(), c.as_ref());
                        });
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            local_addr,
            shutdown,
            accept_thread: Some(handle),
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Signals the accept loop to stop and waits for it to exit cleanly.
    pub fn stop(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PgServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedStmt {
    sql: String,
    param_count: usize,
}

#[derive(Debug, Clone)]
struct BoundPortal {
    resolved_sql: String,
    described_failed: bool,
    cached_results: Option<Vec<SqlResult>>,
}

fn handle_connection<B: SqlBackend>(
    mut stream: TcpStream,
    backend: &B,
    config: &ServerConfig,
) -> io::Result<()> {
    let (user_name, database_name) = match perform_startup(&mut stream, config)? {
        Some(pair) => pair,
        None => return Ok(()),
    };

    let mut session = backend.new_session();
    let mut prepared: HashMap<String, PreparedStmt> = HashMap::new();
    let mut portals: HashMap<String, BoundPortal> = HashMap::new();

    while let Some((tag, payload)) = read_frontend_message(&mut stream)? {
        match tag {
            b'Q' => {
                // Simple Query protocol.
                let sql = read_cstring(&payload, 0)
                    .map(|(s, _)| s)
                    .unwrap_or_default();
                execute_and_write_results(
                    &mut stream,
                    &mut session,
                    &sql,
                    &database_name,
                    &user_name,
                )?;
                write_ready_for_query(&mut stream, session.in_transaction())?;
                stream.flush()?;
            }
            b'P' => {
                // Extended Query: Parse(stmt_name, query, param_type_count, param_oids...)
                match handle_parse(&payload, &mut prepared) {
                    Ok(()) => write_simple_tag(&mut stream, b'1')?,
                    Err(msg) => write_error_response(&mut stream, "42601", &msg)?,
                }
            }
            b'B' => {
                // Extended Query: Bind(portal_name, stmt_name, formats..., params...)
                match handle_bind(&payload, &prepared, &mut portals) {
                    Ok(()) => write_simple_tag(&mut stream, b'2')?,
                    Err(msg) => write_error_response(&mut stream, "08P01", &msg)?,
                }
            }
            b'D' => {
                // Extended Query: Describe('S' | 'P', name)
                handle_describe(
                    &mut stream,
                    &payload,
                    &mut session,
                    &prepared,
                    &mut portals,
                    &database_name,
                    &user_name,
                )?;
            }
            b'E' => {
                // Extended Query: Execute(portal_name, max_rows)
                // Per PostgreSQL v3 protocol, Execute NEVER emits RowDescription ('T');
                // RowDescription is emitted only by Describe('S') / Describe('P').
                let portal_name = read_cstring(&payload, 0)
                    .map(|(s, _)| s)
                    .unwrap_or_default();
                if let Some(portal) = portals.get_mut(&portal_name) {
                    if portal.described_failed {
                        continue;
                    }
                    let sql = portal.resolved_sql.clone();
                    if let Some(cached) = portal.cached_results.take() {
                        write_sql_results(&mut stream, &sql, cached, false)?;
                    } else {
                        execute_and_write_results_opts(
                            &mut stream,
                            &mut session,
                            &sql,
                            &database_name,
                            &user_name,
                            false,
                        )?;
                    }
                } else {
                    write_error_response(
                        &mut stream,
                        "34000",
                        &format!("portal '{portal_name}' does not exist"),
                    )?;
                }
            }
            b'C' => {
                // Close('S' | 'P', name)
                if let Some(&kind) = payload.first() {
                    let name = read_cstring(&payload, 1)
                        .map(|(s, _)| s)
                        .unwrap_or_default();
                    match kind {
                        b'S' => {
                            prepared.remove(&name);
                        }
                        b'P' => {
                            portals.remove(&name);
                        }
                        _ => {}
                    }
                }
                write_simple_tag(&mut stream, b'3')?;
            }
            b'H' => {
                // Flush
                stream.flush()?;
            }
            b'S' => {
                // Sync
                write_ready_for_query(&mut stream, session.in_transaction())?;
                stream.flush()?;
            }
            b'X' => {
                // Terminate
                break;
            }
            other => {
                write_error_response(
                    &mut stream,
                    "0A000",
                    &format!("unsupported frontend message type '{}'", other as char),
                )?;
                write_ready_for_query(&mut stream, session.in_transaction())?;
                stream.flush()?;
            }
        }
    }

    session.rollback_if_active();
    Ok(())
}

fn perform_startup(
    stream: &mut TcpStream,
    config: &ServerConfig,
) -> io::Result<Option<(String, String)>> {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err),
        }
        let total_len = u32::from_be_bytes(len_buf) as usize;
        if !(8..=MAX_MESSAGE_LEN).contains(&total_len) {
            return Ok(None);
        }
        let mut body = vec![0u8; total_len - 4];
        stream.read_exact(&mut body)?;

        let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        if code == SSL_REQUEST_CODE {
            // Decline TLS upgrade so standard clients fall back to plain TCP.
            stream.write_all(b"N")?;
            stream.flush()?;
            continue;
        }
        if code != PROTOCOL_V3_0 {
            write_error_response(
                stream,
                "0A000",
                &format!("unsupported PostgreSQL protocol code {code}"),
            )?;
            stream.flush()?;
            return Ok(None);
        }

        // Parse startup key-value pairs.
        let mut params: HashMap<String, String> = HashMap::new();
        let mut cursor = 4usize;
        while cursor < body.len() && body[cursor] != 0 {
            let Some((k, next)) = read_cstring(&body, cursor) else {
                break;
            };
            let Some((v, next2)) = read_cstring(&body, next) else {
                break;
            };
            params.insert(k, v);
            cursor = next2;
        }

        let user = params
            .get("user")
            .cloned()
            .unwrap_or_else(|| "postgres".to_owned());
        let database = params
            .get("database")
            .cloned()
            .unwrap_or_else(|| user.clone());

        // Optional CleartextPassword authentication.
        if let Some(expected_pwd) = &config.password {
            write_auth_cleartext_request(stream)?;
            stream.flush()?;
            let Some((tag, pwd_payload)) = read_frontend_message(stream)? else {
                return Ok(None);
            };
            let provided = if tag == b'p' {
                read_cstring(&pwd_payload, 0)
                    .map(|(s, _)| s)
                    .unwrap_or_default()
            } else {
                String::new()
            };
            if provided != *expected_pwd {
                write_error_response(
                    stream,
                    "28P01",
                    &format!("password authentication failed for user \"{user}\""),
                )?;
                stream.flush()?;
                return Ok(None);
            }
        }

        // Send AuthenticationOk + standard PostgreSQL ParameterStatus set + ReadyForQuery.
        write_auth_ok(stream)?;
        for (k, v) in [
            ("server_version", "15.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("standard_conforming_strings", "on"),
            ("DateStyle", "ISO, MDY"),
            ("TimeZone", "UTC"),
            ("integer_datetimes", "on"),
            ("is_superuser", "on"),
            ("session_authorization", user.as_str()),
        ] {
            write_parameter_status(stream, k, v)?;
        }
        write_backend_key_data(stream, std::process::id() as i32, 0x414E_4352)?;
        write_ready_for_query(stream, false)?;
        stream.flush()?;

        return Ok(Some((user, database)));
    }
}

fn execute_and_write_results<S: ServerSession>(
    stream: &mut TcpStream,
    session: &mut S,
    sql: &str,
    database_name: &str,
    user_name: &str,
) -> io::Result<()> {
    execute_and_write_results_opts(stream, session, sql, database_name, user_name, true)
}

fn run_sql_with_compat<S: ServerSession>(
    session: &mut S,
    trimmed_sql: &str,
    database_name: &str,
    user_name: &str,
) -> Result<Vec<SqlResult>, SqlError> {
    match try_handle_pg_compat(session, trimmed_sql, database_name, user_name)? {
        Some(results) => Ok(results),
        None => session.execute(trimmed_sql),
    }
}

fn execute_and_write_results_opts<S: ServerSession>(
    stream: &mut TcpStream,
    session: &mut S,
    sql: &str,
    database_name: &str,
    user_name: &str,
    send_row_desc: bool,
) -> io::Result<()> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        write_empty_query_response(stream)?;
        return Ok(());
    }

    match run_sql_with_compat(session, trimmed, database_name, user_name) {
        Ok(results) => write_sql_results(stream, trimmed, results, send_row_desc),
        Err(err) => {
            let code = sql_error_to_pg_state(&err);
            write_error_response(stream, code, &err.to_string())
        }
    }
}

fn write_sql_results(
    stream: &mut TcpStream,
    trimmed_sql: &str,
    results: Vec<SqlResult>,
    send_row_desc: bool,
) -> io::Result<()> {
    if results.is_empty() {
        write_empty_query_response(stream)?;
        return Ok(());
    }
    for res in results {
        match res {
            SqlResult::Rows { columns, rows } => {
                if send_row_desc {
                    let col_oids = infer_column_oids(&columns, &rows);
                    write_row_description(stream, &columns, &col_oids)?;
                }
                for row in &rows {
                    write_data_row(stream, row)?;
                }
                write_command_complete(stream, &format!("SELECT {}", rows.len()))?;
            }
            SqlResult::Affected { count } => {
                let tag = infer_dml_tag(trimmed_sql, count);
                write_command_complete(stream, &tag)?;
            }
            SqlResult::Message(msg) => {
                write_command_complete(stream, &msg)?;
            }
        }
    }
    Ok(())
}

fn infer_column_oids(columns: &[String], rows: &[Vec<table_codec::Value>]) -> Vec<i32> {
    (0..columns.len())
        .map(|c_idx| {
            rows.iter()
                .find_map(|r| r.get(c_idx).filter(|v| !v.is_null()).map(value_to_pg_oid))
                .unwrap_or(OID_TEXT)
        })
        .collect()
}

fn infer_dml_tag(sql: &str, count: usize) -> String {
    let upper = sql.trim_start().to_uppercase();
    if upper.starts_with("INSERT") {
        format!("INSERT 0 {count}")
    } else if upper.starts_with("UPDATE") {
        format!("UPDATE {count}")
    } else if upper.starts_with("DELETE") {
        format!("DELETE {count}")
    } else {
        format!("OK {count}")
    }
}

fn sql_error_to_pg_state(err: &SqlError) -> &'static str {
    match err {
        SqlError::Parse(_) => "42601",
        SqlError::Bind(_) => "42703",
        SqlError::Unsupported(_) => "0A000",
        SqlError::MemoryBudgetExceeded { .. } => "53200",
        SqlError::Kv(KvError::WriteConflict) => "40001",
        SqlError::Codec(
            CodecError::DuplicatePrimaryKey | CodecError::DuplicateUniqueKey { .. },
        ) => "23505",
        SqlError::Codec(CodecError::NullConstraintViolation { .. }) => "23502",
        SqlError::Codec(CodecError::TableNotFound(_)) => "42P01",
        SqlError::Codec(CodecError::TableAlreadyExists(_)) => "42P07",
        _ => "XX000",
    }
}

// ============================================================================
// Extended Query Protocol Helpers (Parse / Bind / Describe)
// ============================================================================

fn handle_parse(
    payload: &[u8],
    prepared: &mut HashMap<String, PreparedStmt>,
) -> Result<(), String> {
    let (stmt_name, pos1) = read_cstring(payload, 0).ok_or("malformed Parse statement name")?;
    let (sql, pos2) = read_cstring(payload, pos1).ok_or("malformed Parse query string")?;
    let declared_params = if pos2 + 2 <= payload.len() {
        u16::from_be_bytes([payload[pos2], payload[pos2 + 1]]) as usize
    } else {
        0
    };
    let inferred_params = count_dollar_params(&sql);
    prepared.insert(
        stmt_name,
        PreparedStmt {
            sql,
            param_count: declared_params.max(inferred_params),
        },
    );
    Ok(())
}

fn handle_bind(
    payload: &[u8],
    prepared: &HashMap<String, PreparedStmt>,
    portals: &mut HashMap<String, BoundPortal>,
) -> Result<(), String> {
    let (portal_name, mut pos) = read_cstring(payload, 0).ok_or("malformed Bind portal")?;
    let (stmt_name, next) = read_cstring(payload, pos).ok_or("malformed Bind stmt")?;
    pos = next;

    let stmt = prepared
        .get(&stmt_name)
        .ok_or_else(|| format!("prepared statement '{stmt_name}' does not exist"))?;

    // Skip parameter format codes.
    let fmt_count = read_be_u16(payload, &mut pos)? as usize;
    pos = pos
        .checked_add(fmt_count * 2)
        .filter(|&p| p <= payload.len())
        .ok_or("malformed Bind parameter format codes")?;

    // Read parameter values.
    let param_count = read_be_u16(payload, &mut pos)? as usize;
    let mut params: Vec<Option<String>> = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        let len = read_be_i32(payload, &mut pos)?;
        if len < 0 {
            params.push(None);
        } else {
            let ulen = len as usize;
            let end = pos
                .checked_add(ulen)
                .filter(|&e| e <= payload.len())
                .ok_or("truncated Bind parameter value")?;
            let text = String::from_utf8_lossy(&payload[pos..end]).into_owned();
            params.push(Some(text));
            pos = end;
        }
    }

    let resolved_sql = substitute_dollar_params(&stmt.sql, &params);
    portals.insert(
        portal_name,
        BoundPortal {
            resolved_sql,
            described_failed: false,
            cached_results: None,
        },
    );
    Ok(())
}

fn handle_describe<S: ServerSession>(
    stream: &mut TcpStream,
    payload: &[u8],
    session: &mut S,
    prepared: &HashMap<String, PreparedStmt>,
    portals: &mut HashMap<String, BoundPortal>,
    database_name: &str,
    user_name: &str,
) -> io::Result<()> {
    let Some(&kind) = payload.first() else {
        write_error_response(stream, "08P01", "malformed Describe message")?;
        return Ok(());
    };
    let name = read_cstring(payload, 1).map(|(s, _)| s).unwrap_or_default();
    match kind {
        b'S' => {
            if let Some(stmt) = prepared.get(&name) {
                write_parameter_description(stream, stmt.param_count)?;
                let trimmed = stmt.sql.trim();
                if is_read_only_sql(trimmed) {
                    let probe_sql = if stmt.param_count > 0 {
                        let nulls = vec![None; stmt.param_count];
                        substitute_dollar_params(trimmed, &nulls)
                    } else {
                        trimmed.to_owned()
                    };
                    if let Ok(results) =
                        run_sql_with_compat(session, &probe_sql, database_name, user_name)
                        && let Some(SqlResult::Rows { columns, rows }) = results.first()
                    {
                        let oids = infer_column_oids(columns, rows);
                        write_row_description(stream, columns, &oids)?;
                        return Ok(());
                    }
                }
                write_simple_tag(stream, b'n')?;
            } else {
                write_error_response(
                    stream,
                    "26000",
                    &format!("prepared statement '{name}' does not exist"),
                )?;
            }
        }
        b'P' => {
            let Some(portal) = portals.get_mut(&name) else {
                write_error_response(stream, "34000", &format!("portal '{name}' does not exist"))?;
                return Ok(());
            };
            let trimmed = portal.resolved_sql.trim();
            if trimmed.is_empty() {
                write_simple_tag(stream, b'n')?;
                return Ok(());
            }
            // Per PostgreSQL v3 Extended Query spec (strictly enforced by pgjdbc/DBeaver):
            // Describe('P') MUST send RowDescription ('T') for row-returning portals and
            // Execute('E') MUST NOT resend RowDescription.
            match run_sql_with_compat(session, trimmed, database_name, user_name) {
                Ok(results) => {
                    if let Some(SqlResult::Rows { columns, rows }) = results.first() {
                        let oids = infer_column_oids(columns, rows);
                        write_row_description(stream, columns, &oids)?;
                    } else {
                        write_simple_tag(stream, b'n')?;
                    }
                    portal.cached_results = Some(results);
                }
                Err(err) => {
                    portal.described_failed = true;
                    let code = sql_error_to_pg_state(&err);
                    write_error_response(stream, code, &err.to_string())?;
                }
            }
        }
        _ => {
            write_error_response(stream, "08P01", "invalid Describe target")?;
        }
    }
    Ok(())
}

fn is_read_only_sql(trimmed: &str) -> bool {
    let lower = trimmed.to_lowercase();
    lower.starts_with("select") || lower.starts_with("show") || lower.starts_with("explain")
}

fn count_dollar_params(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max_idx = 0usize;
    let mut i = 0usize;
    let mut in_single = false;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_single = !in_single;
            i += 1;
            continue;
        }
        if !in_single && bytes[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start
                && let Ok(n) = sql[start..end].parse::<usize>()
            {
                max_idx = max_idx.max(n);
            }
            i = end;
            continue;
        }
        i += 1;
    }
    max_idx
}

fn substitute_dollar_params(sql: &str, params: &[Option<String>]) -> String {
    if params.is_empty() {
        return sql.to_owned();
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 32);
    let mut i = 0usize;
    let mut in_single = false;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_single = !in_single;
            out.push('\'');
            i += 1;
            continue;
        }
        if !in_single && bytes[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start
                && let Ok(idx) = sql[start..end].parse::<usize>()
                && idx >= 1
                && let Some(param) = params.get(idx - 1)
            {
                match param {
                    None => out.push_str("NULL"),
                    Some(raw) => {
                        if raw.parse::<i64>().is_ok()
                            || raw.parse::<f64>().is_ok()
                            || raw.eq_ignore_ascii_case("true")
                            || raw.eq_ignore_ascii_case("false")
                        {
                            out.push_str(raw);
                        } else {
                            out.push('\'');
                            out.push_str(&raw.replace('\'', "''"));
                            out.push('\'');
                        }
                    }
                }
                i = end;
                continue;
            }
        }
        if let Some(ch) = sql[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }
    out
}

// ============================================================================
// Wire Framing Read / Write Primitives
// ============================================================================

fn read_frontend_message(stream: &mut TcpStream) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 5];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let tag = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if !(4..=MAX_MESSAGE_LEN).contains(&len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frontend message length",
        ));
    }
    let mut payload = vec![0u8; len - 4];
    stream.read_exact(&mut payload)?;
    Ok(Some((tag, payload)))
}

fn read_cstring(buf: &[u8], start: usize) -> Option<(String, usize)> {
    let slice = buf.get(start..)?;
    let nul_pos = slice.iter().position(|&b| b == 0)?;
    let text = String::from_utf8_lossy(&slice[..nul_pos]).into_owned();
    Some((text, start + nul_pos + 1))
}

fn read_be_u16(buf: &[u8], pos: &mut usize) -> Result<u16, String> {
    let end = pos.checked_add(2).ok_or("u16 overflow")?;
    let s = buf.get(*pos..end).ok_or("unexpected EOF reading u16")?;
    *pos = end;
    Ok(u16::from_be_bytes([s[0], s[1]]))
}

fn read_be_i32(buf: &[u8], pos: &mut usize) -> Result<i32, String> {
    let end = pos.checked_add(4).ok_or("i32 overflow")?;
    let s = buf.get(*pos..end).ok_or("unexpected EOF reading i32")?;
    *pos = end;
    Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn write_framed(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> io::Result<()> {
    let len = (payload.len() + 4) as u32;
    stream.write_all(&[tag])?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

fn write_simple_tag(stream: &mut TcpStream, tag: u8) -> io::Result<()> {
    write_framed(stream, tag, &[])
}

fn write_auth_ok(stream: &mut TcpStream) -> io::Result<()> {
    write_framed(stream, b'R', &0i32.to_be_bytes())
}

fn write_auth_cleartext_request(stream: &mut TcpStream) -> io::Result<()> {
    write_framed(stream, b'R', &3i32.to_be_bytes())
}

fn write_parameter_status(stream: &mut TcpStream, key: &str, val: &str) -> io::Result<()> {
    let mut buf = Vec::with_capacity(key.len() + val.len() + 2);
    buf.extend_from_slice(key.as_bytes());
    buf.push(0);
    buf.extend_from_slice(val.as_bytes());
    buf.push(0);
    write_framed(stream, b'S', &buf)
}

fn write_backend_key_data(stream: &mut TcpStream, pid: i32, secret: i32) -> io::Result<()> {
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&pid.to_be_bytes());
    buf[4..8].copy_from_slice(&secret.to_be_bytes());
    write_framed(stream, b'K', &buf)
}

fn write_ready_for_query(stream: &mut TcpStream, in_txn: bool) -> io::Result<()> {
    let status = if in_txn { b'T' } else { b'I' };
    write_framed(stream, b'Z', &[status])
}

fn write_empty_query_response(stream: &mut TcpStream) -> io::Result<()> {
    write_framed(stream, b'I', &[])
}

fn write_parameter_description(stream: &mut TcpStream, param_count: usize) -> io::Result<()> {
    let mut buf = Vec::with_capacity(2 + param_count * 4);
    buf.extend_from_slice(&(param_count as u16).to_be_bytes());
    for _ in 0..param_count {
        buf.extend_from_slice(&OID_TEXT.to_be_bytes());
    }
    write_framed(stream, b't', &buf)
}

fn write_row_description(
    stream: &mut TcpStream,
    columns: &[String],
    oids: &[i32],
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(2 + columns.len() * 24);
    buf.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for (i, col) in columns.iter().enumerate() {
        let oid = oids.get(i).copied().unwrap_or(OID_TEXT);
        buf.extend_from_slice(col.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&0i32.to_be_bytes()); // table OID
        buf.extend_from_slice(&0i16.to_be_bytes()); // attribute number
        buf.extend_from_slice(&oid.to_be_bytes()); // data type OID
        buf.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        buf.extend_from_slice(&0i16.to_be_bytes()); // format code (0 = text)
    }
    write_framed(stream, b'T', &buf)
}

fn write_data_row(stream: &mut TcpStream, row: &[table_codec::Value]) -> io::Result<()> {
    let mut buf = Vec::with_capacity(2 + row.len() * 12);
    buf.extend_from_slice(&(row.len() as u16).to_be_bytes());
    for val in row {
        if val.is_null() {
            buf.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            let text = format_value(val);
            let bytes = text.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    write_framed(stream, b'D', &buf)
}

fn write_command_complete(stream: &mut TcpStream, tag: &str) -> io::Result<()> {
    let mut buf = Vec::with_capacity(tag.len() + 1);
    buf.extend_from_slice(tag.as_bytes());
    buf.push(0);
    write_framed(stream, b'C', &buf)
}

fn write_error_response(stream: &mut TcpStream, sqlstate: &str, message: &str) -> io::Result<()> {
    let mut buf = Vec::with_capacity(32 + message.len());
    buf.push(b'S');
    buf.extend_from_slice(b"ERROR");
    buf.push(0);
    buf.push(b'C');
    buf.extend_from_slice(sqlstate.as_bytes());
    buf.push(0);
    buf.push(b'M');
    buf.extend_from_slice(message.as_bytes());
    buf.push(0);
    buf.push(0); // terminator
    write_framed(stream, b'E', &buf)
}
