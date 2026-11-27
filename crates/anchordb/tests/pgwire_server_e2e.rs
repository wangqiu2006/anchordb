#![cfg(feature = "mode-server")]

//! Real TCP end-to-end tests for AnchorDB's PostgreSQL Wire Protocol v3.0 server (`mode-server`).

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use anchordb::hal_vfs::fault_sim::FaultSimFs;
use anchordb::kv_core::LsmConfig;
use anchordb::{AnchorDb, QueryConfig, ServerConfig, start_pg_server};

type TextRows = Vec<Vec<Option<String>>>;

struct PgTestClient {
    stream: TcpStream,
}

impl PgTestClient {
    fn connect(addr: SocketAddr, user: &str, db: &str, password: Option<&str>) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;

        // 1. Send SSLRequest (len=8, code=80877103) and expect 'N'.
        let mut ssl_req = [0u8; 8];
        ssl_req[0..4].copy_from_slice(&8u32.to_be_bytes());
        ssl_req[4..8].copy_from_slice(&80_877_103u32.to_be_bytes());
        stream.write_all(&ssl_req)?;
        let mut ssl_resp = [0u8; 1];
        stream.read_exact(&mut ssl_resp)?;
        assert_eq!(ssl_resp[0], b'N');

        // 2. Send StartupMessage v3.0.
        let mut body = Vec::new();
        body.extend_from_slice(&196_608u32.to_be_bytes());
        for (k, v) in [("user", user), ("database", db)] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let len = (body.len() + 4) as u32;
        stream.write_all(&len.to_be_bytes())?;
        stream.write_all(&body)?;
        stream.flush()?;

        let mut client = Self { stream };
        loop {
            let (tag, payload) = client.read_msg()?;
            match tag {
                b'R' => {
                    let auth_code =
                        i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    if auth_code == 3 {
                        let pwd = password.unwrap_or("");
                        let mut pbuf = Vec::with_capacity(pwd.len() + 1);
                        pbuf.extend_from_slice(pwd.as_bytes());
                        pbuf.push(0);
                        client.write_msg(b'p', &pbuf)?;
                        client.stream.flush()?;
                    }
                }
                b'Z' => return Ok(client),
                b'E' => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        parse_error_payload(&payload),
                    ));
                }
                _ => {}
            }
        }
    }

    fn write_msg(&mut self, tag: u8, payload: &[u8]) -> io::Result<()> {
        let len = (payload.len() + 4) as u32;
        self.stream.write_all(&[tag])?;
        self.stream.write_all(&len.to_be_bytes())?;
        self.stream.write_all(payload)?;
        Ok(())
    }

    fn read_msg(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut hdr = [0u8; 5];
        self.stream.read_exact(&mut hdr)?;
        let tag = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut payload = vec![0u8; len - 4];
        self.stream.read_exact(&mut payload)?;
        Ok((tag, payload))
    }

    /// Executes a Simple Query and returns `(rows, ready_status, error_sqlstate_opt)`.
    fn simple_query(&mut self, sql: &str) -> io::Result<(TextRows, u8, Option<String>)> {
        let mut q = Vec::with_capacity(sql.len() + 1);
        q.extend_from_slice(sql.as_bytes());
        q.push(0);
        self.write_msg(b'Q', &q)?;
        self.stream.flush()?;

        let mut rows = Vec::new();
        let mut err_code = None;
        loop {
            let (tag, payload) = self.read_msg()?;
            match tag {
                b'D' => rows.push(parse_data_row(&payload)),
                b'E' => err_code = Some(extract_sqlstate(&payload)),
                b'Z' => return Ok((rows, payload[0], err_code)),
                _ => {}
            }
        }
    }

    /// Executes a parameterized Extended Query (`Parse` + `Bind` + `Describe` + `Execute` + `Sync`).
    fn extended_query(&mut self, sql: &str, params: &[Option<&str>]) -> io::Result<TextRows> {
        // Parse
        let mut p = Vec::new();
        p.push(0); // unnamed stmt
        p.extend_from_slice(sql.as_bytes());
        p.push(0);
        p.extend_from_slice(&0u16.to_be_bytes());
        self.write_msg(b'P', &p)?;

        // Bind
        let mut b = Vec::new();
        b.push(0); // unnamed portal
        b.push(0); // unnamed stmt
        b.extend_from_slice(&0u16.to_be_bytes()); // 0 format codes
        b.extend_from_slice(&(params.len() as u16).to_be_bytes());
        for param in params {
            match param {
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(s) => {
                    b.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    b.extend_from_slice(s.as_bytes());
                }
            }
        }
        b.extend_from_slice(&0u16.to_be_bytes()); // result formats
        self.write_msg(b'B', &b)?;

        // Describe statement
        self.write_msg(b'D', &[b'S', 0])?;

        // Execute portal
        let mut e = Vec::new();
        e.push(0);
        e.extend_from_slice(&0i32.to_be_bytes()); // unlimited rows
        self.write_msg(b'E', &e)?;

        // Sync
        self.write_msg(b'S', &[])?;
        self.stream.flush()?;

        let mut rows = Vec::new();
        loop {
            let (tag, payload) = self.read_msg()?;
            match tag {
                b'D' => rows.push(parse_data_row(&payload)),
                b'E' => {
                    return Err(io::Error::other(parse_error_payload(&payload)));
                }
                b'Z' => return Ok(rows),
                _ => {}
            }
        }
    }

    fn terminate(mut self) -> io::Result<()> {
        self.write_msg(b'X', &[])?;
        self.stream.flush()
    }
}

fn parse_data_row(payload: &[u8]) -> Vec<Option<String>> {
    let cols = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut pos = 2usize;
    let mut out = Vec::with_capacity(cols);
    for _ in 0..cols {
        let len = i32::from_be_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        pos += 4;
        if len < 0 {
            out.push(None);
        } else {
            let ulen = len as usize;
            let text = String::from_utf8_lossy(&payload[pos..pos + ulen]).into_owned();
            pos += ulen;
            out.push(Some(text));
        }
    }
    out
}

fn parse_error_payload(payload: &[u8]) -> String {
    String::from_utf8_lossy(payload).into_owned()
}

fn extract_sqlstate(payload: &[u8]) -> String {
    let mut pos = 0usize;
    while pos < payload.len() && payload[pos] != 0 {
        let field_type = payload[pos];
        pos += 1;
        let end = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .map_or(payload.len(), |i| pos + i);
        if field_type == b'C' {
            return String::from_utf8_lossy(&payload[pos..end]).into_owned();
        }
        pos = end + 1;
    }
    String::new()
}

#[test]
fn pgwire_simple_extended_pg_catalog_and_auth_e2e() {
    let fs = FaultSimFs::new(5432);
    let db = Arc::new(AnchorDb::open_fs(fs, LsmConfig::default(), QueryConfig::default()).unwrap());
    let cfg = ServerConfig {
        listen_addr: "127.0.0.1:0".into(),
        password: Some("secret123".into()),
        query: QueryConfig::default(),
    };
    let server = start_pg_server(db, cfg).unwrap();
    let addr = server.local_addr();

    // 1. Wrong password is rejected with SQLSTATE 28P01.
    let bad = PgTestClient::connect(addr, "postgres", "anchordb", Some("wrong"));
    assert!(bad.is_err());

    // 2. Correct password completes handshake.
    let mut client =
        PgTestClient::connect(addr, "postgres", "anchordb", Some("secret123")).unwrap();

    // Client startup probes used by psql / DBeaver / Navicat.
    let (_rows, ready, err) = client
        .simple_query("SET client_encoding = 'UTF8';")
        .unwrap();
    assert_eq!(ready, b'I');
    assert!(err.is_none());

    let (probe_rows, _, _) = client
        .simple_query("SELECT current_schema(), current_database(), version();")
        .unwrap();
    assert_eq!(probe_rows[0][0].as_deref(), Some("public"));
    assert_eq!(probe_rows[0][1].as_deref(), Some("anchordb"));
    assert!(
        probe_rows[0][2]
            .as_deref()
            .is_some_and(|v| v.contains("AnchorDB"))
    );

    // Create table + insert via Simple Query.
    let (_, _, err) = client
        .simple_query(
            "CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL, price BIGINT NOT NULL);
             INSERT INTO products VALUES (1, 'Keyboard', 399), (2, 'Mouse', 199);",
        )
        .unwrap();
    assert!(err.is_none());

    // Extended Query with $1/$2 parameters.
    let ext_rows = client
        .extended_query(
            "SELECT name, price FROM products WHERE price >= $1 AND name != $2 ORDER BY id ASC;",
            &[Some("150"), Some("Mouse")],
        )
        .unwrap();
    assert_eq!(
        ext_rows,
        vec![vec![Some("Keyboard".into()), Some("399".into())]]
    );

    // Introspect via `pg_catalog.pg_class` and `information_schema.columns` with exact column projection.
    let (pg_class_rows, _, _) = client
        .simple_query("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r';")
        .unwrap();
    assert_eq!(pg_class_rows, vec![vec![Some("products".into())]]);

    let (info_cols, _, _) = client
        .simple_query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'products' ORDER BY ordinal_position ASC;",
        )
        .unwrap();
    assert_eq!(
        info_cols,
        vec![
            vec![Some("id".into())],
            vec![Some("name".into())],
            vec![Some("price".into())],
        ]
    );

    client.terminate().unwrap();
    server.stop();
}

#[test]
fn pgwire_concurrent_clients_enforce_si_write_conflict_40001() {
    let fs = FaultSimFs::new(5433);
    let db = Arc::new(AnchorDb::open_fs(fs, LsmConfig::default(), QueryConfig::default()).unwrap());
    let cfg = ServerConfig {
        listen_addr: "127.0.0.1:0".into(),
        password: None,
        query: QueryConfig::default(),
    };
    let server = start_pg_server(db, cfg).unwrap();
    let addr = server.local_addr();

    let mut c1 = PgTestClient::connect(addr, "postgres", "anchordb", None).unwrap();
    let mut c2 = PgTestClient::connect(addr, "postgres", "anchordb", None).unwrap();

    c1.simple_query(
        "CREATE TABLE wallets (id BIGINT PRIMARY KEY, bal BIGINT NOT NULL);
         INSERT INTO wallets VALUES (1, 100);",
    )
    .unwrap();

    // Both clients open concurrent transactions (ReadyForQuery reports 'T').
    let (_, r1, _) = c1.simple_query("BEGIN;").unwrap();
    let (_, r2, _) = c2.simple_query("BEGIN;").unwrap();
    assert_eq!(r1, b'T');
    assert_eq!(r2, b'T');

    c1.simple_query("UPDATE wallets SET bal = 120 WHERE id = 1;")
        .unwrap();
    c2.simple_query("UPDATE wallets SET bal = 150 WHERE id = 1;")
        .unwrap();

    // Client 1 commits first; Client 2 receives PostgreSQL SQLSTATE 40001 (serialization_failure).
    let (_, r1_after, e1) = c1.simple_query("COMMIT;").unwrap();
    assert_eq!(r1_after, b'I');
    assert!(e1.is_none());

    let (_, r2_after, e2) = c2.simple_query("COMMIT;").unwrap();
    assert_eq!(r2_after, b'I');
    assert_eq!(e2.as_deref(), Some("40001"));

    let (rows, _, _) = c1
        .simple_query("SELECT bal FROM wallets WHERE id = 1;")
        .unwrap();
    assert_eq!(rows, vec![vec![Some("120".into())]]);

    c1.terminate().unwrap();
    c2.terminate().unwrap();
    server.stop();
}
