use std::io::{self, BufRead, IsTerminal, Write};

use anchordb::{AnchorDb, BuildProfile, format_sql_result};

fn main() {
    let profile = BuildProfile::current().as_str();
    let version = env!("CARGO_PKG_VERSION");

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version" | "-V") => {
            println!("anchordb {version} ({profile})");
        }
        Some("doctor") => {
            let Some(db_path) = args.next() else {
                eprintln!("Usage: anchordb doctor <db_path>");
                std::process::exit(2);
            };
            match AnchorDb::doctor(&db_path) {
                Ok(report) => {
                    println!("AnchorDB Doctor OK ({db_path})");
                    println!("  format_version    : {}", report.format_version);
                    println!("  manifest_file     : {}", report.manifest_file);
                    println!("  checkpoint_lsn    : {}", report.checkpoint_lsn);
                    println!("  active_wal_file   : {}", report.active_wal_file);
                    println!("  wal_records       : {}", report.wal_records);
                    println!("  wal_had_torn_tail : {}", report.wal_had_torn_tail);
                    println!("  sst_count         : {}", report.sst_count);
                    println!("  total_sst_entries : {}", report.total_sst_entries);
                }
                Err(err) => {
                    eprintln!("AnchorDB Doctor FAILED ({db_path}): {err}");
                    std::process::exit(1);
                }
            }
        }
        Some("shell") => {
            let Some(db_path) = args.next() else {
                eprintln!("Usage: anchordb shell <db_path>");
                std::process::exit(2);
            };
            run_shell(&db_path, version, profile);
        }
        Some("serve") => {
            #[cfg(feature = "mode-server")]
            {
                run_serve(args, version, profile);
            }
            #[cfg(not(feature = "mode-server"))]
            {
                eprintln!(
                    "anchordb {version} ({profile}): rebuild with `--features mode-server` to enable `serve`"
                );
                std::process::exit(2);
            }
        }
        _ => {
            println!("AnchorDB {version} ({profile})");
            println!("Usage: anchordb <shell|doctor|serve> [options]");
        }
    }
}

#[cfg(feature = "mode-server")]
fn run_serve(mut args: impl Iterator<Item = String>, version: &str, profile: &str) {
    use std::sync::Arc;

    let Some(db_path) = args.next() else {
        eprintln!("Usage: anchordb serve <db_path> [--listen 127.0.0.1:5432] [--password <pwd>]");
        std::process::exit(2);
    };

    let mut cfg = anchordb::ServerConfig::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--listen" => {
                if let Some(addr) = args.next() {
                    cfg.listen_addr = addr;
                } else {
                    eprintln!("--listen requires an address (e.g. 127.0.0.1:5432)");
                    std::process::exit(2);
                }
            }
            "--password" => {
                if let Some(pwd) = args.next() {
                    cfg.password = Some(pwd);
                } else {
                    eprintln!("--password requires a value");
                    std::process::exit(2);
                }
            }
            other => {
                eprintln!("Unknown option for `serve`: {other}");
                std::process::exit(2);
            }
        }
    }

    let db = match AnchorDb::open(&db_path) {
        Ok(db) => Arc::new(db),
        Err(err) => {
            eprintln!("Failed to open database '{db_path}': {err}");
            std::process::exit(1);
        }
    };

    let server = match anchordb::start_pg_server(db, cfg) {
        Ok(srv) => srv,
        Err(err) => {
            eprintln!("Failed to start PostgreSQL wire server: {err}");
            std::process::exit(1);
        }
    };

    let addr = server.local_addr();
    println!("AnchorDB {version} ({profile}) listening on {addr}");
    println!("Database: {db_path}");
    println!(
        "Connect with: psql -h {} -p {} -U postgres",
        addr.ip(),
        addr.port()
    );
    println!("Press Ctrl+C to stop.");

    // Block main thread cleanly until terminated by signal.
    loop {
        std::thread::park();
    }
}

fn run_shell(db_path: &str, version: &str, profile: &str) {
    let db = match AnchorDb::open(db_path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("Failed to open database '{db_path}': {err}");
            std::process::exit(1);
        }
    };

    let interactive = io::stdin().is_terminal();
    if interactive {
        println!("AnchorDB {version} ({profile}) interactive SQL shell");
        println!("Opened: {db_path}");
        println!(
            "Type SQL statements ending with ';', or meta-commands: .tables, .checkpoint, .help, .quit"
        );
    }

    let mut session = db.sql_session();
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut buffer = String::new();

    for line_res in stdin.lock().lines() {
        if interactive && buffer.is_empty() {
            let prompt = if session.in_transaction() {
                "anchordb* > "
            } else {
                "anchordb> "
            };
            let _ = write!(stdout, "{prompt}");
            let _ = stdout.flush();
        } else if interactive {
            let _ = write!(stdout, "      ...> ");
            let _ = stdout.flush();
        }

        let Ok(line) = line_res else {
            break;
        };
        let trimmed = line.trim();
        if buffer.is_empty() {
            if trimmed.is_empty() {
                continue;
            }
            match trimmed {
                ".quit" | ".exit" | "\\q" => break,
                ".help" => {
                    println!("Meta-commands:");
                    println!("  .tables      List all tables");
                    println!("  .checkpoint  Flush MemTable to SSTable and rotate WAL");
                    println!("  .quit        Exit shell");
                    continue;
                }
                ".tables" => {
                    execute_and_print(&mut session, "SHOW TABLES;");
                    continue;
                }
                ".checkpoint" => {
                    match db.checkpoint() {
                        Ok(()) => println!("OK (checkpoint complete)"),
                        Err(err) => eprintln!("ERROR: {err}"),
                    }
                    continue;
                }
                _ => {}
            }
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);

        if trimmed.ends_with(';') {
            execute_and_print(&mut session, &buffer);
            buffer.clear();
        }
    }

    // Execute any trailing non-empty statement piped without a final semicolon.
    if !buffer.trim().is_empty() {
        execute_and_print(&mut session, &buffer);
    }
}

fn execute_and_print(
    session: &mut anchordb::SqlSession<'_, hal_vfs::std_fs::StdFileSystem>,
    sql: &str,
) {
    match session.execute(sql) {
        Ok(results) => {
            for res in results {
                println!("{}", format_sql_result(&res));
            }
        }
        Err(err) => {
            eprintln!("ERROR: {err}");
        }
    }
}
