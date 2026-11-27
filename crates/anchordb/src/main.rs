use anchordb::{AnchorDb, BuildProfile};

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
        Some("shell" | "serve") => {
            eprintln!(
                "anchordb {version} ({profile}): SQL shell/server arrives in Milestone 3 (mode-embedded library & `doctor` are ready)"
            );
            std::process::exit(2);
        }
        _ => {
            println!("AnchorDB {version} ({profile})");
            println!("Usage: anchordb <doctor|shell|serve> [options]");
        }
    }
}
