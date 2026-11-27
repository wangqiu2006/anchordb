use anchordb::BuildProfile;

fn main() {
    let profile = BuildProfile::current().as_str();
    let version = env!("CARGO_PKG_VERSION");

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version" | "-V") => {
            println!("anchordb {version} ({profile})");
        }
        Some("shell" | "serve" | "doctor") => {
            eprintln!(
                "anchordb {version} ({profile}): command not implemented yet (Milestone 1 in progress)"
            );
            std::process::exit(2);
        }
        _ => {
            println!("AnchorDB {version} ({profile})");
            println!("Usage: anchordb <shell|serve|doctor> [options]");
        }
    }
}
