use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // Initialize SQLite database
    let db_path = Path::new("soil_moisture.db");
    if !db_path.exists() {
        fs::File::create(db_path).unwrap();
    }

    // Initialize TUF repository
    let tuf_repo_path = Path::new("tuf_repo");
    if !tuf_repo_path.exists() {
        fs::create_dir_all(tuf_repo_path).unwrap();
    }

    // Set environment variables
    env::set_var("RUST_LOG", "info");
    env::set_var("RUST_BACKTRACE", "1");

    // Print build information
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:warning=Building soil_moisture_api");
}