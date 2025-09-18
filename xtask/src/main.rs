use anyhow::Result;
use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;

mod msrv;
mod test_wasm;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Update the Minimum Supported Rust Version (MSRV) across the repository.
    Msrv {
        /// The new MSRV to set, e.g., "1.75".
        version: String,
    },
    /// Run the Wasm browser tests in a hermetic environment.
    TestWasm,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let workspace_dir = manifest_dir.parent().unwrap();

    match &cli.command {
        Commands::Msrv { version } => {
            msrv::msrv(version.clone(), workspace_dir)?;
        }
        Commands::TestWasm => {
            test_wasm::test_wasm().await?;
        }
    }

    Ok(())
}
