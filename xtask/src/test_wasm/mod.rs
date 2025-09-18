use anyhow::{bail, Context, Result};
use setup::setup_environment;
use std::process::Command;

mod models;
mod setup;

pub async fn test_wasm() -> Result<()> {
    let env = setup_environment().await?;
    println!("\n--- Running Wasm Tests ---");
    let mut cmd = Command::new("cargo");
    cmd.args([
        "test",
        "--release",
        "--target",
        "wasm32-unknown-unknown",
        "--no-default-features",
        "--",
        "--nocapture",
    ]);
    cmd.env(
        "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER",
        "wasm-bindgen-test-runner",
    );
    cmd.env("WASM_BINDGEN_TEST_ONLY_WEB", "1");
    cmd.env("CHROMEDRIVER", &env.driver_exe);
    if let Some(browser_path) = env.browser_exe {
        println!(
            "✅ Using hermetic Chrome binary: {}",
            browser_path.display()
        );
        cmd.env("CHROME_HEADLESS", browser_path);
    } else {
        println!("Using local Chrome installation.");
    }
    let status = cmd.status().context("Failed to execute cargo test")?;
    if !status.success() {
        bail!("Wasm tests failed with status: {}", status);
    }
    println!("\nWasm tests passed!");
    Ok(())
}
