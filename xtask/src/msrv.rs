use anyhow::{self, bail};
use regex::Regex;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::io::Write;
use std::path::Path;

/// Tuples of the form (PATH, REGEX) where PATH and REGEX are literals where PATH is a file that
/// conains the Minimum Supported Rust Version and REGEX is the pattern to find the appropriate
/// line in the file. PATH is relative to the root directory in the repo.
const MSRV_PATH_REGEX: &[(&str, &str)] = &[
    (
        ".github/workflows/checks.yml",
        r#"toolchain: "[0-9.]+*" # MSRV"#,
    ),
    (".github/workflows/rust-tests.yml", r#""[0-9.]+" # MSRV"#),
    ("src/crate-doc.md", r#"Rust version [0-9.]+ and higher"#),
    ("Cargo.toml", r#"^rust-version = "[0-9.]+""#),
];

/// `cargo xtask msrv (X.Y)`
///
/// This checks and updates the Minimum Supported Rust Version for all files specified in MSRV_PATH_REGEX`.
/// Each line where the regex matches will have all values of the form `#.##` replaced with the given MSRV.
pub fn msrv(version: String, workspace_dir: &Path) -> anyhow::Result<()> {
    if !version.chars().all(|c| c.is_numeric() || c == '.') {
        bail!("xtask: Invalid argument format. Version must be in the form \"X.Y\", e.g., `1.68`");
    }
    let version_replacement_string = &version;

    // set regex for replacing version number only within the pattern found within a line
    let re_msrv_version = Regex::new(r"([0-9]+(\.|[0-9]+|))+")?;

    // for each file in const paths tuple
    for msrv_file in MSRV_PATH_REGEX {
        let mut is_pattern_in_file = false;
        let mut updated_file = false;

        let path = workspace_dir.join(msrv_file.0);
        let path = Path::new(&path);
        if !path.exists() {
            anyhow::bail!("xtask: path does not exist {}", &path.display());
        };

        let mut file = File::options().read(true).write(true).open(path)?;
        let reader = BufReader::new(&file);

        // set search string and the replacement string for version number content
        let re_msrv_pattern = Regex::new(msrv_file.1)?;

        // for each line in file
        let mut file_string = String::new();
        for line in reader.lines() {
            let line = &line?;

            // if rust version pattern is found and is different, update it
            if let Some(pattern_offset) = re_msrv_pattern.find(line) {
                is_pattern_in_file = true;
                if !pattern_offset.as_str().contains(version_replacement_string) {
                    file_string
                        .push_str(&re_msrv_version.replace(line, version_replacement_string));
                    file_string.push_str("\n");
                    updated_file = true;
                    continue;
                }
            }

            file_string.push_str(line);
            file_string.push_str("\n");
        }

        // if pattern was found and updated, write to disk
        if updated_file {
            //  Set the file length to the file_string length
            file.set_len(file_string.len() as u64)?;

            //  set the cursor to the beginning of the file and write
            file.seek(std::io::SeekFrom::Start(0))?;
            file.write_all(file_string.as_bytes())?;

            // notify user this file was updated
            println!("xtask: Updated MSRV in {}", msrv_file.0);
        } else if !is_pattern_in_file {
            println!(
                "xtask: Pattern {:?} not found in {}",
                msrv_file.1, msrv_file.0
            );
        }
    }

    Ok(())
}
