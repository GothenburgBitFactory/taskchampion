# Release process

1. Run `git pull upstream main`
1. Run `cargo test`
1. Run `cargo clean && cargo clippy`
1. Remove the `-pre` from `version` in `taskchampion/Cargo.toml`.
1. Run `cargo semver-checks` (https://crates.io/crates/cargo-semver-checks)
1. Run `mdbook test docs`
1. Run `cargo build --release -p taskchampion`
1. Commit the changes (Cargo.lock will change too) with comment `vX.Y.Z`.
1. Run `git tag vX.Y.Z`
1. Bump the patch version in `taskchampion/Cargo.toml` and add the `-pre` suffix. This allows `cargo-semver-checks` to check for changes not accounted for in the version delta.
1. Commit that change with comment "Bump to -pre version".
1. Run `git push upstream`
1. Run `git push --tags upstream`
1. Run `(cd taskchampion; cargo publish)`
1. Navigate to the tag in the GitHub releases UI and create a release with general comments about the changes in the release
