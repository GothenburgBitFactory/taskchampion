use super::models::VersionsResponse;
use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct TestEnvironment {
    pub browser_exe: Option<PathBuf>,
    pub driver_exe: PathBuf,
}

pub async fn setup_environment() -> Result<TestEnvironment> {
    let setup = TestSetup::new().await?;
    if let Ok((_path, version)) = get_local_chrome_version() {
        setup.setup_with_local_chrome(&version).await
    } else {
        setup.setup_with_downloaded_chrome().await
    }
}

struct PlatformDetails {
    platform_string: &'static str,
    browser_exe_subpath: PathBuf,
    driver_exe_subpath: PathBuf,
}

impl PlatformDetails {
    fn new() -> Result<Self> {
        if cfg!(target_os = "macos") {
            let platform = if cfg!(target_arch = "aarch64") {
                "mac-arm64"
            } else {
                "mac-x64"
            };
            Ok(Self {
                platform_string: platform,
                browser_exe_subpath: format!("chrome-{}/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing", platform).into(),
                driver_exe_subpath: format!("chromedriver-{}/chromedriver", platform).into(),
            })
        } else if cfg!(target_os = "linux") {
            Ok(Self {
                platform_string: "linux64",
                browser_exe_subpath: "chrome-linux64/chrome".into(),
                driver_exe_subpath: "chromedriver-linux64/chromedriver".into(),
            })
        } else if cfg!(target_os = "windows") {
            Ok(Self {
                platform_string: "win64",
                browser_exe_subpath: "chrome-win64/chrome.exe".into(),
                driver_exe_subpath: "chromedriver-win64/chromedriver.exe".into(),
            })
        } else {
            bail!("Unsupported OS")
        }
    }
}

/// A context for the setup process.
struct TestSetup {
    platform: PlatformDetails,
    client: reqwest::Client,
    base_cache_dir: PathBuf,
}

impl TestSetup {
    async fn new() -> Result<Self> {
        Ok(Self {
            platform: PlatformDetails::new()?,
            client: reqwest::Client::new(),
            base_cache_dir: dirs::cache_dir()
                .context("Could not get cache dir")?
                .join("taskchampion-xtask"),
        })
    }

    /// Centralized data fetching.
    async fn fetch_versions_data(&self) -> Result<VersionsResponse> {
        let url = "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json";
        self.client
            .get(url)
            .send()
            .await?
            .json()
            .await
            .context("Failed to fetch or parse versions data")
    }

    /// Centralized asset download and preparation logic.
    async fn prepare_executable(
        &self,
        asset_name: &str,
        url: &str,
        version: &str,
        exe_subpath: &Path,
    ) -> Result<PathBuf> {
        let cache_dir = self
            .base_cache_dir
            .join(format!("{}-{}", asset_name, version));
        if !cache_dir.exists() {
            println!("Downloading {} v{} from {}", asset_name, version, url);
            fs::create_dir_all(&cache_dir)?;
            let response = self.client.get(url).send().await?.bytes().await?;
            let cursor = std::io::Cursor::new(response);
            zip::ZipArchive::new(cursor)?.extract(&cache_dir)?;
        } else {
            println!("Using cached {} at {}", asset_name, cache_dir.display());
        }

        let exe_path = cache_dir.join(exe_subpath);
        if !exe_path.exists() {
            bail!("Executable not found at {}", exe_path.display());
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&exe_path, fs::Permissions::from_mode(0o755))?;
        }

        Ok(exe_path)
    }

    /// Set up using a locally installed Chrome browser.
    async fn setup_with_local_chrome(&self, version: &str) -> Result<TestEnvironment> {
        println!("Found local Chrome version: {}", version);
        let major_version = version.split('.').next().unwrap_or("");
        let versions_data = self.fetch_versions_data().await?;
        let version_info = versions_data
            .versions
            .iter()
            .rev()
            .find(|v| v.version.starts_with(major_version))
            .with_context(|| format!("Could not find info for Chrome v{}", major_version))?;

        let driver_url =
            version_info.get_download_url("chromedriver", self.platform.platform_string)?;

        let driver_exe = self
            .prepare_executable(
                "chromedriver",
                &driver_url,
                &version_info.version,
                &self.platform.driver_exe_subpath,
            )
            .await?;

        Ok(TestEnvironment {
            browser_exe: None,
            driver_exe,
        })
    }

    /// Set up by downloading a hermetic Chrome browser and matching driver.
    async fn setup_with_downloaded_chrome(&self) -> Result<TestEnvironment> {
        println!("Local Chrome not found. Falling back to browser download.");
        let versions_data = self.fetch_versions_data().await?;
        let latest_stable = versions_data
            .versions
            .last()
            .context("Could not find latest stable version")?;

        let chrome_url = latest_stable.get_download_url("chrome", self.platform.platform_string)?;
        let driver_url =
            latest_stable.get_download_url("chromedriver", self.platform.platform_string)?;

        let browser_exe = self
            .prepare_executable(
                "chrome",
                &chrome_url,
                &latest_stable.version,
                &self.platform.browser_exe_subpath,
            )
            .await?;

        let driver_exe = self
            .prepare_executable(
                "chromedriver",
                &driver_url,
                &latest_stable.version,
                &self.platform.driver_exe_subpath,
            )
            .await?;

        Ok(TestEnvironment {
            browser_exe: Some(browser_exe),
            driver_exe,
        })
    }
}

/// Tries to find the path and version of a locally installed Google Chrome.
fn get_local_chrome_version() -> Result<(PathBuf, String)> {
    let (path, command) = if cfg!(target_os = "macos") {
        let path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
        (path, Command::new(path).arg("--version").output())
    } else {
        bail!("Unsupported OS for local Chrome detection")
    };
    let output = command.context(format!(
        "Failed to get Chrome version. Is Chrome at {}?",
        path
    ))?;
    let version_str = String::from_utf8(output.stdout)?;
    let version = version_str.split_whitespace().last().unwrap_or("").trim();
    if version.is_empty() {
        bail!("Could not parse Chrome version: {}", version_str);
    }
    Ok((path.into(), version.to_string()))
}
