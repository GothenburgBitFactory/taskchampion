use anyhow::{bail, Context, Result};
use serde::Deserialize;

// Note: These structs are marked `pub` so they can be used by other modules.
#[derive(Debug, Deserialize)]
pub struct VersionsResponse {
    pub versions: Vec<VersionInfo>,
}

#[derive(Debug, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub downloads: Downloads,
}

#[derive(Debug, Deserialize)]
pub struct Downloads {
    pub chrome: Option<Vec<DownloadPlatform>>,
    pub chromedriver: Option<Vec<DownloadPlatform>>,
}

#[derive(Debug, Deserialize)]
pub struct DownloadPlatform {
    pub platform: String,
    pub url: String,
}

impl VersionInfo {
    /// Helper to find a specific download URL for a platform.
    pub fn get_download_url(&self, asset_name: &str, platform: &str) -> Result<String> {
        let downloads_list = match asset_name {
            "chrome" => self.downloads.chrome.as_ref(),
            "chromedriver" => self.downloads.chromedriver.as_ref(),
            _ => bail!("Unknown asset type: {}", asset_name),
        }
        .with_context(|| {
            format!(
                "No downloads found for asset '{}' in version {}",
                asset_name, self.version
            )
        })?;

        let download = downloads_list
            .iter()
            .find(|d| d.platform == platform)
            .with_context(|| {
                format!(
                    "No download found for platform '{}' for asset '{}'",
                    platform, asset_name
                )
            })?;

        Ok(download.url.clone())
    }
}
