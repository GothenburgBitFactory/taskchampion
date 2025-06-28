use std::{
    fs::File,
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crate::hooks::hook::HookMetadata;

use super::{hook_kinds::HookKind, Hook, METADATA_EXTENSION};

#[derive(Debug, thiserror::Error)]
/// The Error returned by [`Hook::from_path`].
pub enum Error {
    #[error("The supplied path ('{0}') does not point to a file.")]
    PathNotToFile(PathBuf),

    #[error("The supplied path ('{0}') has less than the required two components: {1}.")]
    NotEnoughComponents(PathBuf, usize),

    #[error("Failed to check for the existance of the metadata file at '{path}': {err}")]
    MetadataCheck { err: std::io::Error, path: PathBuf },

    #[error("Failed to open the metadata file at '{path}': {err}")]
    MetadataOpen { err: std::io::Error, path: PathBuf },

    #[error("Failed to parse the metadata file at '{path}': {err}")]
    MetadataParse {
        err: serde_json::Error,
        path: PathBuf,
    },

    #[error("The hook path component should be utf8 encoded, but was not: {0}")]
    NonUtf8Component(String),

    #[error("The `<hook_kind>-<version>` part was missing it's `-`: {0}")]
    MalformedHookKind(String),

    #[error("The version '{0}' was unknown.")]
    UnknownVersion(String),

    #[error("The hook kind ('{hook_kind}') was unkonwn: {err}")]
    UnknownHookKind {
        err: super::hook_kinds::decode::Error,
        hook_kind: String,
    },
}

impl<K: HookKind> Hook<K> {
    /// Read a Hook from it's path.
    /// This path should contain at least two components, of which the last two mean:
    /// `./<type>_<version>/<hook_name>`.
    ///
    /// The `<hook_name>` must be a file.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - `path` does not point to a file
    /// - `path` does not contain at least three last components.
    pub fn from_path(path: &Path) -> Result<Self, Error> {
        if !path.is_file() {
            return Err(Error::PathNotToFile(path.to_owned()));
        }

        let components = path.components().rev().take(2).collect::<Vec<_>>();

        if components.len() < 2 {
            return Err(Error::NotEnoughComponents(
                path.to_owned(),
                components.len(),
            ));
        }

        // `./<type>_<version>/<hook_name>`
        //          ^                ^
        //  <-   components[1]    components[0]
        let _: K = expect_hook_kind(components[1])?;

        let metadata_path = path.with_extension(METADATA_EXTENSION);
        let metadata = if metadata_path
            .try_exists()
            .map_err(|err| Error::MetadataCheck {
                err,
                path: metadata_path.clone(),
            })? {
            serde_json::from_reader(File::open(&metadata_path).map_err(|err| {
                Error::MetadataOpen {
                    err,
                    path: metadata_path.clone(),
                }
            })?)
            .map_err(|err| Error::MetadataParse {
                err,
                path: metadata_path,
            })?
        } else {
            HookMetadata::default()
        };

        Ok(Self {
            kind: PhantomData,
            metadata,
            path: path.to_owned(),
        })
    }
}

fn expect_hook_kind<K: HookKind>(component: std::path::Component<'_>) -> Result<K, Error> {
    let comp = component
        .as_os_str()
        .to_str()
        .ok_or(Error::NonUtf8Component(
            component.as_os_str().to_string_lossy().to_string(),
        ))?;

    let (hook_kind, version) = comp
        .split_once('_')
        .ok_or(Error::MalformedHookKind(comp.to_owned()))?;

    match version {
        "v3" => hook_kind.parse().map_err(|err| Error::UnknownHookKind {
            err,
            hook_kind: hook_kind.to_owned(),
        }),
        other => Err(Error::UnknownVersion(other.to_owned())),
    }
}
