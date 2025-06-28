use std::{marker::PhantomData, path::Path};

use hook::{hook_kinds::HookKind, Hook, METADATA_EXTENSION};

pub mod hook;

type HooksResult<K> = Result<Result<Hook<K>, hook::read::Error>, load::Error<K>>;

/// Return all hooks found under `base` with kind `K`.
/// Will return [`None`], if the kind `K` does not exist under the base directory.
///
/// This function will construct the required path.
///
/// # Note
/// This does not check that `base` is only composed of two components.
///
/// As such, the following in valid:
/// ```no_run
/// tw_hooks::hooks_under("/".into())
/// ```
/// And will return all hooks found in the file-system.
pub fn hooks<K: HookKind>(
    base_directory: &Path,
) -> Result<Option<impl Iterator<Item = HooksResult<K>>>, load::Error<K>> {
    let hook_directory = base_directory.join(K::NAME_VERSION);

    if !hook_directory.exists() {
        return Ok(None);
    }

    Ok(Some(
        hook_directory
            .read_dir()
            .map_err(|err| load::Error::ReadHookDir {
                err,
                hook_directory: hook_directory.clone(),
                kind: PhantomData,
            })?
            .filter_map(move |maybe_entry| match maybe_entry {
                Ok(entry) => {
                    let ft = match entry.file_type() {
                        Ok(ok) => ok,
                        Err(err) => {
                            return Some(Err(load::Error::<K>::MissingFileType {
                                err,
                                path: entry.path(),
                                hook_directory: hook_directory.clone(),
                            }));
                        }
                    };

                    if !ft.is_file() {
                        return None;
                    }

                    {
                        let file_name = entry.file_name();

                        if file_name.to_string_lossy().ends_with(METADATA_EXTENSION) {
                            return None;
                        }
                    }

                    Some(Ok(Hook::<K>::from_path(&entry.path())))
                }
                Err(err) => Some(Err(load::Error::EntryRead {
                    err,
                    hook_directory: hook_directory.clone(),
                })),
            }),
    ))
}

#[allow(missing_docs)]
pub mod load {
    use std::{marker::PhantomData, path::PathBuf};

    use super::hook::hook_kinds::HookKind;

    /// The error returned by [`load_hooks`][`crate::load_hooks`].
    #[derive(Debug, thiserror::Error)]
    pub enum Error<K: HookKind> {
        #[error(
            "Failed to read the hook directory ('{hook_directory}') for hook kind '{kind}': {err}", kind = K::NAME_VERSION
        )]
        ReadHookDir {
            err: std::io::Error,
            hook_directory: PathBuf,
            kind: PhantomData<K>,
        },

        #[error(
            "Failed to read an entry's ('{path}) file type while listing directory ('{hook_directory}'): {err}"
        )]
        MissingFileType {
            err: std::io::Error,
            path: PathBuf,
            hook_directory: PathBuf,
        },

        #[error("Failed to read an entry while listing directory ('{hook_directory}'): {err}")]
        EntryRead {
            err: std::io::Error,
            hook_directory: PathBuf,
        },
    }
}
