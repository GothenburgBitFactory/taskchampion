use std::{fmt::Display, marker::PhantomData, path::PathBuf};

use hook_kinds::HookKind;
use serde::{Deserialize, Serialize};

pub mod execute;
pub mod hook_kinds;
pub mod read;

pub const METADATA_EXTENSION: &str = "metadata.json";

#[derive(Debug)]
pub struct Hook<K: HookKind> {
    metadata: HookMetadata,
    path: PathBuf,
    kind: PhantomData<K>,
}

impl<K: HookKind> Display for Hook<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.path.to_string_lossy())
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub struct HookMetadata {
    #[serde(default)]
    sync_compatible: bool,
}

impl Display for HookMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            serde_json::to_string(&self)
                .expect("Will always work")
                .as_str(),
        )
    }
}
