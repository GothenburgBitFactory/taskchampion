use std::process::ExitStatus;

use serde_json::{Map, Value};

use crate::composing_json;

/// The Error returned from [`Hook::execute`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to spawn the hook ('{script}'): {err}")]
    HookSpawn {
        script: std::path::PathBuf,
        err: std::io::Error,
    },

    #[error("Failed to write input into the hook's ('{script}') stdin: {err}")]
    HookWrite {
        err: std::io::Error,
        script: std::path::PathBuf,
    },
    #[error("Failed to read output into the hook's ('{script}') stdout: {err}")]
    HookRead {
        err: std::io::Error,
        script: std::path::PathBuf,
    },

    #[error("Failed to decode hook ('{script}') stdout ('{output:?}') as utf8 string: {err}")]
    OutputToString {
        err: std::string::FromUtf8Error,
        output: Vec<u8>,
        script: std::path::PathBuf,
    },

    #[error(
        "Returned json ('{line:?}') of hook ('{script}') can not be interpreted as task: {err}"
    )]
    InvalidTaskJson {
        err: composing_json::parse::Error,
        line: Map<String, Value>,
        script: std::path::PathBuf,
    },

    #[error(
        "Hook ('{script}') returned a wrong number of tasks: found {tasks}, but expected {expected}"
    )]
    WrongTaskNum {
        tasks: usize,
        expected: usize,
        script: std::path::PathBuf,
    },

    #[error("Hook ('{script}') failed with status: {status}")]
    HookExecFailed {
        status: ExitStatus,
        script: std::path::PathBuf,
    },
}
