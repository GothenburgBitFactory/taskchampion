use std::{
    io::Write,
    process::{Command, ExitStatus, Stdio},
};

use log::debug;
use taskchampion::Task;

use super::{Hook, hook_kinds::HookKind};

mod imply;

mod error;
pub use error::Error;

struct HookOutput {
    tasks: Vec<Task>,
    feedback: Vec<String>,
}

/// Call a hook and return it's status and stdout output.
///
/// The stdout output is already separated into valid Task lines and non-valid Task lines (so
/// called feedback.)
fn call_hook<K: HookKind>(
    hook: &Hook<K>,
    input: impl Iterator<Item = String>,
) -> Result<(ExitStatus, HookOutput), Error> {
    fn process_output(stdout: String) -> HookOutput {
        let (output_json, output_feedback): (Vec<Option<Task>>, Vec<Option<String>>) = stdout
            .lines()
            .map(|line| match serde_json::from_str::<_>(line) {
                Ok(v) => match Task::from_composed_json(v) {
                    Ok(task) => (Some(task), None),
                    Err(err) => {
                        debug!("Failed to decompose valid json value as task: {err}");
                        (None, Some(line.to_owned()))
                    }
                },
                Err(_) => (None, Some(line.to_owned())),
            })
            .unzip();

        HookOutput {
            tasks: output_json.into_iter().flatten().collect(),
            feedback: output_feedback.into_iter().flatten().collect(),
        }
    }

    debug!("Calling hook: {}", hook.path.display());

    let mut child = Command::new(&hook.path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| Error::HookSpawn {
            err,
            script: hook.path.clone(),
        })?;

    let mut stdin = child.stdin.take().expect("Available, as we piped it");

    for line in input {
        debug!("Writing line of input: '{}'", line);
        stdin
            .write_all(line.as_bytes())
            .map_err(|err| Error::HookWrite {
                err,
                script: hook.path.clone(),
            })?;
        stdin.write_all(b"\n").map_err(|err| Error::HookWrite {
            err,
            script: hook.path.clone(),
        })?;
    }
    drop(stdin);

    debug!("Waiting for hook to exit");
    let output = child.wait_with_output().map_err(|err| Error::HookRead {
        err,
        script: hook.path.clone(),
    })?;

    let output_str =
        String::from_utf8(output.stdout.clone()).map_err(|err| Error::OutputToString {
            err,
            output: output.stdout,
            script: hook.path.clone(),
        })?;

    Ok((output.status, process_output(output_str)))
}
