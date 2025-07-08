use hook::{
    hook_kinds::{OnAdd, OnExit, OnLaunch, OnModify},
    Hook,
};
use log::{error, info};
use serde_json::Value;

use crate::{
    hooks::hook::{self, hook_kinds::OnCommit},
    Operations, Task,
};

use super::{call_hook, Error};

impl Hook<OnCommit> {
    /// Execute a on-commit hook.
    ///
    /// # Hook Input
    /// - a line of JSON for every operation, scheduled to be committed.
    ///
    /// # Hook Output
    /// - emitted JSON is parsed as operations, if the exit code is zero,
    ///   otherwise ignored.
    /// - all emitted non-JSON lines are considered feedback or error messages
    ///   depending on the status code.
    ///
    /// # Errors
    ///
    /// This function will return an error if
    /// - The hook execution failed.
    pub fn execute(&self, operations: Operations) -> Result<Operations, Error> {
        let (status, output) = call_hook(
            self,
            operations
                .iter()
                .map(|op| serde_json::to_string(op).expect("This should be serializable")),
            |map| serde_json::from_value(Value::Object(map)),
        )?;

        if status.success() {
            for line in output.feedback {
                info!("hook feedback: {line}")
            }

            Ok(output.json)
        } else {
            for line in output.feedback {
                error!("hook feedback: {line}")
            }
            Err(Error::HookExecFailed {
                status,
                script: self.path.clone(),
            })
        }
    }
}

impl Hook<OnAdd> {
    /// Execute a on-add hook.
    ///
    /// # Hook Input
    /// - a line of JSON for the added task.
    ///
    /// # Hook Output
    /// - emitted JSON for the input task is added, if the exit code is zero,
    ///   otherwise ignored.
    /// - all emitted non-JSON lines are considered feedback or error messages
    ///   depending on the status code.
    ///
    /// # Errors
    ///
    /// This function will return an error if
    /// - The hook execution failed.
    pub fn execute(&self, added: Task) -> Result<Task, Error> {
        let (uuid, composed) = added.compose_json();
        let (status, mut output) = call_hook(self, [composed.to_string()].into_iter(), |v| {
            Task::from_composed_json(v, uuid)
        })?;

        if status.success() {
            if output.json.len() != 1 {
                return Err(Error::WrongTaskNum {
                    expected: 1,
                    tasks: output.json.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                info!("hook feedback: {line}")
            }

            Ok(output.json.remove(0))
        } else {
            for line in output.feedback {
                error!("hook feedback: {line}")
            }
            Err(Error::HookExecFailed {
                status,
                script: self.path.clone(),
            })
        }
    }
}

impl Hook<OnModify> {
    /// Execute a on-modify hook.
    ///
    /// # Hook Input
    /// - line of JSON for the original task
    /// - line of JSON for the modified task, the diff being the modification
    ///
    /// # Hook Output
    /// - emitted JSON for the input task is saved, if the exit code is zero,
    ///   otherwise ignored.
    /// - all emitted non-JSON lines are considered feedback or error messages
    ///   depending on the status code.
    ///
    /// # Errors
    ///
    /// This function will return an error if
    /// - The hook execution failed.
    pub fn execute(&self, original: Task, modified: Task) -> Result<Task, Error> {
        let (original_uuid, original_composed) = original.compose_json();
        let (modified_uuid, modified_composed) = modified.compose_json();

        assert_eq!(original_uuid, modified_uuid);

        let (status, mut output) = call_hook(
            self,
            [original_composed.to_string(), modified_composed.to_string()].into_iter(),
            |v| Task::from_composed_json(v, modified_uuid),
        )?;

        if status.success() {
            if output.json.len() != 1 {
                return Err(Error::WrongTaskNum {
                    expected: 1,
                    tasks: output.json.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                info!("hook feedback: {line}")
            }

            Ok(output.json.remove(0))
        } else {
            for line in output.feedback {
                error!("hook feedback: {line}")
            }
            Err(Error::HookExecFailed {
                status,
                script: self.path.clone(),
            })
        }
    }
}

impl Hook<OnExit> {
    /// Execute a on-exit hook.
    ///
    /// # Hook Input
    /// - read-only line of JSON for each task added/modified
    ///
    /// # Hook Output
    /// - all emitted JSON is ignored
    /// - all emitted non-JSON lines are considered feedback or error messages
    ///   depending on the status code.
    ///
    /// # Errors
    ///
    /// This function will return an error if
    /// - The hook execution failed.
    pub fn execute(&self, changed_tasks: Vec<Task>) -> Result<(), Error> {
        let (status, output) = call_hook(
            self,
            changed_tasks
                .iter()
                .map(Task::compose_json)
                .map(|(_, value)| value.to_string()),
            |_| Ok::<_, Error>(()),
        )?;

        if status.success() {
            if !output.json.is_empty() {
                return Err(Error::WrongTaskNum {
                    expected: 0,
                    tasks: output.json.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                info!("hook feedback: {line}")
            }

            Ok(())
        } else {
            for line in output.feedback {
                error!("hook feedback: {line}")
            }
            Err(Error::HookExecFailed {
                status,
                script: self.path.clone(),
            })
        }
    }
}

impl Hook<OnLaunch> {
    /// Execute a on-launch hook.
    ///
    /// # Hook Input
    /// - none
    ///
    /// # Hook Output
    /// - JSON not allowed.
    /// - all emitted non-JSON lines are considered feedback or error messages
    ///   depending on the status code.
    ///
    /// # Errors
    ///
    /// This function will return an error if
    /// - The hook execution failed.
    pub fn execute(&self) -> Result<(), Error> {
        let (status, output) = call_hook(self, [].into_iter(), |_| Ok::<_, Error>(()))?;

        if status.success() {
            if !output.json.is_empty() {
                return Err(Error::WrongTaskNum {
                    expected: 0,
                    tasks: output.json.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                info!("hook feedback: {line}")
            }

            Ok(())
        } else {
            for line in output.feedback {
                error!("hook feedback: {line}")
            }
            Err(Error::HookExecFailed {
                status,
                script: self.path.clone(),
            })
        }
    }
}
