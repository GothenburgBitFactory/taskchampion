use taskchampion::Task;

use crate::hook::{
    Hook,
    hook_kinds::{OnAdd, OnExit, OnLaunch, OnModify},
};

use super::{Error, call_hook};

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
        let (status, mut output) = call_hook(self, [added.compose_json().to_string()].into_iter())?;

        if status.success() {
            if output.tasks.len() != 1 {
                return Err(Error::WrongTaskNum {
                    expected: 1,
                    tasks: output.tasks.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                println!("FEEDBACK: {line}")
            }

            Ok(output.tasks.remove(0))
        } else {
            for line in output.feedback {
                println!("ERROR FEEDBACK: {line}")
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
        let (status, mut output) = call_hook(
            self,
            [
                original.compose_json().to_string(),
                modified.compose_json().to_string(),
            ]
            .into_iter(),
        )?;

        if status.success() {
            if output.tasks.len() != 1 {
                return Err(Error::WrongTaskNum {
                    expected: 1,
                    tasks: output.tasks.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                println!("FEEDBACK: {line}")
            }

            Ok(output.tasks.remove(0))
        } else {
            for line in output.feedback {
                println!("ERROR FEEDBACK: {line}")
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
                .map(|value| value.to_string()),
        )?;

        if status.success() {
            if !output.tasks.is_empty() {
                return Err(Error::WrongTaskNum {
                    expected: 0,
                    tasks: output.tasks.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                println!("FEEDBACK: {line}")
            }

            Ok(())
        } else {
            for line in output.feedback {
                println!("ERROR FEEDBACK: {line}")
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
        let (status, output) = call_hook(self, [].into_iter())?;

        if status.success() {
            if !output.tasks.is_empty() {
                return Err(Error::WrongTaskNum {
                    expected: 0,
                    tasks: output.tasks.len(),
                    script: self.path.clone(),
                });
            }

            for line in output.feedback {
                println!("FEEDBACK: {line}")
            }

            Ok(())
        } else {
            for line in output.feedback {
                println!("ERROR FEEDBACK: {line}")
            }
            Err(Error::HookExecFailed {
                status,
                script: self.path.clone(),
            })
        }
    }
}
