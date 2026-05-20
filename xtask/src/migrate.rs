//! `cargo xtask migrate-recurring` - convert TaskWarrior-style recurring tasks
//! into the new iterative-task format
//!
//! This is intended as a one-time migration. Existing pending child tasks are
//! deliberately left untouched so users keep their in-flight work.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use taskchampion::storage::AccessMode;
use taskchampion::{IterType, Operations, Replica, SqliteStorage, Status, Uuid};

/// Result of a migration run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MigrateSummary {
    /// Recurring tasks that were converted to iterative.
    pub migrated: usize,
    /// Pending child tasks left untouched.
    pub children_left: usize,
    /// Recurring-status tasks without a `recur` UDA, which were skipped.
    pub skipped_no_recur: usize,
}

/// CLI entry point used by `xtask` `main`.
pub fn migrate_recurring_cli(args: &[String]) -> Result<()> {
    let mut store: Option<PathBuf> = None;
    let mut dry_run = false;
    let mut iter_type = IterType::Chained;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--store" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow!("--store requires a path argument"))?;
                store = Some(PathBuf::from(v));
            }
            "--dry-run" => dry_run = true,
            "--iter-type" => {
                let v = iter
                    .next()
                    .ok_or_else(|| anyhow!("--iter-type requires a value"))?;
                iter_type =
                    IterType::from_str(v).map_err(|e| anyhow!("invalid --iter-type {v:?}: {e}"))?;
            }
            other => anyhow::bail!("unknown argument {other:?}"),
        }
    }
    let store = store.ok_or_else(|| anyhow!("--store <PATH> is required"))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    let summary = runtime.block_on(migrate_recurring(&store, dry_run, iter_type))?;

    let mode = if dry_run { " (dry-run)" } else { "" };
    println!(
        "migrate-recurring{mode}: migrated={}, children_left_untouched={}, skipped_no_recur={}",
        summary.migrated, summary.children_left, summary.skipped_no_recur
    );
    Ok(())
}

/// Run the migration against a sqlite store. Returns counts and commits unless
/// `dry_run` is true.
pub async fn migrate_recurring(
    store_path: &Path,
    dry_run: bool,
    iter_type: IterType,
) -> Result<MigrateSummary> {
    let storage = SqliteStorage::new(store_path, AccessMode::ReadWrite, false)
        .await
        .with_context(|| format!("opening sqlite store at {}", store_path.display()))?;
    let mut replica = Replica::new(storage);

    let mut tasks = replica.all_tasks().await.context("loading tasks")?;

    // Two-pass: first identify recurring task UUIDs so we can spot their children
    // in the second pass.
    let recurring_uuids: Vec<Uuid> = tasks
        .iter()
        .filter_map(|(uuid, task)| (task.get_status() == Status::Recurring).then_some(*uuid))
        .collect();

    let mut summary = MigrateSummary::default();
    let mut ops = Operations::new();

    for uuid in &recurring_uuids {
        let task = tasks.get_mut(uuid).expect("uuid came from this map");
        let Some(recur) = task.get_value("recur").map(str::to_owned) else {
            summary.skipped_no_recur += 1;
            continue;
        };
        // Drop legacy recurring bookkeeping fields. `recur` is replaced by
        // `iter` with the same value.
        task.set_value("recur", None, &mut ops)?;
        task.set_value("mask", None, &mut ops)?;
        task.set_value("imask", None, &mut ops)?;
        task.set_value("iter", Some(recur), &mut ops)?;
        if task.get_value("iter_type").is_none() {
            task.set_value("iter_type", Some(iter_type.to_string()), &mut ops)?;
        }
        // Initialize rrule + due via the normal status transition.
        task.set_status(Status::Iterative, &mut ops)?;
        summary.migrated += 1;
    }

    // Count untouched pending children.
    for task in tasks.values() {
        if task.get_status() == Status::Pending {
            if let Some(parent_str) = task.get_value("parent") {
                if let Ok(parent_uuid) = Uuid::parse_str(parent_str) {
                    if recurring_uuids.contains(&parent_uuid) {
                        summary.children_left += 1;
                    }
                }
            }
        }
    }

    if !dry_run {
        replica
            .commit_operations(ops)
            .await
            .context("committing migration operations")?;
    }

    Ok(summary)
}
