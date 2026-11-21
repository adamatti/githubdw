//! Clearing stored patch text while keeping every file row and its counts.

use chrono::{Duration, Utc};
use rusqlite::{OptionalExtension, params};

use crate::GithubDW;
use crate::error::{Error, Result};
use crate::storage::sync_state_repository::SyncLease;

/// Rows cleared per committed batch, so a stop loses nothing done so far.
const PURGE_BATCH: i64 = 5_000;
/// The lock the purge holds while it runs.
const PURGE_LOCK: &str = "purge:diffs";
/// Same age as the sync lock's own stale rule.
const LIVE_LOCK_MINUTES: i64 = 30;

/// What [`GithubDW::purge_stored_diffs`] cleared, or would clear.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct PurgeReport {
    /// Rows whose `patch` went from non-empty to NULL.
    pub rows_cleared: u64,
    /// `sum(length(patch))` of those rows, in bytes.
    pub bytes_cleared: u64,
    pub vacuumed: bool,
    /// Database size (`page_count * page_size`) before and after.
    pub file_bytes_before: u64,
    pub file_bytes_after: u64,
}

impl GithubDW {
    /// Clear stored patch text (`fact_file_diffs.patch` -> NULL). Keeps every
    /// row and its file_path, previous_path, change_type, additions and
    /// deletions, so file lists and counts are unchanged. Nothing reads a
    /// NULL patch as an incomplete pull request, so no sync refetches it.
    ///
    /// Refuses with [`Error::SyncInProgress`] while any sync holds a live
    /// lock, and holds its own lock while it runs. Commits in batches of
    /// 5,000 rows. With `vacuum`, runs VACUUM after the last batch.
    pub fn purge_stored_diffs(&self, vacuum: bool) -> Result<PurgeReport> {
        self.refuse_while_syncing()?;
        let lease = SyncLease::acquire(self.connection(), PURGE_LOCK)?;
        let out = self.purge_locked(vacuum);
        let released = lease.release();
        let report = out?;
        released?;
        Ok(report)
    }

    /// What [`Self::purge_stored_diffs`] would clear; writes nothing.
    pub fn purge_stored_diffs_dry_run(&self) -> Result<PurgeReport> {
        let (rows, bytes): (i64, i64) = self.connection().query_row(
            "SELECT count(*), coalesce(sum(length(CAST(patch AS BLOB))), 0)
               FROM fact_file_diffs WHERE patch IS NOT NULL AND patch != ''",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let size = self.file_bytes()?;
        Ok(PurgeReport {
            rows_cleared: rows as u64,
            bytes_cleared: bytes as u64,
            vacuumed: false,
            file_bytes_before: size,
            file_bytes_after: size,
        })
    }

    fn refuse_while_syncing(&self) -> Result<()> {
        let since = Utc::now() - Duration::minutes(LIVE_LOCK_MINUTES);
        let mut statement = self
            .connection()
            .prepare("SELECT entity_key, started_at FROM sync_locks WHERE entity_key != ?1")?;
        let locks = statement
            .query_map([PURGE_LOCK], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        // A lock whose time cannot be read counts as stale, as in the sync.
        let live = locks.into_iter().find(|(_, started)| {
            started
                .parse::<chrono::DateTime<Utc>>()
                .is_ok_and(|t| t > since)
        });
        match live {
            Some((entity_key, _)) => Err(Error::SyncInProgress(entity_key)),
            None => Ok(()),
        }
    }

    fn purge_locked(&self, vacuum: bool) -> Result<PurgeReport> {
        let connection = self.connection();
        let mut report = PurgeReport {
            file_bytes_before: self.file_bytes()?,
            ..PurgeReport::default()
        };
        let mut after: i64 = 0;
        loop {
            let tx = connection.unchecked_transaction()?;
            let batch: Option<(i64, i64, i64)> = tx
                .query_row(
                    "SELECT count(*), coalesce(sum(length(CAST(patch AS BLOB))), 0), max(rowid)
                       FROM (SELECT rowid, patch FROM fact_file_diffs
                              WHERE rowid > ?1 AND patch IS NOT NULL AND patch != ''
                              ORDER BY rowid LIMIT ?2)
                     HAVING count(*) > 0",
                    params![after, PURGE_BATCH],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let Some((rows, bytes, last)) = batch else {
                break;
            };
            tx.execute(
                "UPDATE fact_file_diffs SET patch = NULL
                  WHERE rowid > ?1 AND rowid <= ?2 AND patch IS NOT NULL AND patch != ''",
                params![after, last],
            )?;
            tx.commit()?;
            report.rows_cleared += rows as u64;
            report.bytes_cleared += bytes as u64;
            after = last;
        }
        if vacuum {
            connection.execute_batch("VACUUM")?;
            report.vacuumed = true;
        }
        report.file_bytes_after = self.file_bytes()?;
        Ok(report)
    }

    fn file_bytes(&self) -> Result<u64> {
        let pages: i64 = self
            .connection()
            .query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let size: i64 = self
            .connection()
            .query_row("PRAGMA page_size", [], |r| r.get(0))?;
        Ok((pages * size) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(warehouse: &GithubDW, n: usize) {
        let connection = warehouse.connection();
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        for i in 0..n {
            let patch = (i % 3 != 0).then_some("@@ -1 +1 @@\n+abc");
            connection
                .execute(
                    "INSERT INTO fact_file_diffs (file_diff_key, pr_key, repo_key, file_path,
                         change_type, patch, additions, deletions)
                     VALUES (?1, 'pr:o/r#1', 'repo:o/r', ?2, 'modified', ?3, 1, 2)",
                    params![format!("pr:o/r#1:f{i}"), format!("f{i}"), patch],
                )
                .unwrap();
        }
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .unwrap();
    }

    fn totals(warehouse: &GithubDW) -> (i64, i64, i64) {
        warehouse
            .connection()
            .query_row(
                "SELECT count(*), count(patch), sum(additions) FROM fact_file_diffs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap()
    }

    #[test]
    fn purge_clears_patch_text_and_keeps_rows_and_counts() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = GithubDW::open(dir.path().join("g.db")).unwrap();
        seed(&warehouse, 12_001);
        let with_text = (0..12_001).filter(|i| i % 3 != 0).count() as u64;
        let patch_bytes = "@@ -1 +1 @@\n+abc".len() as u64;
        let plan = warehouse.purge_stored_diffs_dry_run().unwrap();
        assert_eq!(
            (plan.rows_cleared, plan.bytes_cleared),
            (with_text, with_text * patch_bytes)
        );
        assert_eq!(
            totals(&warehouse).1,
            with_text as i64,
            "dry run writes nothing"
        );

        let report = warehouse.purge_stored_diffs(true).unwrap();
        assert_eq!(
            (report.rows_cleared, report.bytes_cleared),
            (plan.rows_cleared, plan.bytes_cleared)
        );
        assert!(report.vacuumed);
        assert_eq!(totals(&warehouse), (12_001, 0, 12_001));
        assert_eq!(warehouse.purge_stored_diffs(false).unwrap().rows_cleared, 0);
    }

    #[test]
    fn purge_refuses_while_a_sync_holds_its_lock() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = GithubDW::open(dir.path().join("g.db")).unwrap();
        seed(&warehouse, 3);
        let _lease = SyncLease::acquire(warehouse.connection(), "repo:o/r").unwrap();
        let error = warehouse.purge_stored_diffs(false).unwrap_err();
        assert!(matches!(error, Error::SyncInProgress(_)), "{error}");
        assert_eq!(totals(&warehouse).1, 2);
    }
}
