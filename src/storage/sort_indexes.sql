-- Sort read index, additive only. Applied on every open like the
-- collection tables: `PRAGMA user_version` stays at 8, so an older githubdw
-- still opens an upgraded warehouse, and `IF NOT EXISTS` makes a repeat open
-- a no-op.
--
-- Ordering a whole selection of pull requests by comment count, or by the
-- status a listing shows (state plus draft), read every pull request's full
-- row (bodies included) because no index holds comment_count or is_draft:
-- 37.7 s on a cold 17 GB copy for 49k pull requests. This index holds both,
-- and the row id, so either read is one pass over a small index. It leads
-- with state, so it also serves every lookup idx_pr_state serves; that index
-- is kept, since this file only adds. Sizes and dates are already covered
-- by idx_pr_author_created_cover, idx_pr_created_at and idx_pr_updated.
CREATE INDEX IF NOT EXISTS idx_pr_state_draft_comments
    ON fact_pull_requests(state, is_draft, comment_count);
