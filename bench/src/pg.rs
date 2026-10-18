//! The engine's Postgres work, from `pg_stat_statements` and the table
//! statistics.
//!
//! Each scenario starts from reset statistics and reads them when it ends,
//! so what is reported is the work that scenario made the engine do: per
//! statement, the calls, rows returned or written, shared buffers hit and
//! read, and WAL written; per table, the tuples read by sequential and index
//! scans and the tuples written. Statements are grouped into classes by verb
//! and main table (`select workflow_execution_journal`), which is stable
//! across engine versions where the exact SQL is not.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};

/// The table the engine keeps run journals in.
pub const JOURNAL_TABLE: &str = "workflow_execution_journal";

/// What one statement, or one class of statements, cost.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatementStats {
    pub calls: u64,
    /// Rows returned or written.
    pub rows: u64,
    pub shared_blks_hit: u64,
    pub shared_blks_read: u64,
    pub shared_blks_dirtied: u64,
    pub shared_blks_written: u64,
    pub wal_bytes: u64,
    /// Server time spent executing, recorded but not compared: it is a laptop's.
    pub exec_ms: f64,
}

impl StatementStats {
    fn add(&mut self, other: &StatementStats) {
        self.calls += other.calls;
        self.rows += other.rows;
        self.shared_blks_hit += other.shared_blks_hit;
        self.shared_blks_read += other.shared_blks_read;
        self.shared_blks_dirtied += other.shared_blks_dirtied;
        self.shared_blks_written += other.shared_blks_written;
        self.wal_bytes += other.wal_bytes;
        self.exec_ms += other.exec_ms;
    }

    /// Shared buffers touched: hit plus read.
    pub fn blocks(&self) -> u64 {
        self.shared_blks_hit + self.shared_blks_read
    }
}

/// What one table saw.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableStats {
    pub seq_scan: u64,
    pub seq_tup_read: u64,
    pub idx_scan: u64,
    pub idx_tup_fetch: u64,
    pub n_tup_ins: u64,
    pub n_tup_upd: u64,
    pub n_tup_del: u64,
}

/// Everything read from Postgres for a scenario.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PgStats {
    /// Every statement, together.
    pub total: StatementStats,
    /// By class (verb and main table).
    pub classes: BTreeMap<String, StatementStats>,
    /// By table, only tables that saw something.
    pub tables: BTreeMap<String, TableStats>,
    /// The costliest statements by buffers touched, with their text, to
    /// see what a class is made of.
    pub top_statements: Vec<(String, StatementStats)>,
}

impl PgStats {
    /// Statements that read the journal.
    pub fn journal_reads(&self) -> StatementStats {
        self.classes
            .get(&format!("select {JOURNAL_TABLE}"))
            .cloned()
            .unwrap_or_default()
    }

    /// Statements that wrote anything (insert, update, delete).
    pub fn writes(&self) -> StatementStats {
        let mut total = StatementStats::default();
        for (class, stats) in &self.classes {
            if class.starts_with("insert ")
                || class.starts_with("update ")
                || class.starts_with("delete ")
            {
                total.add(stats);
            }
        }
        total
    }
}

/// A connection to the bench's Postgres.
pub struct Pg {
    client: Client,
}

impl Pg {
    pub async fn connect(url: &str) -> Result<Self, String> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| format!("could not connect to the bench database: {e}"))?;
        tokio::spawn(connection);
        client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .await
            .map_err(|e| format!("pg_stat_statements is not available: {e}"))?;
        Ok(Self { client })
    }

    /// Forgets every statistic, so what is read next is only what follows.
    pub async fn reset(&self) -> Result<(), String> {
        // Table statistics reach the shared counters when a backend's
        // transaction ends, at most a second or so later: let the engine's
        // last statements land before forgetting them.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        self.client
            .batch_execute("SELECT pg_stat_statements_reset(); SELECT pg_stat_reset();")
            .await
            .map_err(|e| format!("could not reset the statistics: {e}"))
    }

    /// Reads what the engine did since the last reset.
    pub async fn read(&self) -> Result<PgStats, String> {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let rows = self
            .client
            .query(
                "SELECT s.query, s.calls, s.rows, s.shared_blks_hit, s.shared_blks_read, \
                        s.shared_blks_dirtied, s.shared_blks_written, s.wal_bytes::bigint, \
                        s.total_exec_time \
                 FROM pg_stat_statements s \
                 JOIN pg_database d ON d.oid = s.dbid \
                 WHERE d.datname = current_database() \
                   AND s.query NOT ILIKE '%pg_stat%' \
                   AND s.query NOT ILIKE '%pg_database%'",
                &[],
            )
            .await
            .map_err(|e| format!("could not read pg_stat_statements: {e}"))?;
        let mut stats = PgStats::default();
        let mut all = Vec::new();
        for row in rows {
            let query: String = row.get(0);
            let n = |i: usize| row.get::<_, i64>(i).max(0) as u64;
            let statement = StatementStats {
                calls: n(1),
                rows: n(2),
                shared_blks_hit: n(3),
                shared_blks_read: n(4),
                shared_blks_dirtied: n(5),
                shared_blks_written: n(6),
                wal_bytes: row.get::<_, Option<i64>>(7).unwrap_or(0).max(0) as u64,
                exec_ms: row.get(8),
            };
            stats.total.add(&statement);
            stats
                .classes
                .entry(classify(&query))
                .or_default()
                .add(&statement);
            all.push((query, statement));
        }
        all.sort_by_key(|(_, s)| std::cmp::Reverse(s.blocks()));
        stats.top_statements = all
            .into_iter()
            .take(8)
            .map(|(query, s)| (shorten(&query), s))
            .collect();

        let tables = self
            .client
            .query(
                "SELECT relname, seq_scan, seq_tup_read, COALESCE(idx_scan, 0), \
                        COALESCE(idx_tup_fetch, 0), n_tup_ins, n_tup_upd, n_tup_del \
                 FROM pg_stat_user_tables",
                &[],
            )
            .await
            .map_err(|e| format!("could not read the table statistics: {e}"))?;
        for row in tables {
            let n = |i: usize| row.get::<_, i64>(i).max(0) as u64;
            let table = TableStats {
                seq_scan: n(1),
                seq_tup_read: n(2),
                idx_scan: n(3),
                idx_tup_fetch: n(4),
                n_tup_ins: n(5),
                n_tup_upd: n(6),
                n_tup_del: n(7),
            };
            let touched = table.seq_scan
                + table.idx_scan
                + table.n_tup_ins
                + table.n_tup_upd
                + table.n_tup_del;
            if touched > 0 {
                stats.tables.insert(row.get(0), table);
            }
        }
        Ok(stats)
    }
}

/// A statement's class: its verb and the first table it names.
pub fn classify(query: &str) -> String {
    let lower = query.to_ascii_lowercase();
    let words: Vec<&str> = lower
        .split(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',' || c == ';')
        .filter(|w| !w.is_empty())
        .collect();
    let verb = words
        .iter()
        .find(|w| {
            matches!(
                **w,
                "select" | "insert" | "update" | "delete" | "begin" | "commit" | "rollback"
            )
        })
        .copied()
        .unwrap_or_else(|| words.first().copied().unwrap_or("other"));
    let target = match verb {
        "insert" => after(&words, "into"),
        "update" => after(&words, "update"),
        "delete" | "select" => after(&words, "from"),
        _ => None,
    };
    match target {
        Some(table) => format!("{verb} {}", table.trim_matches('"')),
        None => verb.to_string(),
    }
}

fn after<'a>(words: &[&'a str], keyword: &str) -> Option<&'a str> {
    let position = words.iter().position(|w| *w == keyword)?;
    words
        .get(position + 1)
        .copied()
        .map(|table| table.rsplit('.').next().unwrap_or(table))
        .filter(|table| {
            table
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '"')
        })
}

fn shorten(query: &str) -> String {
    let flat: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.len() > 160 {
        let mut end = 160;
        while !flat.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &flat[..end])
    } else {
        flat
    }
}

#[cfg(test)]
mod tests {
    use super::classify;

    #[test]
    fn statements_are_classed_by_verb_and_table() {
        assert_eq!(
            classify("SELECT entry_id, x FROM workflow_execution_journal WHERE a = $1"),
            "select workflow_execution_journal"
        );
        assert_eq!(
            classify("INSERT INTO public.task_executions (a) VALUES ($1)"),
            "insert task_executions"
        );
        assert_eq!(
            classify("UPDATE workflow_executions SET a = $1"),
            "update workflow_executions"
        );
        assert_eq!(
            classify("WITH c AS (SELECT id FROM task_executions) UPDATE task_executions SET x = 1"),
            "select task_executions"
        );
        assert_eq!(classify("BEGIN"), "begin");
    }
}
