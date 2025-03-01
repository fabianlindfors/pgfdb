#![feature(noop_waker)]

use std::env;

use pgrx::pg_sys::RegisterXactCallback;
use pgrx::prelude::*;

::pgrx::pg_module_magic!();

mod fdb;
mod health;
mod iam;
mod subspace;
mod tam;
mod transaction;

#[pg_guard]
pub extern "C" fn _PG_init() {
    env::set_var("RUST_BACKTRACE", "1");

    fdb::init();

    unsafe {
        RegisterXactCallback(
            Some(transaction::transaction_callback),
            std::ptr::null_mut(),
        );
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn create_table() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
    }

    #[pg_test]
    fn insert() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();

        Spi::run("INSERT INTO test (id) VALUES (10)").unwrap();
    }

    #[pg_test]
    fn select() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();

        Spi::run("INSERT INTO test (id) VALUES (1), (2), (3), (4)").unwrap();

        let result: i32 = Spi::get_one("SELECT id FROM test WHERE id = 3")
            .unwrap()
            .unwrap();
        assert_eq!(3, result);
    }

    #[pg_test]
    fn aggregates() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();

        Spi::run("INSERT INTO test (id) VALUES (1), (2), (3), (4)").unwrap();

        let (count, avg): (Option<i64>, Option<pgrx::AnyNumeric>) =
            Spi::get_two("SELECT COUNT(*), AVG(id) FROM test").unwrap();
        assert_eq!(4, count.unwrap());
        assert_eq!("2.5", avg.unwrap().normalize());
    }

    #[pg_test]
    fn select_with_heap_index() {
        // Create table with a primary key index (will be a regular Postgres index)
        Spi::run("CREATE TABLE test (id INTEGER PRIMARY KEY) USING pgfdb").unwrap();
        Spi::run("INSERT INTO test (id) VALUES (1), (2), (3), (4)").unwrap();

        // Disable sequential scans to force index use
        Spi::run("SET enable_seqscan = off").unwrap();

        let count: i64 = Spi::get_one("SELECT COUNT(*) FROM test WHERE id > 2")
            .unwrap()
            .unwrap();
        assert_eq!(2, count);
    }

    #[pg_test]
    fn create_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();
    }

    #[pg_test]
    fn insert_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();
        Spi::run("INSERT INTO test(id) VALUES (1)").unwrap();
    }

    #[pg_test]
    fn select_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();

        // Ensure the select will use our index
        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id = 1;").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index returns the correct results
        Spi::run("INSERT INTO test(id) VALUES (1), (1), (2)").unwrap();
        let result: Option<i64> = Spi::get_one("SELECT count(*) FROM test WHERE id = 1").unwrap();
        assert_eq!(Some(2), result);
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
