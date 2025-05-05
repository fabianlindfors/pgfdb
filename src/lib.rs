use std::env;

use pgrx::pg_sys::RegisterXactCallback;
use pgrx::prelude::*;

::pgrx::pg_module_magic!();

mod coding;
mod errors;
mod fdb;
mod health;
mod iam;
mod subspace;
mod tam;
mod transaction;
mod tuple_cache;
mod utils;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
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
    use pgrx::{prelude::*, Uuid};

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
    fn update() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("INSERT INTO test (id) VALUES (10), (11), (12)").unwrap();

        Spi::run("UPDATE test SET id = 12 WHERE id = 10").unwrap();

        let result: i64 = Spi::get_one("SELECT count(*) FROM test WHERE id = 12")
            .unwrap()
            .unwrap();
        assert_eq!(2, result);
    }

    #[pg_test]
    fn delete() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("INSERT INTO test (id) VALUES (1), (2), (2), (3)").unwrap();

        let initial_count: i64 = Spi::get_one("SELECT count(*) FROM test WHERE id = 2")
            .unwrap()
            .unwrap();
        assert_eq!(2, initial_count);

        Spi::run("DELETE FROM test WHERE id = 2").unwrap();

        let remaining_count: i64 = Spi::get_one("SELECT count(*) FROM test").unwrap().unwrap();
        assert_eq!(2, remaining_count);
        let deleted_count: i64 = Spi::get_one("SELECT count(*) FROM test WHERE id = 2")
            .unwrap()
            .unwrap();
        assert_eq!(0, deleted_count);
    }

    #[pg_test]
    fn select() {
        Spi::run("CREATE TABLE test (id INTEGER, uuid UUID) USING pgfdb").unwrap();

        Spi::run(
            "INSERT INTO test (id, uuid) VALUES (1, gen_random_uuid()), (2, gen_random_uuid())",
        )
        .unwrap();

        let (id, uuid): (Option<i32>, Option<Uuid>) =
            Spi::get_two("SELECT id, uuid FROM test WHERE id = 2").unwrap();
        assert_eq!(2, id.unwrap());
        assert!(uuid.is_some());
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
    fn create_index_with_existing_rows() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("INSERT INTO test(id) VALUES (1), (2), (3)").unwrap();

        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();

        // Ensure the select will use our index
        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain(&format!("SELECT count(*) FROM test WHERE id = 2")).unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index returns the correct results
        let result: Option<i64> =
            Spi::get_one(&format!("SELECT count(*) FROM test WHERE id = 2")).unwrap();
        assert_eq!(Some(1), result);
    }

    #[pg_test]
    fn insert_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();
        Spi::run("INSERT INTO test(id) VALUES (1)").unwrap();
    }

    #[pg_test]
    fn update_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER, name TEXT) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX name_idx ON test USING pgfdb_idx(name)").unwrap();

        Spi::run("INSERT INTO test(id, name) VALUES (1, 'Test Person')").unwrap();
        Spi::run("UPDATE test SET name = 'Another Test Person' WHERE name = 'Test Person'")
            .unwrap();

        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain(&format!(
            "SELECT count(*) FROM test WHERE name = 'Another Test Person'"
        ))
        .unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        let result: Option<i64> = Spi::get_one(&format!(
            "SELECT count(*) FROM test WHERE name = 'Another Test Person'"
        ))
        .unwrap();
        assert_eq!(Some(1), result);
    }

    #[pg_test]
    fn delete_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER, name TEXT) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX name_idx ON test USING pgfdb_idx(name)").unwrap();

        Spi::run("INSERT INTO test(id, name) VALUES (1, 'Test Person')").unwrap();
        Spi::run("DELETE FROM test WHERE name = 'Test Person'").unwrap();

        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain(&format!(
            "SELECT count(*) FROM test WHERE name = 'Test Person'"
        ))
        .unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        let result: Option<i64> = Spi::get_one(&format!(
            "SELECT count(*) FROM test WHERE name = 'Test Person'"
        ))
        .unwrap();
        assert_eq!(Some(0), result);
    }

    const INTEGER_TEST_VALUES: (&'static str, &'static str, &'static str) = ("1", "2", "3");
    const FLOAT_TEST_VALUES: (&'static str, &'static str, &'static str) = ("1.1", "2.2", "3.3");
    const STRING_TEST_VALUES: (&'static str, &'static str, &'static str) =
        ("'test1'", "'test2'", "'test3'");
    const UUID_TEST_VALUES: (&'static str, &'static str, &'static str) = (
        "'00be8a3b-7747-4d96-a60e-0a0289825433'",
        "'573a831e-4c5a-4888-b98f-51f8e0017985'",
        "'c552b673-47ba-4734-bd34-36905f1bf815'",
    );

    #[pg_test]
    fn select_eq_with_index() {
        let cases = vec![
            ("INTEGER", INTEGER_TEST_VALUES),
            ("BIGINT", INTEGER_TEST_VALUES),
            ("SMALLINT", INTEGER_TEST_VALUES),
            ("REAL", FLOAT_TEST_VALUES),
            ("FLOAT", FLOAT_TEST_VALUES),
            ("TEXT", STRING_TEST_VALUES),
            ("UUID", UUID_TEST_VALUES),
        ];

        for (column_type, (value1, value2, value3)) in cases {
            let table = format!("test_{}", column_type.to_lowercase());

            Spi::run(&format!(
                "CREATE TABLE {table} (id {column_type}) USING pgfdb"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb_idx(id)"
            ))
            .unwrap();

            // Ensure the select will use our index
            Spi::run("SET enable_seqscan=0").unwrap();
            let explain = Spi::explain(&format!(
                "SELECT count(*) FROM {table} WHERE id = CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            // Ensure querying using the index returns the correct results
            Spi::run(&format!(
                "INSERT INTO {table}(id) VALUES ({value1}), ({value1}), ({value2}), ({value2}), ({value3}), ({value3})"
            ))
            .unwrap();
            let result: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM {table} WHERE id = CAST({value2} as {column_type})"
            ))
            .unwrap();
            assert_eq!(Some(2), result);
        }
    }

    #[pg_test]
    fn select_not_eq_with_index() {
        let cases = vec![
            ("INTEGER", INTEGER_TEST_VALUES),
            ("BIGINT", INTEGER_TEST_VALUES),
            ("SMALLINT", INTEGER_TEST_VALUES),
            ("REAL", FLOAT_TEST_VALUES),
            ("FLOAT", FLOAT_TEST_VALUES),
            ("TEXT", STRING_TEST_VALUES),
            ("UUID", UUID_TEST_VALUES),
        ];

        for (column_type, (value1, value2, value3)) in cases {
            let table = format!("test_{}", column_type.to_lowercase());

            Spi::run(&format!(
                "CREATE TABLE {table} (id {column_type}) USING pgfdb"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb_idx(id)"
            ))
            .unwrap();

            // Ensure the select will use our index
            Spi::run("SET enable_seqscan=0").unwrap();
            let explain = Spi::explain(&format!(
                "SELECT count(*) FROM {table} WHERE id != CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            // Ensure querying using the index returns the correct results
            Spi::run(&format!(
                "INSERT INTO {table}(id) VALUES ({value1}), ({value1}), ({value2}), ({value2}), ({value3}), ({value3})"
            ))
            .unwrap();
            let result: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM {table} WHERE id != CAST({value2} as {column_type})"
            ))
            .unwrap();
            assert_eq!(Some(4), result);
        }
    }

    #[pg_test]
    fn select_lt_with_index() {
        let cases = vec![
            ("INTEGER", INTEGER_TEST_VALUES),
            ("BIGINT", INTEGER_TEST_VALUES),
            ("SMALLINT", INTEGER_TEST_VALUES),
            ("REAL", FLOAT_TEST_VALUES),
            ("FLOAT", FLOAT_TEST_VALUES),
        ];

        for (column_type, (value1, value2, value3)) in cases {
            let table = format!("test_{}", column_type.to_lowercase());

            Spi::run(&format!(
                "CREATE TABLE {table} (id {column_type}) USING pgfdb"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb_idx(id)"
            ))
            .unwrap();

            // Ensure the select will use our index
            Spi::run("SET enable_seqscan=0").unwrap();
            let explain = Spi::explain(&format!(
                "SELECT count(*) FROM {table} WHERE id < CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            // Ensure querying using the index returns the correct results
            Spi::run(&format!(
                "INSERT INTO {table}(id) VALUES ({value1}), ({value1}), ({value2}), ({value2}), ({value3}), ({value3})"
            ))
            .unwrap();
            let result: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM {table} WHERE id < CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert_eq!(Some(2), result);
        }
    }

    #[pg_test]
    fn select_lte_with_index() {
        let cases = vec![
            ("INTEGER", INTEGER_TEST_VALUES),
            ("BIGINT", INTEGER_TEST_VALUES),
            ("SMALLINT", INTEGER_TEST_VALUES),
            ("REAL", FLOAT_TEST_VALUES),
            ("FLOAT", FLOAT_TEST_VALUES),
        ];

        for (column_type, (value1, value2, value3)) in cases {
            let table = format!("test_{}", column_type.to_lowercase());

            Spi::run(&format!(
                "CREATE TABLE {table} (id {column_type}) USING pgfdb"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb_idx(id)"
            ))
            .unwrap();

            // Ensure the select will use our index
            Spi::run("SET enable_seqscan=0").unwrap();
            let explain = Spi::explain(&format!(
                "SELECT count(*) FROM {table} WHERE id <= CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            // Ensure querying using the index returns the correct results
            Spi::run(&format!(
                "INSERT INTO {table}(id) VALUES ({value1}), ({value1}), ({value2}), ({value2}), ({value3}), ({value3})"
            ))
            .unwrap();
            let result: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM {table} WHERE id <= CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert_eq!(Some(4), result);
        }
    }

    #[pg_test]
    fn select_gt_with_index() {
        let cases = vec![
            ("INTEGER", INTEGER_TEST_VALUES),
            ("BIGINT", INTEGER_TEST_VALUES),
            ("SMALLINT", INTEGER_TEST_VALUES),
            ("REAL", FLOAT_TEST_VALUES),
            ("FLOAT", FLOAT_TEST_VALUES),
        ];

        for (column_type, (value1, value2, value3)) in cases {
            let table = format!("test_{}", column_type.to_lowercase());

            Spi::run(&format!(
                "CREATE TABLE {table} (id {column_type}) USING pgfdb"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb_idx(id)"
            ))
            .unwrap();

            // Ensure the select will use our index
            Spi::run("SET enable_seqscan=0").unwrap();
            let explain = Spi::explain(&format!(
                "SELECT count(*) FROM {table} WHERE id > CAST({value1} AS {column_type})"
            ))
            .unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            // Ensure querying using the index returns the correct results
            Spi::run(&format!(
                "INSERT INTO {table}(id) VALUES ({value1}), ({value1}), ({value2}), ({value2}), ({value3}), ({value3})"
            ))
            .unwrap();
            let result: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM {table} WHERE id > CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert_eq!(Some(2), result);
        }
    }

    #[pg_test]
    fn select_gte_with_index() {
        let cases = vec![
            ("INTEGER", INTEGER_TEST_VALUES),
            ("BIGINT", INTEGER_TEST_VALUES),
            ("SMALLINT", INTEGER_TEST_VALUES),
            ("REAL", FLOAT_TEST_VALUES),
            ("FLOAT", FLOAT_TEST_VALUES),
        ];

        for (column_type, (value1, value2, value3)) in cases {
            let table = format!("test_{}", column_type.to_lowercase());

            Spi::run(&format!(
                "CREATE TABLE {table} (id {column_type}) USING pgfdb"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb_idx(id)"
            ))
            .unwrap();

            // Ensure the select will use our index
            Spi::run("SET enable_seqscan=0").unwrap();
            let explain = Spi::explain(&format!(
                "SELECT count(*) FROM {table} WHERE id >= CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            // Ensure querying using the index returns the correct results
            Spi::run(&format!(
                "INSERT INTO {table}(id) VALUES ({value1}), ({value1}), ({value2}), ({value2}), ({value3}), ({value3})"
            ))
            .unwrap();
            let result: Option<i64> = Spi::get_one(&format!(
                "SELECT count(*) FROM {table} WHERE id >= CAST({value2} AS {column_type})"
            ))
            .unwrap();
            assert_eq!(Some(4), result);
        }
    }

    #[pg_test]
    fn select_lt_on_nulls_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();

        // Ensure the select will use our index
        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id < 2;").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index does not count the NULL value
        Spi::run("INSERT INTO test(id) VALUES (NULL), (1), (2), (3), (3)").unwrap();
        let result: Option<i64> = Spi::get_one("SELECT count(*) FROM test WHERE id < 2").unwrap();
        assert_eq!(Some(1), result);
    }

    #[pg_test]
    fn select_null_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();

        // Ensure the select will use our index
        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id IS NULL;").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index returns the correct results
        Spi::run("INSERT INTO test(id) VALUES (1), (NULL), (NULL), (3)").unwrap();
        let result: Option<i64> =
            Spi::get_one("SELECT count(*) FROM test WHERE id IS NULL").unwrap();
        assert_eq!(Some(2), result);
    }

    #[pg_test]
    fn select_not_null_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id)").unwrap();

        // Ensure the select will use our index
        Spi::run("SET enable_seqscan=0").unwrap();
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id IS NOT NULL;").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index returns the correct results
        Spi::run("INSERT INTO test(id) VALUES (1), (NULL), (NULL), (3)").unwrap();
        let result: Option<i64> =
            Spi::get_one("SELECT count(*) FROM test WHERE id IS NOT NULL").unwrap();
        assert_eq!(Some(2), result);
    }

    #[pg_test]
    fn select_eq_with_index_and_non_indexed_column() {
        // Create a table with a non-indexed column first, followed by an indexed column
        // This ensures we don't rely on column numbering between indices and tables
        Spi::run("CREATE TABLE test (id INTEGER, name TEXT) USING pgfdb").unwrap();
        Spi::run("INSERT INTO test(id, name) VALUES (1, 'test1'), (2, 'test1'), (3, 'test2')")
            .unwrap();

        // Create index with existing rows to trigger index build that constructs index keys from table tuples
        Spi::run("CREATE INDEX name_idx ON test USING pgfdb_idx(name)").unwrap();
        Spi::run("SET enable_seqscan=0").unwrap();

        // Ensure a select will use our index
        let explain = Spi::explain("SELECT count(*) FROM test WHERE name = 'test1';").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index returns the correct results
        let result: Option<i64> =
            Spi::get_one("SELECT count(*) FROM test WHERE name = 'test1'").unwrap();
        assert_eq!(Some(2), result);
    }

    #[pg_test]
    fn select_eq_with_multi_column_index() {
        Spi::run("CREATE TABLE test (id1 INTEGER, id2 INTEGER) USING pgfdb").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb_idx(id1, id2)").unwrap();
        Spi::run("SET enable_seqscan=0").unwrap();

        // Ensure a select on the initial column will use our index
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id1 = 1;").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying using the index returns the correct results
        Spi::run("INSERT INTO test(id1, id2) VALUES (1, 1), (1, 2), (2, 1)").unwrap();
        let result: Option<i64> = Spi::get_one("SELECT count(*) FROM test WHERE id1 = 1").unwrap();
        assert_eq!(Some(2), result);

        // Ensure a select on the second column does not use index
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id2 = 1").unwrap();
        assert!(
            !format!("{:?}", explain).contains("Index Name"),
            "expected query plan to not use index: {:?}",
            explain.0.to_string()
        );

        // Ensure a select on both columns will use our index
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id1 = 1 AND id2 = 2").unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Ensure querying on both columns using the index returns the correct results
        let result: Option<i64> =
            Spi::get_one("SELECT count(*) FROM test WHERE id1 = 1 AND id2 = 2").unwrap();
        assert_eq!(Some(1), result);

        // Ensure an OR select on both columns will not use our index
        let explain = Spi::explain("SELECT count(*) FROM test WHERE id1 = 1 OR id2 = 2").unwrap();
        assert!(
            !format!("{:?}", explain).contains("Index Name"),
            "expected query plan to not use index: {:?}",
            explain.0.to_string()
        );
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
