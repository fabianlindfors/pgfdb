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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
    }

    #[pg_test]
    fn insert() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();

        Spi::run("INSERT INTO test (id) VALUES (10)").unwrap();
    }

    #[pg_test]
    fn update() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("INSERT INTO test (id) VALUES (10), (11), (12)").unwrap();

        Spi::run("UPDATE test SET id = 12 WHERE id = 10").unwrap();

        let result: i64 = Spi::get_one("SELECT count(*) FROM test WHERE id = 12")
            .unwrap()
            .unwrap();
        assert_eq!(2, result);
    }

    #[pg_test]
    fn delete() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
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
        Spi::run("CREATE TABLE test (id INTEGER, uuid UUID) USING pgfdb_table").unwrap();

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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();

        Spi::run("INSERT INTO test (id) VALUES (1), (2), (3), (4)").unwrap();

        let (count, avg): (Option<i64>, Option<pgrx::AnyNumeric>) =
            Spi::get_two("SELECT COUNT(*), AVG(id) FROM test").unwrap();
        assert_eq!(4, count.unwrap());
        assert_eq!("2.5", avg.unwrap().normalize());
    }

    #[pg_test]
    fn select_with_heap_index() {
        // Create table with a primary key index (will be a regular Postgres index)
        Spi::run("CREATE TABLE test (id INTEGER PRIMARY KEY) USING pgfdb_table").unwrap();
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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id)").unwrap();
    }

    #[pg_test]
    fn create_index_with_existing_rows() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("INSERT INTO test(id) VALUES (1), (2), (3)").unwrap();

        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id)").unwrap();

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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id)").unwrap();
        Spi::run("INSERT INTO test(id) VALUES (1)").unwrap();
    }

    #[pg_test]
    fn update_with_index() {
        Spi::run("CREATE TABLE test (id INTEGER, name TEXT) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX name_idx ON test USING pgfdb(name)").unwrap();

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
        Spi::run("CREATE TABLE test (id INTEGER, name TEXT) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX name_idx ON test USING pgfdb(name)").unwrap();

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
                "CREATE TABLE {table} (id {column_type}) USING pgfdb_table"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb(id)"
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
                "CREATE TABLE {table} (id {column_type}) USING pgfdb_table"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb(id)"
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
                "CREATE TABLE {table} (id {column_type}) USING pgfdb_table"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb(id)"
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
                "CREATE TABLE {table} (id {column_type}) USING pgfdb_table"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb(id)"
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
                "CREATE TABLE {table} (id {column_type}) USING pgfdb_table"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb(id)"
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
                "CREATE TABLE {table} (id {column_type}) USING pgfdb_table"
            ))
            .unwrap();
            Spi::run(&format!(
                "CREATE INDEX {table}_id_idx ON {table} USING pgfdb(id)"
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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id)").unwrap();

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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id)").unwrap();

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
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id)").unwrap();

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
        Spi::run("CREATE TABLE test (id INTEGER, name TEXT) USING pgfdb_table").unwrap();
        Spi::run("INSERT INTO test(id, name) VALUES (1, 'test1'), (2, 'test1'), (3, 'test2')")
            .unwrap();

        // Create index with existing rows to trigger index build that constructs index keys from table tuples
        Spi::run("CREATE INDEX name_idx ON test USING pgfdb(name)").unwrap();
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
        Spi::run("CREATE TABLE test (id1 INTEGER, id2 INTEGER) USING pgfdb_table").unwrap();
        Spi::run("CREATE INDEX id_idx ON test USING pgfdb(id1, id2)").unwrap();
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

    #[pg_test]
    fn join_with_table_scans() {
        // Create two tables with pgfdb storage
        Spi::run(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER) USING pgfdb_table",
        )
        .unwrap();
        Spi::run("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT) USING pgfdb_table")
            .unwrap();

        // Insert test data
        Spi::run(
            "INSERT INTO customers (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO orders (id, customer_id) VALUES (101, 1), (102, 1), (103, 2), (104, NULL)",
        )
        .unwrap();

        // Test inner join
        let count: Option<i64> = Spi::get_one(
            "SELECT COUNT(*) FROM orders o INNER JOIN customers c ON o.customer_id = c.id",
        )
        .unwrap();
        assert_eq!(Some(3), count, "Inner join should return 3 rows");

        // Test left join
        let count: Option<i64> = Spi::get_one(
            "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
        )
        .unwrap();
        assert_eq!(
            Some(4),
            count,
            "Left join should return 4 rows including NULL match"
        );

        // Test right join
        let count: Option<i64> = Spi::get_one(
            "SELECT COUNT(*) FROM orders o RIGHT JOIN customers c ON o.customer_id = c.id",
        )
        .unwrap();
        assert_eq!(
            Some(4),
            count,
            "Right join should return 4 rows including unmatched customer"
        );

        // Test join with additional filter
        let count: Option<i64> = Spi::get_one(
            "SELECT COUNT(*) FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.name = 'Alice'"
        ).unwrap();
        assert_eq!(Some(2), count, "Join with filter should return 2 rows");
    }

    #[pg_test]
    fn join_with_indexed_column() {
        // Create tables and add an index on the join column
        Spi::run(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, category_id INTEGER) USING pgfdb_table",
        )
        .unwrap();
        Spi::run("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT) USING pgfdb_table")
            .unwrap();
        Spi::run("CREATE INDEX products_category_idx ON products USING pgfdb(category_id)")
            .unwrap();

        // Insert test data
        Spi::run("INSERT INTO categories (id, name) VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Clothing')").unwrap();
        Spi::run("INSERT INTO products (id, category_id) VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, NULL)").unwrap();

        // Disable sequential scans to force index use
        Spi::run("SET enable_seqscan=0").unwrap();

        // Verify index is used for join
        let explain = Spi::explain(
            "SELECT p.id, c.name FROM products p JOIN categories c ON p.category_id = c.id",
        )
        .unwrap();
        assert!(
            format!("{:?}", explain).contains("Index Name"),
            "expected query plan to use index: {:?}",
            explain.0.to_string()
        );

        // Test join with the index
        let count: Option<i64> = Spi::get_one(
            "SELECT COUNT(*) FROM products p JOIN categories c ON p.category_id = c.id",
        )
        .unwrap();
        assert_eq!(
            Some(5),
            count,
            "Join should return 5 rows with indexed join column"
        );

        // Test join with filter on indexed column
        let count: Option<i64> = Spi::get_one(
            "SELECT COUNT(*) FROM products p JOIN categories c ON p.category_id = c.id WHERE p.category_id = 1"
        ).unwrap();
        assert_eq!(Some(2), count, "Join with filter should return 2 rows");
    }

    #[pg_test]
    fn join_with_multiple_conditions() {
        Spi::run("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, manager_id INTEGER) USING pgfdb_table").unwrap();
        Spi::run(
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, location TEXT) USING pgfdb_table",
        )
        .unwrap();

        Spi::run("INSERT INTO departments (id, location) VALUES (1, 'New York'), (2, 'Boston'), (3, 'San Francisco')").unwrap();
        Spi::run(
            "INSERT INTO employees (id, dept_id, manager_id) VALUES
            (1, 1, NULL), (2, 1, 1), (3, 1, 1),
            (4, 2, NULL), (5, 2, 4), (6, 2, 4),
            (7, 3, NULL), (8, 3, 7), (9, NULL, NULL)",
        )
        .unwrap();

        let cases = &[
            // Multiple joins
            (
                5,
                "SELECT COUNT(*) FROM employees e
                JOIN departments d ON e.dept_id = d.id
                JOIN employees m ON e.manager_id = m.id",
            ),
            // Join with additional where clause
            (
                3,
                "SELECT COUNT(*) FROM employees e
                JOIN departments d ON e.dept_id = d.id
                WHERE d.location = 'New York'",
            ),
            // Self-join
            (
                2,
                "SELECT COUNT(*) FROM employees e1
                JOIN employees e2 ON e1.manager_id = e2.id
                WHERE e2.dept_id = 1",
            ),
        ];

        // Run test cases without indices
        for (expected, query) in cases {
            let result: Option<i64> = Spi::get_one(query).unwrap();
            assert_eq!(Some(*expected), result);
        }

        // Add indices and run test cases with them
        Spi::run("CREATE INDEX emp_dept_idx ON employees USING pgfdb(dept_id)").unwrap();
        Spi::run("CREATE INDEX emp_manager_idx ON employees USING pgfdb(manager_id)").unwrap();

        // Disable sequential scans to force index use
        Spi::run("SET enable_seqscan=0").unwrap();

        for (expected, query) in cases {
            // Verify index is used for join
            let explain = Spi::explain(query).unwrap();
            assert!(
                format!("{:?}", explain).contains("Index Name"),
                "expected query plan to use index: {:?}",
                explain.0.to_string()
            );

            let result: Option<i64> = Spi::get_one(query).unwrap();
            assert_eq!(Some(*expected), result);
        }
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
