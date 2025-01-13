use std::env;

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

mod database;
mod health;
mod tam;

#[pg_guard]
pub extern "C" fn _PG_init() {
    database::init_database();

    env::set_var("RUST_BACKTRACE", "1");
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn create_table() {
        Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();
    }

    // #[pg_test]
    // fn insert() {
    //     Spi::run("CREATE TABLE test (id INTEGER) USING pgfdb").unwrap();

    //     Spi::run("INSERT INTO test (id) VALUES (10)").unwrap();
    //     let result = Spi::get_one::<i64>("SELECT id FROM test LIMIT 1").unwrap();
    //     assert_eq!(Some(10), result);
    // }
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
