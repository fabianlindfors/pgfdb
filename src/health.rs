use crate::errors::FdbErrorExt;
use foundationdb::options::TransactionOption;
use pgrx::prelude::*;
use pollster::FutureExt;

// Expose a custom function `fdb_is_healthy` which will check that the connection to FDB is working.
// Can be run with `SELECT fdb_is_healthy()`, which will return true if healthy or an error if not.
#[pg_extern]
fn fdb_is_healthy() -> bool {
    let db = foundationdb::Database::default().unwrap_or_pg_error();

    let txn = db.create_trx().unwrap_or_pg_error();
    txn.set_option(TransactionOption::Timeout(1000))
        .unwrap_or_pg_error();

    txn.get_read_version().block_on().unwrap_or_pg_error();

    true
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn health_check() {
        let result = Spi::get_one::<bool>("SELECT fdb_is_healthy();").unwrap();
        assert!(result.unwrap(), "fdb_is_healthy() did not succeed");
    }
}
