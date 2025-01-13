use pgrx::prelude::*;
use pollster::FutureExt;

// Expose a custom function `fdb_is_healthy` which will check that the connection to FDB is working.
// Can be run with `SELECT fdb_is_healthy()`, which will return true if healthy or an error if not.
#[pg_extern]
fn fdb_is_healthy() -> bool {
    let result = foundationdb::Database::default();

    let db = match result {
        Ok(db) => db,
        Err(err) => panic!("Failed to connect to FDB: {:?}", err),
    };

    let result = db
        .run(|trx, _| async move {
            trx.set(b"hello", b"Hello, pgfdb!");
            Ok(())
        })
        .block_on();

    if let Err(err) = result {
        panic!("Failed to write to FDB: {:?}", err);
    }

    let result = db
        .run(|trx, _| async move { Ok(trx.get(b"hello", false).await.unwrap()) })
        .block_on();

    if let Err(err) = result {
        panic!("Failed to read from FDB: {:?}", err);
    }

    return true;
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
