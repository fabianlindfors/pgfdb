use pgrx::prelude::*;

#[pg_test]
fn health_check() {
    let result = Spi::get_one::<bool>("SELECT fdb_is_healthy();").unwrap();
    assert!(result.unwrap(), "fdb_is_healthy() did not succeed");
}
