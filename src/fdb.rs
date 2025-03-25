use std::sync::OnceLock;

use foundationdb::api::NetworkAutoStop;
use pgrx::{pg_sys::panic::ErrorReportable, prelude::*};
use pollster::FutureExt;

use crate::errors::FdbErrorExt;

static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();

#[pg_guard]
pub(crate) fn init() {
    let network = unsafe { foundationdb::boot() };

    // Ensure the network thread was booted and is working
    let db = foundationdb::Database::default().unwrap_or_pg_error();
    db.perform_no_op().block_on().unwrap_or_report();

    let _ = NETWORK.set(network);

    eprintln!("FDB network thread started");
}
