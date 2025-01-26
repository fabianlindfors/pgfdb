use std::sync::OnceLock;

use foundationdb::api::NetworkAutoStop;
use pgrx::prelude::*;

static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();

#[pg_guard]
pub(crate) fn init() {
    let network = unsafe { foundationdb::boot() };
    let _ = NETWORK.set(network);

    eprintln!("Setting up FDB");
}
