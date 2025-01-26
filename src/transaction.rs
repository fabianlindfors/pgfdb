use std::sync::OnceLock;

use foundationdb::Transaction;
use pg_sys::XactEvent;
use pgrx::prelude::*;
use pollster::FutureExt;

static mut TRANSACTION: OnceLock<Transaction> = OnceLock::new();

#[pg_guard]
pub unsafe extern "C" fn transaction_callback(event: u32, _arg: *mut ::std::os::raw::c_void) {
    match event {
        XactEvent::XACT_EVENT_COMMIT => commit_transaction(),
        XactEvent::XACT_EVENT_ABORT => abort_transaction(),
        _ => (),
    }
    log!("TXN: Tranasction callback for event {}", event);
}

pub fn get_transaction() -> &'static Transaction {
    unsafe {
        TRANSACTION.get_or_init(|| {
            let db = foundationdb::Database::default().unwrap();
            let transaction = db.create_trx().unwrap();
            log!("TXN: Transaction initiated");
            transaction
        })
    }
}

fn commit_transaction() {
    if let Some(txn) = unsafe { TRANSACTION.take() } {
        let result = txn.commit().block_on().unwrap();
        log!(
            "TXN: Transaction committed, version={}",
            result.committed_version().unwrap()
        );
    }
}

fn abort_transaction() {
    unsafe { TRANSACTION.take() };
    log!("TXN: Transaction aborted");
}
