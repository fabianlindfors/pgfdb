use std::sync::OnceLock;

use foundationdb::{options::TransactionOption, Transaction};
use pg_sys::XactEvent;
use pgrx::{pg_sys::panic::ErrorReportable, prelude::*};
use pollster::FutureExt;

// Not sure how well this will work with multiple connections at the same time
// Perhaps thread_local! can be used instead, although lifetimes are more painful using that
static mut TRANSACTION: OnceLock<Transaction> = OnceLock::new();

#[pg_guard]
pub unsafe extern "C-unwind" fn transaction_callback(
    event: u32,
    _arg: *mut ::std::os::raw::c_void,
) {
    match event {
        XactEvent::XACT_EVENT_COMMIT => commit_transaction(),
        XactEvent::XACT_EVENT_ABORT => abort_transaction(),
        _ => (),
    }
    log!("TXN: Tranasction callback for event {}", event);
}

pub fn get_transaction() -> &'static Transaction {
    // Static mut reference is highly discouraged but haven't found a better way yet
    #[allow(static_mut_refs)]
    unsafe {
        TRANSACTION.get_or_init(|| {
            let db = foundationdb::Database::default().unwrap_or_report();
            let transaction = db.create_trx().unwrap_or_report();

            transaction
                .set_option(TransactionOption::Timeout(5_000))
                .unwrap_or_report();

            log!("TXN: Transaction initiated");
            transaction
        })
    }
}

fn commit_transaction() {
    #[allow(static_mut_refs)]
    if let Some(txn) = unsafe { TRANSACTION.take() } {
        let result = txn.commit().block_on().unwrap();
        log!(
            "TXN: Transaction committed, version={}",
            result.committed_version().unwrap()
        );
    }
}

fn abort_transaction() {
    #[allow(static_mut_refs)]
    unsafe {
        TRANSACTION.take()
    };
    log!("TXN: Transaction aborted");
}
