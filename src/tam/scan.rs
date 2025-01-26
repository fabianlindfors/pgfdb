use std::task::{Context, Poll, Waker};

use foundationdb::{future::FdbValue, FdbResult, RangeOption};
use futures::{stream::BoxStream, FutureExt, StreamExt};
use pgrx::{
    log,
    pg_sys::{
        uint32, ParallelTableScanDesc, Relation, ScanKeyData, Snapshot, TableScanDesc,
        TableScanDescData,
    },
    PgBox,
};

#[repr(C)]
pub struct FdbScanDesc {
    base: TableScanDescData,
    values: BoxStream<'static, FdbResult<FdbValue>>,
}

impl FdbScanDesc {
    pub fn init(
        rel: Relation,
        snapshot: Snapshot,
        nkeys: ::std::os::raw::c_int,
        key: *mut ScanKeyData,
        pscan: ParallelTableScanDesc,
        flags: uint32,
    ) -> TableScanDesc {
        let mut scan = unsafe { PgBox::<FdbScanDesc>::alloc() };

        scan.base.rs_rd = rel;
        scan.base.rs_snapshot = snapshot;
        scan.base.rs_nkeys = nkeys;
        scan.base.rs_key = key;
        scan.base.rs_parallel = pscan;
        scan.base.rs_flags = flags;

        let table_oid = unsafe { (*rel).rd_id };
        let table_subspace = crate::subspace::table(table_oid);
        log!(
            "SCAN: Initalizing scan for table oid={}",
            table_oid.as_u32()
        );

        let txn = crate::transaction::get_transaction();
        let range_option = RangeOption::from(table_subspace.range());

        let stream = txn.get_ranges_keyvalues(range_option, false).boxed();

        // We can't assign with `scan.values = ...` because `scan.values` is unitialized
        // Rust would attempt to drop the existing, nonsense value leading to UB and a crash.
        // We must take care to free `stream` later to not leak the FDB future it references.
        unsafe {
            let scan_pointer = scan.as_ptr();
            std::ptr::write(&mut (*scan_pointer).values, stream);
        }

        scan.into_pg() as *mut TableScanDescData
    }

    pub fn next_value(self: &mut FdbScanDesc) -> Option<FdbValue> {
        let mut next_fut = self.values.next();
        let mut ctx = Context::from_waker(&Waker::noop());
        let next = loop {
            match next_fut.poll_unpin(&mut ctx) {
                Poll::Ready(result) => {
                    break result;
                }
                Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(1)),
            }
        };

        // let next = self.values.next().block_on();
        let Some(value) = next else {
            return None;
        };

        Some(value.unwrap())
    }
}
