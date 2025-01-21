use foundationdb::{future::FdbValue, FdbResult, RangeOption};
use futures::{
    stream::{empty, BoxStream},
    Stream, StreamExt,
};
use pgrx::{
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

        let txn = crate::transaction::get_transaction();
        let range_option = RangeOption::from(table_subspace.range());
        let stream = txn.get_ranges_keyvalues(range_option, false).boxed();

        // We can't just assign with `scan.values = ...` because `scan.values is uninitialised and Rust will try to drop
        // the existing non-sense value, causing undefined behaviour
        unsafe {
            let scan_pointer = scan.as_ptr();
            (*scan_pointer).values = stream;
        }

        scan.into_pg() as *mut TableScanDescData
    }
}
