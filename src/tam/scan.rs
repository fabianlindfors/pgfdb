use foundationdb::{FdbResult, RangeOption};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use pgrx::{
    log,
    pg_sys::{
        uint32, Oid, ParallelTableScanDesc, Relation, ScanKeyData, Snapshot, TableScanDesc,
        TableScanDescData,
    },
    PgBox,
};
use pollster::FutureExt as PollsterFutureExt;

use crate::errors::FdbErrorExt;

#[repr(C)]
pub struct FdbScanDesc {
    base: TableScanDescData,
    values: BoxStream<'static, FdbResult<crate::coding::Tuple>>,
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
        log!(
            "SCAN: Initalizing scan for table oid={}",
            table_oid.to_u32()
        );

        let stream = Self::create_stream(table_oid);

        // We can't assign with `scan.values = ...` because `scan.values` is unitialized
        // Rust would attempt to drop the existing, nonsense value leading to UB and a crash.
        // We must take care to free `stream` later to not leak the FDB future it references.
        unsafe {
            let scan_pointer = scan.as_ptr();
            std::ptr::write(&mut (*scan_pointer).values, stream);
        }

        scan.into_pg() as *mut TableScanDescData
    }

    // Helper function to create a stream for the given table subspace
    fn create_stream(table_oid: Oid) -> BoxStream<'static, FdbResult<crate::coding::Tuple>> {
        let table_subspace = crate::subspace::table(table_oid);
        let txn = crate::transaction::get_transaction();
        let range_option = RangeOption::from(table_subspace.range());

        txn.get_ranges(range_option, false)
            .map_ok(|values| {
                futures::stream::iter(
                    values
                        .into_iter()
                        .map(|keyvalue| Ok(crate::coding::Tuple::deserialize(keyvalue.value()))),
                )
            })
            .try_flatten()
            .fuse()
            .boxed()
    }

    // Reinitialize the scan with a new stream
    pub fn reinit(&mut self, key: *mut ScanKeyData) {
        // Update the scan key if provided
        if !key.is_null() {
            self.base.rs_key = key;
        }

        let table_oid = unsafe { (*self.base.rs_rd).rd_id };
        log!(
            "SCAN: Reinitializing scan for table oid={}",
            table_oid.to_u32()
        );

        // Create a new stream
        let new_stream = Self::create_stream(table_oid);

        // Replace the existing stream
        // We need to be careful to drop the old stream to avoid memory leaks
        // Take ownership of the old stream so it gets dropped
        let old_stream = std::mem::replace(&mut self.values, new_stream);
        // Explicitly drop the old stream
        std::mem::drop(old_stream);
    }

    pub fn next_value(self: &mut FdbScanDesc) -> Option<crate::coding::Tuple> {
        let tuple = self.values.next().block_on()?;
        Some(tuple.unwrap_or_pg_error())
    }
}
