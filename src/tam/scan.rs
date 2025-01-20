use pgrx::pg_sys::{
    TableScanDescData, Relation, Snapshot, ScanKeyData, ParallelTableScanDesc, uint32,
};

#[repr(C)]
pub struct FdbScanDesc {
    pub rs_base: TableScanDescData,
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
        let mut scan = PgBox::<FdbScanDesc>::alloc();
        
        scan.rs_base.rs_rd = rel;
        scan.rs_base.rs_snapshot = snapshot;
        scan.rs_base.rs_nkeys = nkeys;
        scan.rs_base.rs_key = key;
        scan.rs_base.rs_parallel = pscan;
        scan.rs_base.rs_flags = flags;

        scan.into_pg() as *mut TableScanDescData
    }
}
