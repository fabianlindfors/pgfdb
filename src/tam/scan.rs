use pgrx::pg_sys::{
    TableScanDescData, Relation, Snapshot, ScanKeyData, ParallelTableScanDesc, uint32,
};

#[repr(C)]
pub struct FdbScanDesc {
    pub rs_base: TableScanDescData,
}

impl FdbScanDesc {
    pub fn init(
        &mut self,
        rel: Relation,
        snapshot: Snapshot,
        nkeys: ::std::os::raw::c_int,
        key: *mut ScanKeyData,
        pscan: ParallelTableScanDesc,
        flags: uint32,
    ) {
        self.rs_base.rs_rd = rel;
        self.rs_base.rs_snapshot = snapshot;
        self.rs_base.rs_nkeys = nkeys;
        self.rs_base.rs_key = key;
        self.rs_base.rs_parallel = pscan;
        self.rs_base.rs_flags = flags;
    }
}
