use pgrx::pg_sys::TableScanDescData;

#[repr(C)]
pub struct FdbScanDesc {
    pub rs_base: TableScanDescData,
}
