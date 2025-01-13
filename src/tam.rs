use std::ptr::addr_of_mut;

use pgrx::{
    callconv::BoxRet,
    extension_sql,
    itemptr::item_pointer_get_block_number,
    log, name_data_to_str, pg_extern, pg_guard,
    pg_sys::{
        int32, uint32, uint64, uint8, BlockNumber, BufferAccessStrategy, BulkInsertStateData,
        CommandId, Datum, ForkNumber, IndexBuildCallback, IndexFetchTableData, IndexInfo,
        ItemPointer, LockTupleMode, LockWaitPolicy, MultiXactId, ParallelTableScanDesc, ReadStream,
        RelFileLocator, Relation, SampleScanState, ScanDirection, ScanKeyData, Size, Snapshot,
        TM_FailureData, TM_IndexDeleteOp, TM_Result, TTSOpsVirtual, TU_UpdateIndexes,
        TableAmRoutine, TableScanDesc, TableScanDescData, TransactionId, TupleTableSlot,
        TupleTableSlotOps, VacuumParams, ValidateIndexState,
    },
    PgBox,
};
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};

#[pg_extern(sql = "
    -- We need to use custom SQL to define our TAM handler function as Postgres requires the function signature
    -- to be: `(internal) -> table_am_handler`
    CREATE OR REPLACE FUNCTION pgfdb_tam_handler(internal)
    RETURNS table_am_handler AS 'MODULE_PATHNAME', $function$pgfdb_tam_handler_wrapper$function$
    LANGUAGE C STRICT;

    -- Create the corresponding table access method from the just-registered TAM handler
    CREATE ACCESS METHOD pgfdb TYPE TABLE HANDLER pgfdb_tam_handler;
    ")]
pub fn pgfdb_tam_handler() -> TableAmHandler {
    TableAmHandler
}

pub struct TableAmHandler;

unsafe impl BoxRet for TableAmHandler {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        fcinfo.return_raw_datum(Datum::from(addr_of_mut!(FDB_TABLE_AM_ROUTINE)))
    }
}

unsafe impl SqlTranslatable for TableAmHandler {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::literal("table_am_handler"))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("table_am_handler")))
    }
}

#[repr(C)]
pub struct ScanDesc {
    pub rs_base: TableScanDescData,
}

#[repr(C)]
pub struct IndexScan {
    pub base: IndexFetchTableData,
}

static mut FDB_TABLE_AM_ROUTINE: TableAmRoutine = TableAmRoutine {
    type_: pgrx::pg_sys::NodeTag::T_TableAmRoutine,

    slot_callbacks: Some(slot_callbacks),

    scan_begin: Some(scan_begin),
    scan_end: Some(scan_end),
    scan_rescan: Some(rescan),
    scan_getnextslot: Some(scan_get_next_slot),
    scan_set_tidrange: Some(scan_set_tidrange),
    scan_getnextslot_tidrange: Some(scan_get_next_slot_tidrange),

    parallelscan_estimate: Some(parallelscan_estimate),
    parallelscan_initialize: Some(parallelscan_initialize),
    parallelscan_reinitialize: Some(parallelscan_reinitialize),

    index_fetch_begin: Some(index_fetch_begin),
    index_fetch_reset: Some(index_fetch_reset),
    index_fetch_end: Some(index_fetch_end),
    index_fetch_tuple: Some(index_fetch_tuple),
    index_delete_tuples: Some(index_delete_tuples),
    index_validate_scan: Some(index_validate_scan),
    index_build_range_scan: Some(index_build_range_scan),

    tuple_fetch_row_version: Some(tuple_fetch_row_version),
    tuple_tid_valid: Some(tuple_tid_valid),
    tuple_get_latest_tid: Some(tuple_get_latest_tid),
    tuple_satisfies_snapshot: Some(tuple_satisfies_snapshot),
    tuple_insert: Some(tuple_insert),
    tuple_insert_speculative: Some(tuple_insert_speculative),
    tuple_complete_speculative: Some(tuple_complete_speculative),
    tuple_delete: Some(tuple_delete),
    tuple_update: Some(tuple_update),
    tuple_lock: Some(tuple_lock),

    multi_insert: Some(multi_insert),

    finish_bulk_insert: None,

    relation_set_new_filelocator: Some(relation_set_new_filelocator),
    relation_nontransactional_truncate: Some(relation_nontransactional_truncate),
    relation_copy_data: Some(relation_copy_data),
    relation_copy_for_cluster: Some(relation_copy_for_cluster),
    relation_vacuum: Some(relation_vacuum),

    relation_size: Some(relation_size),
    relation_estimate_size: Some(relation_estimate_size),

    // Toast
    relation_needs_toast_table: Some(relation_needs_toast_table),
    relation_toast_am: None,
    relation_fetch_toast_slice: None,

    scan_analyze_next_block: Some(scan_analyze_next_block),
    scan_analyze_next_tuple: Some(scan_analyze_next_tuple),
    scan_sample_next_block: Some(scan_sample_next_block),
    scan_sample_next_tuple: Some(scan_sample_next_tuple),

    scan_bitmap_next_block: None,
    scan_bitmap_next_tuple: None,
};

#[pg_guard]
unsafe extern "C" fn slot_callbacks(rel: Relation) -> *const TupleTableSlotOps {
    log!("TAM: Slot callbacks");
    &TTSOpsVirtual
}

#[pg_guard]
unsafe extern "C" fn scan_begin(
    rel: Relation,
    snapshot: Snapshot,
    nkeys: ::std::os::raw::c_int,
    key: *mut ScanKeyData,
    pscan: ParallelTableScanDesc,
    flags: uint32,
) -> TableScanDesc {
    log!("TAM: Scan begin with nkeys {nkeys}");
    let mut desc = unsafe { PgBox::<ScanDesc>::alloc() };

    desc.rs_base.rs_rd = rel;

    // let stream = database::prepare_scan(rel);
    // std::ptr::copy::<FdbStreamKeyValue>(&stream, &mut desc.stream, 1);
    // std::mem::forget(stream);

    // let mut benchmarker = Benchmarker::new();
    // std::ptr::copy::<Benchmarker>(&benchmarker, &mut desc.benchmarker, 1);
    // std::mem::forget(benchmarker);

    // let instant = Instant::now();
    // std::ptr::copy::<Instant>(&instant, &mut desc.instant, 1);
    // std::mem::forget(instant);

    desc.into_pg() as TableScanDesc
}

#[pg_guard]
unsafe extern "C" fn scan_end(scan: TableScanDesc) {
    log!("TAM: Scan end");
    let mut fscan = scan as *mut ScanDesc;
}

#[pg_guard]
unsafe extern "C" fn rescan(
    scan: TableScanDesc,
    key: *mut ScanKeyData,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) {
}

static mut DONE: bool = false;
static mut FALSE: bool = false;

#[pg_guard]
#[pg_guard]
unsafe extern "C" fn scan_get_next_slot(
    scan: TableScanDesc,
    direction: ScanDirection::Type,
    slot: *mut TupleTableSlot,
) -> bool {
    let mut fscan = scan as *mut ScanDesc;

    // if let Some(clear) = (*(*slot).tts_ops).clear {
    //     clear(slot);
    // }

    // let result = database::get_next_row(fscan, slot);
    // if result.is_none() {
    //     return false;
    // }

    // ExecStoreVirtualTuple(slot);

    return true;
}

#[pg_guard]
unsafe extern "C" fn scan_set_tidrange(
    scan: TableScanDesc,
    mintid: ItemPointer,
    maxtid: ItemPointer,
) {
}

#[pg_guard]
unsafe extern "C" fn scan_get_next_slot_tidrange(
    scan: TableScanDesc,
    direction: ScanDirection::Type,
    slot: *mut TupleTableSlot,
) -> bool {
    return false;
}

#[pg_guard]
unsafe extern "C" fn index_fetch_begin(rel: Relation) -> *mut IndexFetchTableData {
    log!("TAM: Index fetch begin");
    let mut index_scan = unsafe { PgBox::<IndexScan>::alloc() };

    index_scan.base.rel = rel;

    index_scan.into_pg() as *mut IndexFetchTableData
}

#[pg_guard]
unsafe extern "C" fn index_fetch_reset(data: *mut IndexFetchTableData) {
    log!("TAM: Index fetch reset");
}

#[pg_guard]
unsafe extern "C" fn index_fetch_end(data: *mut IndexFetchTableData) {
    log!("TAM: Index fetch end");
    let index_scan = data as *mut IndexScan;
}

#[pg_guard]
unsafe extern "C" fn index_fetch_tuple(
    scan: *mut IndexFetchTableData,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    call_again: *mut bool,
    all_dead: *mut bool,
) -> bool {
    log!(
        "TAM: Fetch tuple, id = {:?}, offset = {:?}",
        (*tid).ip_blkid,
        (*tid).ip_posid
    );
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }

    // database::get_row_by_id(
    //     (*scan).rel,
    //     item_pointer_get_block_number(tid).try_into().unwrap(),
    //     slot,
    // );

    // ExecStoreVirtualTuple(slot);

    return true;
}

#[pg_guard]
unsafe extern "C" fn tuple_insert(
    rel: Relation,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    options: ::std::os::raw::c_int,
    bistate: *mut BulkInsertStateData,
) {
    let tuple_desc = (*rel).rd_att;

    // let row = Row::from_pg(rel, slot);
    // database::insert_row(&row);

    // let id: BlockNumber = row.id.try_into().unwrap();
    // item_pointer_set_all(&mut (*slot).tts_tid, id, 1);
}

#[pg_guard]
unsafe extern "C" fn tuple_insert_speculative(
    rel: Relation,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    options: ::std::os::raw::c_int,
    bistate: *mut BulkInsertStateData,
    specToken: uint32,
) {
}

#[pg_guard]
unsafe extern "C" fn tuple_complete_speculative(
    rel: Relation,
    slot: *mut TupleTableSlot,
    specToken: uint32,
    succeeded: bool,
) {
}

#[pg_guard]
unsafe extern "C" fn multi_insert(
    rel: Relation,
    slots: *mut *mut TupleTableSlot,
    nslots: ::std::os::raw::c_int,
    cid: CommandId,
    options: ::std::os::raw::c_int,
    bistate: *mut BulkInsertStateData,
) {
}

#[pg_guard]
unsafe extern "C" fn tuple_delete(
    rel: Relation,
    tid: ItemPointer,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    changingPart: bool,
) -> TM_Result::Type {
    log!(
        "TAM: Delete tuple with block id = {:?}, offset = {:?}",
        (*tid).ip_blkid,
        (*tid).ip_posid
    );
    let row_id = item_pointer_get_block_number(tid);
    // database::delete_row_by_id(rel, row_id.try_into().unwrap());
    return TM_Result::TM_Deleted;
}

#[pg_guard]
unsafe extern "C" fn tuple_update(
    rel: Relation,
    otid: ItemPointer,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    lockmode: *mut LockTupleMode::Type,
    update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    log!("TAM: Update tuple");

    // let row = Row::from_pg_with_id(rel, slot, otid);
    // database::insert_row(&row);

    // Write back TID to slot (same as previous one)
    // row.into_tuple_slot(slot);

    *update_indexes = TU_UpdateIndexes::TU_All;

    return 0;
}

#[pg_guard]
unsafe extern "C" fn tuple_lock(
    rel: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    mode: LockTupleMode::Type,
    wait_policy: LockWaitPolicy::Type,
    flags: uint8,
    tmfd: *mut TM_FailureData,
) -> TM_Result::Type {
    log!("TAM: Lock tuple");
    return TM_Result::TM_Ok;
}

#[pg_guard]
unsafe extern "C" fn fetch_row_version(
    rel: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Fetch row version");
    return false;
}

#[pg_guard]
unsafe extern "C" fn get_latest_tid(scan: TableScanDesc, tid: ItemPointer) {
    log!("TAM: Get latest tid");
}

#[pg_guard]
unsafe extern "C" fn tuple_satisfies_snapshot(
    rel: Relation,
    slot: *mut TupleTableSlot,
    snapshot: Snapshot,
) -> bool {
    log!("TAM: Tuple satisfies snapshot");
    return false;
}

#[pg_guard]
unsafe extern "C" fn index_delete_tuples(
    rel: Relation,
    delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    log!("TAM: Index delete tuple");
    return 0;
}

#[pg_guard]
unsafe extern "C" fn relation_nontransactional_truncate(rel: Relation) {}

#[pg_guard]
unsafe extern "C" fn relation_copy_data(rel: Relation, newrlocator: *const RelFileLocator) {}

#[pg_guard]
unsafe extern "C" fn relation_copy_for_cluster(
    NewTable: Relation,
    OldTable: Relation,
    OldIndex: Relation,
    use_sort: bool,
    OldestXmin: TransactionId,
    xid_cutoff: *mut TransactionId,
    multi_cutoff: *mut MultiXactId,
    num_tuples: *mut f64,
    tups_vacuumed: *mut f64,
    tups_recently_dead: *mut f64,
) {
}

#[pg_guard]
unsafe extern "C" fn relation_vacuum(
    rel: Relation,
    params: *mut VacuumParams,
    bstrategy: BufferAccessStrategy,
) {
}

#[pg_guard]
unsafe extern "C" fn scan_analyze_next_block(scan: TableScanDesc, stream: *mut ReadStream) -> bool {
    log!("Scan analyze next block");
    return false;
}

#[pg_guard]
unsafe extern "C" fn scan_analyze_next_tuple(
    scan: TableScanDesc,
    OldestXmin: TransactionId,
    liverows: *mut f64,
    deadrows: *mut f64,
    slot: *mut TupleTableSlot,
) -> bool {
    log!("Scan analyze next tuple");
    return false;
}

#[pg_guard]
unsafe extern "C" fn index_build_range_scan(
    table_rel: Relation,
    index_rel: Relation,
    index_info: *mut IndexInfo,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: BlockNumber,
    numblocks: BlockNumber,
    callback: IndexBuildCallback,
    callback_state: *mut ::std::os::raw::c_void,
    scan: TableScanDesc,
) -> f64 {
    return 0.0;
}

#[pg_guard]
unsafe extern "C" fn index_validate_scan(
    table_rel: Relation,
    index_rel: Relation,
    index_info: *mut IndexInfo,
    snapshot: Snapshot,
    state: *mut ValidateIndexState,
) {
}

#[pg_guard]
unsafe extern "C" fn relation_needs_toast_table(rel: Relation) -> bool {
    log!("TAM: Needs toast table");
    return false;
}

#[pg_guard]
unsafe extern "C" fn scan_sample_next_block(
    scan: TableScanDesc,
    scanstate: *mut SampleScanState,
) -> bool {
    log!("TAM: Scan sample next block");
    return false;
}

#[pg_guard]
unsafe extern "C" fn scan_sample_next_tuple(
    scan: TableScanDesc,
    scanstate: *mut SampleScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Scan sample next tuple");
    return false;
}

#[pg_guard]
unsafe extern "C" fn parallelscan_estimate(rel: Relation) -> Size {
    return 0;
}

#[pg_guard]
unsafe extern "C" fn parallelscan_initialize(rel: Relation, pscan: ParallelTableScanDesc) -> Size {
    return 0;
}

#[pg_guard]
unsafe extern "C" fn parallelscan_reinitialize(rel: Relation, pscan: ParallelTableScanDesc) {}

#[pg_guard]
unsafe extern "C" fn tuple_fetch_row_version(
    rel: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Tuple fetch row version");

    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }

    // database::get_row_by_id(
    //     rel,
    //     item_pointer_get_block_number(tid).try_into().unwrap(),
    //     slot,
    // )
    // .unwrap();

    // ExecStoreVirtualTuple(slot);

    return true;
}

#[pg_guard]
unsafe extern "C" fn tuple_tid_valid(scan: TableScanDesc, tid: ItemPointer) -> bool {
    log!("TAM: Tuple tid valid?");
    return true;
}

#[pg_guard]
unsafe extern "C" fn tuple_get_latest_tid(scan: TableScanDesc, tid: ItemPointer) {
    log!("TAM: Get latest TID");
}

#[pg_guard]
unsafe extern "C" fn relation_set_new_filelocator(
    rel: Relation,
    newrlocator: *const RelFileLocator,
    persistence: ::core::ffi::c_char,
    freezeXid: *mut TransactionId,
    minmulti: *mut MultiXactId,
) {
    // This is called when a new table is created
    log!(
        "TAM: New table created with name {}",
        name_data_to_str(&(*(*rel).rd_rel).relname)
    );
}

#[pg_guard]
unsafe extern "C" fn relation_size(rel: Relation, forkNumber: ForkNumber::Type) -> uint64 {
    log!("Get relation size");

    // let size = database::get_num_rows_in_table(rel);
    // size

    0
}

#[pg_guard]
unsafe extern "C" fn relation_estimate_size(
    rel: Relation,
    attr_widths: *mut int32,
    pages: *mut BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) {
    log!(
        "Estimate relation size, previous estimate {}",
        (*(*rel).rd_rel).reltuples
    );

    // let data_width = get_relation_data_width((*rel).rd_id, attr_widths) as i64;
    // let estimated_size = database::get_size_estimate_for_table(rel);
    // log!(
    //     "Data width {}, estimated size {}",
    //     data_width,
    //     estimated_size
    // );

    // *pages = 1;
    // *tuples = (estimated_size / data_width) as f64;
    // *allvisfrac = 1.0;
}
