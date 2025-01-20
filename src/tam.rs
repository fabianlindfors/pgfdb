use std::{ptr::addr_of_mut, slice::from_raw_parts};

mod scan;
mod tts;

use foundationdb::tuple::pack;
use pgrx::{
    callconv::BoxRet,
    itemptr::item_pointer_get_block_number,
    log, name_data_to_str, pg_extern, pg_guard,
    pg_sys::{
        int32, uint32, uint64, uint8, BlockNumber, BufferAccessStrategy, BulkInsertStateData,
        CommandId, Datum, ForkNumber, IndexBuildCallback, IndexFetchTableData, IndexInfo,
        ItemPointer, LockTupleMode, LockWaitPolicy, MultiXactId, ParallelTableScanDesc, ReadStream,
        RelFileLocator, Relation, SampleScanState, ScanDirection, ScanKeyData, Size, Snapshot,
        TM_FailureData, TM_IndexDeleteOp, TM_Result, TU_UpdateIndexes, TableAmRoutine,
        TableScanDesc, TableScanDescData, TransactionId, TupleTableSlot, TupleTableSlotOps,
        VacuumParams, ValidateIndexState,
    },
    PgBox,
};
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use rand::Rng;

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
unsafe extern "C" fn slot_callbacks(_rel: Relation) -> *const TupleTableSlotOps {
    log!("TAM: Using custom slot callbacks");
    &tts::CUSTOM_SLOT_OPS
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
    scan::FdbScanDesc::init(rel, snapshot, nkeys, key, pscan, flags)
}

#[pg_guard]
unsafe extern "C" fn scan_end(scan: TableScanDesc) {
    log!("TAM: Scan end");
    let mut _fscan = scan as *mut scan::FdbScanDesc;
}

#[pg_guard]
unsafe extern "C" fn rescan(
    _scan: TableScanDesc,
    _key: *mut ScanKeyData,
    _set_params: bool,
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
}

#[pg_guard]
unsafe extern "C" fn scan_get_next_slot(
    scan: TableScanDesc,
    _direction: ScanDirection::Type,
    _slot: *mut TupleTableSlot,
) -> bool {
    let mut _fscan = scan as *mut scan::FdbScanDesc;

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
    _scan: TableScanDesc,
    _mintid: ItemPointer,
    _maxtid: ItemPointer,
) {
}

#[pg_guard]
unsafe extern "C" fn scan_get_next_slot_tidrange(
    _scan: TableScanDesc,
    _direction: ScanDirection::Type,
    _slot: *mut TupleTableSlot,
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
unsafe extern "C" fn index_fetch_reset(_data: *mut IndexFetchTableData) {
    log!("TAM: Index fetch reset");
}

#[pg_guard]
unsafe extern "C" fn index_fetch_end(data: *mut IndexFetchTableData) {
    log!("TAM: Index fetch end");
    let _index_scan = data as *mut IndexScan;
}

#[pg_guard]
unsafe extern "C" fn index_fetch_tuple(
    _scan: *mut IndexFetchTableData,
    tid: ItemPointer,
    _snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
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
    slot_pointer: *mut TupleTableSlot,
    _cid: CommandId,
    _options: ::std::os::raw::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    let tuple_desc = (*rel).rd_att;
    let slot = *slot_pointer;

    log!("Inserting tuple, nvalid={}", slot.tts_nvalid);

    let values = from_raw_parts(slot.tts_values, slot.tts_nvalid.try_into().unwrap());
    log!("First value = {:?}", values[0].value());

    let txn = crate::transaction::get_transaction();

    let mut rng = rand::thread_rng();
    let id = rng.gen_range(0..=i32::MAX);

    let row_key = pack(&("tables", (*tuple_desc).tdtypeid.as_u32(), id));
    let value = pack(&(values[0].value()));

    txn.set(&row_key, &value);
}

#[pg_guard]
unsafe extern "C" fn tuple_insert_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: ::std::os::raw::c_int,
    _bistate: *mut BulkInsertStateData,
    _spec_token: uint32,
) {
}

#[pg_guard]
unsafe extern "C" fn tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _spec_token: uint32,
    _succeeded: bool,
) {
}

#[pg_guard]
unsafe extern "C" fn multi_insert(
    _rel: Relation,
    _slots: *mut *mut TupleTableSlot,
    _nslots: ::std::os::raw::c_int,
    _cid: CommandId,
    _options: ::std::os::raw::c_int,
    _bistate: *mut BulkInsertStateData,
) {
}

#[pg_guard]
unsafe extern "C" fn tuple_delete(
    _rel: Relation,
    tid: ItemPointer,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _changing_part: bool,
) -> TM_Result::Type {
    log!(
        "TAM: Delete tuple with block id = {:?}, offset = {:?}",
        (*tid).ip_blkid,
        (*tid).ip_posid
    );
    let _row_id = item_pointer_get_block_number(tid);
    // database::delete_row_by_id(rel, row_id.try_into().unwrap());
    return TM_Result::TM_Deleted;
}

#[pg_guard]
unsafe extern "C" fn tuple_update(
    _rel: Relation,
    _otid: ItemPointer,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _lockmode: *mut LockTupleMode::Type,
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
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _mode: LockTupleMode::Type,
    _wait_policy: LockWaitPolicy::Type,
    _flags: uint8,
    _tmfd: *mut TM_FailureData,
) -> TM_Result::Type {
    log!("TAM: Lock tuple");
    return TM_Result::TM_Ok;
}

#[pg_guard]
unsafe extern "C" fn fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Fetch row version");
    return false;
}

#[pg_guard]
unsafe extern "C" fn get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    log!("TAM: Get latest tid");
}

#[pg_guard]
unsafe extern "C" fn tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    log!("TAM: Tuple satisfies snapshot");
    return false;
}

#[pg_guard]
unsafe extern "C" fn index_delete_tuples(
    _rel: Relation,
    _delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    log!("TAM: Index delete tuple");
    return 0;
}

#[pg_guard]
unsafe extern "C" fn relation_nontransactional_truncate(_rel: Relation) {}

#[pg_guard]
unsafe extern "C" fn relation_copy_data(_rel: Relation, _newrlocator: *const RelFileLocator) {}

#[pg_guard]
unsafe extern "C" fn relation_copy_for_cluster(
    _new_table: Relation,
    _old_table: Relation,
    _old_index: Relation,
    _use_sort: bool,
    _oldest_xmin: TransactionId,
    _xid_cutoff: *mut TransactionId,
    _multi_cutoff: *mut MultiXactId,
    _num_tuples: *mut f64,
    _tups_vacuumed: *mut f64,
    _tups_recently_dead: *mut f64,
) {
}

#[pg_guard]
unsafe extern "C" fn relation_vacuum(
    _rel: Relation,
    _params: *mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
}

#[pg_guard]
unsafe extern "C" fn scan_analyze_next_block(
    _scan: TableScanDesc,
    _stream: *mut ReadStream,
) -> bool {
    log!("Scan analyze next block");
    return false;
}

#[pg_guard]
unsafe extern "C" fn scan_analyze_next_tuple(
    _scan: TableScanDesc,
    _oldest_xmin: TransactionId,
    _liverows: *mut f64,
    _deadrows: *mut f64,
    _slot: *mut TupleTableSlot,
) -> bool {
    log!("Scan analyze next tuple");
    return false;
}

#[pg_guard]
unsafe extern "C" fn index_build_range_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _progress: bool,
    _start_blockno: BlockNumber,
    _numblocks: BlockNumber,
    _callback: IndexBuildCallback,
    _callback_state: *mut ::std::os::raw::c_void,
    _scan: TableScanDesc,
) -> f64 {
    return 0.0;
}

#[pg_guard]
unsafe extern "C" fn index_validate_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _snapshot: Snapshot,
    _state: *mut ValidateIndexState,
) {
}

#[pg_guard]
unsafe extern "C" fn relation_needs_toast_table(_rel: Relation) -> bool {
    log!("TAM: Needs toast table");
    return false;
}

#[pg_guard]
unsafe extern "C" fn scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    log!("TAM: Scan sample next block");
    return false;
}

#[pg_guard]
unsafe extern "C" fn scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Scan sample next tuple");
    return false;
}

#[pg_guard]
unsafe extern "C" fn parallelscan_estimate(_rel: Relation) -> Size {
    return 0;
}

#[pg_guard]
unsafe extern "C" fn parallelscan_initialize(
    _rel: Relation,
    _pscann: ParallelTableScanDesc,
) -> Size {
    return 0;
}

#[pg_guard]
unsafe extern "C" fn parallelscan_reinitialize(_rel: Relation, _pscan: ParallelTableScanDesc) {}

#[pg_guard]
unsafe extern "C" fn tuple_fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
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
unsafe extern "C" fn tuple_tid_valid(_scan: TableScanDesc, _tidd: ItemPointer) -> bool {
    log!("TAM: Tuple tid valid?");
    return true;
}

#[pg_guard]
unsafe extern "C" fn tuple_get_latest_tid(_scan: TableScanDesc, _tidd: ItemPointer) {
    log!("TAM: Get latest TID");
}

#[pg_guard]
unsafe extern "C" fn relation_set_new_filelocator(
    rel: Relation,
    _newrlocator: *const RelFileLocator,
    _persistence: ::core::ffi::c_char,
    _freeze_xid: *mut TransactionId,
    _minmulti: *mut MultiXactId,
) {
    // This is called when a new table is created
    log!(
        "TAM: New table created with name {}",
        name_data_to_str(&(*(*rel).rd_rel).relname)
    );
}

#[pg_guard]
unsafe extern "C" fn relation_size(_rel: Relation, _fork_number: ForkNumber::Type) -> uint64 {
    log!("Get relation size");

    // let size = database::get_num_rows_in_table(rel);
    // size

    0
}

#[pg_guard]
unsafe extern "C" fn relation_estimate_size(
    rel: Relation,
    _attr_widths: *mut int32,
    _pages: *mut BlockNumber,
    _tuples: *mut f64,
    _allvisfrac: *mut f64,
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
