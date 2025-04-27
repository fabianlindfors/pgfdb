use std::ptr::addr_of_mut;

mod scan;

use pgrx::{
    callconv::BoxRet,
    itemptr::{item_pointer_get_block_number_no_check, item_pointer_set_all},
    list::List,
    log,
    memcx::current_context,
    name_data_to_str, pg_extern, pg_guard,
    pg_sys::{
        int32, uint32, uint64, uint8, BlockNumber, BufferAccessStrategy, BulkInsertStateData,
        CommandId, Datum, ForkNumber, IndexBuildCallback, IndexFetchTableData, IndexInfo,
        ItemPointer, LockTupleMode, LockWaitPolicy, MultiXactId, Oid, ParallelTableScanDesc,
        ReadStream, RelFileLocator, Relation, RelationClose, RelationIdGetRelation,
        SampleScanState, ScanDirection, ScanKeyData, Size, Snapshot, TM_FailureData,
        TM_IndexDeleteOp, TM_Result, TTSOpsVirtual, TU_UpdateIndexes, TableAmRoutine,
        TableScanDesc, TransactionId, TupleTableSlot, TupleTableSlotOps, VacuumParams,
        ValidateIndexState,
    },
    PgBox,
};
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use pollster::FutureExt;
use rand::Rng;

use crate::{errors::FdbErrorExt, subspace};

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
pub struct FdbIndexFetchTableData {
    pub base: IndexFetchTableData,
    // Add any custom fields we need for index operations
    pub current_id: u32,
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
unsafe extern "C-unwind" fn slot_callbacks(_rel: Relation) -> *const TupleTableSlotOps {
    log!("TAM: Using custom slot callbacks");
    &TTSOpsVirtual
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_begin(
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
unsafe extern "C-unwind" fn scan_end(scan: TableScanDesc) {
    log!("TAM: Scan end");
    let mut _fscan = scan as *mut scan::FdbScanDesc;

    crate::tuple_cache::clear();
}

#[pg_guard]
unsafe extern "C-unwind" fn rescan(
    _scan: TableScanDesc,
    _key: *mut ScanKeyData,
    _set_params: bool,
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_get_next_slot(
    raw_scan: TableScanDesc,
    _direction: ScanDirection::Type,
    slot: *mut TupleTableSlot,
) -> bool {
    // log!("TAM: Scan get next slot, slot={:p}", slot);
    let scan = (raw_scan as *mut scan::FdbScanDesc).as_mut().unwrap();

    // Load next value from the ongoing scan
    let Some(tuple) = scan.next_value() else {
        // No value means there are no more tuples in the scan
        return false;
    };

    // Store the decoded values on the TTS
    tuple.load_into_tts(slot.as_mut().unwrap());

    return true;
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_set_tidrange(
    _scan: TableScanDesc,
    _mintid: ItemPointer,
    _maxtid: ItemPointer,
) {
    log!("TAM: Scan set TID range");
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_get_next_slot_tidrange(
    _scan: TableScanDesc,
    _direction: ScanDirection::Type,
    _slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Scan get next slot TID range");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_begin(rel: Relation) -> *mut IndexFetchTableData {
    log!("TAM: Index fetch begin");
    let mut index_scan = unsafe { PgBox::<FdbIndexFetchTableData>::alloc() };

    index_scan.base.rel = rel;
    index_scan.current_id = 0; // Initialize our custom field

    index_scan.into_pg() as *mut IndexFetchTableData
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_reset(_data: *mut IndexFetchTableData) {
    log!("TAM: Index fetch reset");
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_end(data: *mut IndexFetchTableData) {
    log!("TAM: Index fetch end");
    let _index_scan = data as *mut FdbIndexFetchTableData;
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_tuple(
    scan: *mut IndexFetchTableData,
    tid: ItemPointer,
    _snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
) -> bool {
    let fdb_scan = scan as *mut FdbIndexFetchTableData;
    log!("TAM: Fetch tuple, id = {:?}", (*tid).ip_blkid);

    let id = item_pointer_get_block_number_no_check(*tid);
    // Store the current ID in our custom field for potential future use
    (*fdb_scan).current_id = id;

    let key = subspace::table((*(*scan).rel).rd_id).pack(&id);

    let txn = crate::transaction::get_transaction();
    let Some(value) = txn.get(&key, false).block_on().unwrap_or_pg_error() else {
        return false;
    };

    // Decode the value into our intermediate data structure
    let tuple = crate::coding::Tuple::deserialize(&value);

    // Store the decoded values on the TTS
    tuple.load_into_tts(slot.as_mut().unwrap());

    return true;
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_insert(
    rel: Relation,
    slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: ::std::os::raw::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    // let slot = slot_pointer as *mut tts::FdbTupleTableSlot;

    // Generate random ID which we will use as a Postgres item pointer
    // An item pointers stores an unsigned 48 integer (32-bit block number and 16-bit offset)
    // We randomly generate a block number and always set the offset to 1
    let mut rng = rand::rng();
    let id = rng.random_range(0..=u32::MAX);

    // Store the random ID as an item pointer to the slot
    // Offset can't be 0 so we set that to 1
    item_pointer_set_all(&mut (*slot).tts_tid, id, 1);

    // log!(
    //     "Inserting tuple in table={} with id={}",
    //     (*rel).rd_id.as_u32(),
    //     id
    // );

    let tuple = crate::coding::Tuple::from_tts(id, slot.as_ref().unwrap());
    let encoded = tuple.serialize();

    let key = subspace::table((*rel).rd_id).pack(&id);
    let txn = crate::transaction::get_transaction();
    txn.set(&key, &encoded);
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_insert_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: ::std::os::raw::c_int,
    _bistate: *mut BulkInsertStateData,
    _spec_token: uint32,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _spec_token: uint32,
    _succeeded: bool,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn multi_insert(
    _rel: Relation,
    _slots: *mut *mut TupleTableSlot,
    _nslots: ::std::os::raw::c_int,
    _cid: CommandId,
    _options: ::std::os::raw::c_int,
    _bistate: *mut BulkInsertStateData,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_delete(
    rel: Relation,
    tid: ItemPointer,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _changing_part: bool,
) -> TM_Result::Type {
    let id = item_pointer_get_block_number_no_check(*tid);

    log!(
        "TAM: Delete tuple for relation = {:?} with id = {}",
        (*rel).rd_id,
        id,
    );

    // First, fetch the tuple that's being deleted so we can get its values for index deletion
    let key = subspace::table((*rel).rd_id).pack(&id);
    let txn = crate::transaction::get_transaction();

    // Get the tuple data before deleting it
    if let Some(value) = txn.get(&key, false).block_on().unwrap_or_pg_error() {
        // Decode the tuple
        let mut tuple = crate::coding::Tuple::deserialize(&value);

        // Get all indexes on this relation
        current_context(|ctx| {
            let index_oids: List<Oid> =
                List::downcast_ptr_in_memcx((*rel).rd_indexlist, ctx).unwrap();

            for index_oid in index_oids.iter() {
                let index_rel = RelationIdGetRelation(*index_oid);

                if !index_rel.is_null() {
                    // Get index attributes
                    let index_tuple_desc = (*index_rel).rd_att;
                    let natts = (*index_tuple_desc).natts as usize;
                    let attrs = (*index_tuple_desc).attrs.as_slice(natts);

                    // Extract values from the tuple for the index
                    // We need to map table columns to index columns
                    let mut values: Vec<Datum> = Vec::with_capacity(natts);
                    let mut isnull: Vec<bool> = Vec::with_capacity(natts);

                    // For each index attribute, find the corresponding table column
                    for i in 0..natts {
                        let attnum = attrs[i].attnum;
                        // attnum is 1-based, so we need to subtract 1 to get 0-based index
                        let table_col_idx = attnum as usize;

                        // Make sure the column index is valid
                        if table_col_idx < tuple.datums.len() {
                            if let Some(encoded_datum) = &mut tuple.datums[table_col_idx] {
                                let datum =
                                    crate::coding::decode_datum(encoded_datum, attrs[i].atttypid);
                                values.push(datum);
                                isnull.push(false);
                            } else {
                                values.push(Datum::null());
                                isnull.push(true);
                            }
                        } else {
                            // This shouldn't happen, but handle it gracefully
                            values.push(Datum::null());
                            isnull.push(true);
                        }
                    }

                    // Build and clear the index key
                    let key = crate::iam::build::build_key_from_values(
                        *index_oid, id, natts, attrs, &values, &isnull,
                    );
                    txn.clear(&key);

                    // Release the index relation
                    RelationClose(index_rel);
                }
            }
        });
    }

    // Now delete the tuple itself
    txn.clear(&key);

    // For some reason this is not counting correctly how many tuples have been removed
    // The response after running a delete is always "DELETE 0"
    return TM_Result::TM_Deleted;
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_update(
    rel: Relation,
    otid: ItemPointer,
    slot: *mut TupleTableSlot,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _lockmode: *mut LockTupleMode::Type,
    update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    log!("TAM: Update tuple");

    let id = item_pointer_get_block_number_no_check(*otid);
    let tuple = crate::coding::Tuple::from_tts(id, slot.as_ref().unwrap());
    let encoded = tuple.serialize();

    let key = subspace::table((*rel).rd_id).pack(&id);
    let txn = crate::transaction::get_transaction();
    txn.set(&key, &encoded);

    // Store back the old TID as the new one as we don't handle visibility checks and don't need new IDs
    (*slot).tts_tid = *otid;

    *update_indexes = TU_UpdateIndexes::TU_All;

    return TM_Result::TM_Ok;
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_lock(
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
unsafe extern "C-unwind" fn fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Fetch row version");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    log!("TAM: Get latest tid");
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    log!("TAM: Tuple satisfies snapshot");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn index_delete_tuples(
    _rel: Relation,
    _delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    log!("TAM: Index delete tuple");
    return TransactionId::from_inner(0);
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_nontransactional_truncate(_rel: Relation) {}

#[pg_guard]
unsafe extern "C-unwind" fn relation_copy_data(
    _rel: Relation,
    _newrlocator: *const RelFileLocator,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_copy_for_cluster(
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
unsafe extern "C-unwind" fn relation_vacuum(
    _rel: Relation,
    _params: *mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_analyze_next_block(
    _scan: TableScanDesc,
    _stream: *mut ReadStream,
) -> bool {
    log!("Scan analyze next block");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_analyze_next_tuple(
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
unsafe extern "C-unwind" fn index_build_range_scan(
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
unsafe extern "C-unwind" fn index_validate_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _snapshot: Snapshot,
    _state: *mut ValidateIndexState,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_needs_toast_table(_rel: Relation) -> bool {
    log!("TAM: Needs toast table");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    log!("TAM: Scan sample next block");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Scan sample next tuple");
    return false;
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_estimate(_rel: Relation) -> Size {
    return 0;
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_initialize(
    _rel: Relation,
    _pscann: ParallelTableScanDesc,
) -> Size {
    return 0;
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_reinitialize(
    _rel: Relation,
    _pscan: ParallelTableScanDesc,
) {
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_fetch_row_version(
    rel: Relation,
    tid: ItemPointer,
    _snapshot: Snapshot,
    slot: *mut TupleTableSlot,
) -> bool {
    log!("TAM: Tuple fetch row version");

    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }

    let id = item_pointer_get_block_number_no_check(*tid);
    let table_oid = unsafe { (*rel).rd_id };
    let key = subspace::table(table_oid).pack(&id);

    // TODO: This can probably be optimized if we already fetched the tuple in the previous plan node
    // This would for example be the case if doing an UPDATE
    let txn = crate::transaction::get_transaction();
    let Some(data) = txn.get(&key, false).block_on().unwrap_or_pg_error() else {
        return false;
    };

    let tuple = crate::coding::Tuple::deserialize(&data);
    tuple.load_into_tts(slot.as_mut().unwrap());

    crate::tuple_cache::populate(id, slot);

    return true;
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_tid_valid(_scan: TableScanDesc, _tidd: ItemPointer) -> bool {
    log!("TAM: Tuple tid valid?");
    return true;
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_get_latest_tid(_scan: TableScanDesc, _tidd: ItemPointer) {
    log!("TAM: Get latest TID");
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_set_new_filelocator(
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
unsafe extern "C-unwind" fn relation_size(
    _rel: Relation,
    _fork_number: ForkNumber::Type,
) -> uint64 {
    log!("Get relation size");

    // let size = database::get_num_rows_in_table(rel);
    // size

    0
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_estimate_size(
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
