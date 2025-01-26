use std::ptr::addr_of_mut;

use pg_sys::{
    Datum, IndexAmRoutine, IndexBuildResult, IndexInfo, IndexScanDesc, IndexUniqueCheck,
    InvalidOid, ItemPointer, Relation, ScanDirection, ScanKey,
};
use pgrx::callconv::BoxRet;
use pgrx::prelude::*;
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};

#[pg_extern(sql = "
    -- We need to use custom SQL to define our IAM handler function as Postgres requires the function signature
    -- to be: `(internal) -> index_am_handler`
    CREATE OR REPLACE FUNCTION pgfdb_iam_handler(internal)
    RETURNS index_am_handler AS 'MODULE_PATHNAME', $function$pgfdb_iam_handler_wrapper$function$
    LANGUAGE C STRICT;

    -- Create the corresponding index access method from the just-registered IAM handler
    CREATE ACCESS METHOD pgfdb_idx TYPE INDEX HANDLER pgfdb_iam_handler;
    ")]
pub fn pgfdb_iam_handler() -> IndexAmHandler {
    IndexAmHandler
}

pub struct IndexAmHandler;

unsafe impl BoxRet for IndexAmHandler {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        fcinfo.return_raw_datum(Datum::from(addr_of_mut!(FDB_INDEX_AM_ROUTINE)))
    }
}

unsafe impl SqlTranslatable for IndexAmHandler {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::literal("index_am_handler"))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("index_am_handler")))
    }
}

// https://www.postgresql.org/docs/current/index-api.html
// Index build function - Called when CREATE INDEX is executed
unsafe extern "C" fn ambuild(
    _heap_relation: Relation,
    _index_relation: Relation,
    _index_info: *mut IndexInfo,
) -> *mut IndexBuildResult {
    log!("IAM: Build index");

    let mut build_result = unsafe { PgBox::<IndexBuildResult>::alloc() };
    build_result.heap_tuples = 0.0;
    build_result.index_tuples = 0.0;

    // TODO: Actually build the index structure in FDB
    // 1. Create a new subspace for the index
    // 2. Scan the heap relation
    // 3. Extract index keys from heap tuples
    // 4. Insert index entries into FDB
    build_result.into_pg()
}

// Insert an index tuple
unsafe extern "C" fn aminsert(
    _index_relation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_tid: ItemPointer,
    _heap_relation: Relation,
    _check_unique: IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut IndexInfo,
) -> bool {
    log!("IAM: Insert into index");

    // TODO: Insert a new index entry
    // 1. Create key from values
    // 2. Store in FDB with heap_tid as value
    true
}

// Begin an index scan
unsafe extern "C" fn ambeginscan(
    _index_relation: Relation,
    _nkeys: ::std::os::raw::c_int,
    _norderbys: ::std::os::raw::c_int,
) -> IndexScanDesc {
    log!("IAM: Begin scan");

    // TODO: Initialize scan state
    // 1. Create FDB range based on scan keys
    // 2. Setup iterator
    std::ptr::null_mut()
}

// Fetch next tuple from scan
unsafe extern "C" fn amgettuple(_scan: IndexScanDesc, _direction: ScanDirection::Type) -> bool {
    log!("IAM: Get tuple");

    // TODO: Get next matching tuple
    // 1. Get next key-value from iterator
    // 2. Return false if no more results
    false
}

// Restart a scan with new scan keys
unsafe extern "C" fn amrescan(
    _scan: IndexScanDesc,
    _keys: ScanKey,
    _nkeys: ::std::os::raw::c_int,
    _orderbys: ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
    log!("IAM: Re-scan");

    // TODO: Reset scan with new keys
    // 1. Update range based on new keys
    // 2. Reset iterator
}

static mut FDB_INDEX_AM_ROUTINE: IndexAmRoutine = IndexAmRoutine {
    type_: pgrx::pg_sys::NodeTag::T_IndexAmRoutine,
    ambuild: Some(ambuild),
    ambuildempty: None, // Not needed
    aminsert: Some(aminsert),
    aminsertcleanup: None,  // Not needed
    ambulkdelete: None,     // Optional - for bulk deletes
    amvacuumcleanup: None,  // Optional - for VACUUM
    amcanreturn: None,      // Optional - index-only scans
    amcostestimate: None,   // Optional - custom cost estimation
    amoptions: None,        // Optional - index-specific options
    amproperty: None,       // Optional - index properties
    ambuildphasename: None, // Optional - progress reporting
    amvalidate: None,       // Optional - index validation
    amadjustmembers: None,  // Optional - parallel scan
    ambeginscan: Some(ambeginscan),
    amrescan: Some(amrescan),
    amgettuple: Some(amgettuple),
    amendscan: None,  // Optional - cleanup at scan end
    ammarkpos: None,  // Optional - mark/restore position
    amrestrpos: None, // Optional - mark/restore position

    // Bitmap scans not supported
    amgetbitmap: None,
    // Parallel scans not supported
    amestimateparallelscan: None,
    aminitparallelscan: None,
    amparallelrescan: None,

    amstrategies: 0,
    amsupport: 0,
    amoptsprocnum: 0,
    amcanorder: true,
    amcanorderbyop: false,
    amcanbackward: true,
    amcanunique: true,
    amcanmulticol: true,
    amoptionalkey: false,
    amsearcharray: false,
    amsearchnulls: true,
    amstorage: false,
    amclusterable: false,
    ampredlocks: false,
    amcanparallel: false,
    amcanbuildparallel: false,
    amcaninclude: false,
    amusemaintenanceworkmem: false,
    amsummarizing: false,
    amparallelvacuumoptions: 0,
    // Variable type of data stored in index
    amkeytype: InvalidOid,
};
