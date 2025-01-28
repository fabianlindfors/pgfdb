use std::ptr;

use pg_sys::{
    bytea, clauselist_selectivity, get_quals_from_indexclauses, Cost, Datum, IndexAmRoutine,
    IndexBuildResult, IndexInfo, IndexPath, IndexScanDesc, IndexScanDescData, IndexUniqueCheck,
    InvalidOid, ItemPointer, JoinType::JOIN_INNER, PlannerInfo, Relation, ScanDirection, ScanKey,
    Selectivity,
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

    -- Operator classes
    CREATE OPERATOR CLASS pgfdb_idx_integer 
    DEFAULT FOR TYPE INTEGER USING pgfdb_idx AS
    OPERATOR 1 = (INTEGER, INTEGER);
    ")]
pub fn pgfdb_iam_handler() -> IndexAmHandler {
    IndexAmHandler
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

unsafe extern "C" fn ambuildempty(_heap_relation: Relation) {
    log!("IAM: Build empty index");
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

// https://www.postgresql.org/docs/current/index-cost-estimation.html
unsafe extern "C" fn amcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut Cost,
    index_total_cost: *mut Cost,
    index_selectivity: *mut Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    log!("IAM: Calculate cost estimate");

    *index_startup_cost = 0.0;

    *index_total_cost = 0.0;

    *index_correlation = 0.0;

    *index_pages = 0.0;

    let index_quals = get_quals_from_indexclauses((*path).indexclauses);
    *index_selectivity = clauselist_selectivity(
        root,
        index_quals,
        (*(*(*path).indexinfo).rel).relid as i32,
        JOIN_INNER,
        std::ptr::null_mut(),
    );
}

#[repr(C)]
struct FdbIndexScanDesc {
    // Must be first field to ensure proper casting
    base: IndexScanDescData,
}

// Begin an index scan
unsafe extern "C" fn ambeginscan(
    index_relation: Relation,
    nkeys: i32,
    norderbys: i32,
) -> IndexScanDesc {
    log!("IAM: Begin scan");

    let mut scan = PgBox::<FdbIndexScanDesc>::alloc();

    // Initialize the base IndexScanDescData
    scan.base.indexRelation = index_relation;
    scan.base.numberOfKeys = nkeys;
    scan.base.numberOfOrderBys = norderbys;
    scan.base.keyData = std::ptr::null_mut();
    scan.base.orderByData = std::ptr::null_mut();
    scan.base.xs_snapshot = std::ptr::null_mut();
    scan.base.xs_want_itup = false;
    scan.base.xs_temp_snap = false;

    scan.into_pg() as IndexScanDesc
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

unsafe extern "C" fn amoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    // Null for default behaviour
    // We don't support any options on the index yet
    ptr::null_mut()
}

pub struct IndexAmHandler;

unsafe impl BoxRet for IndexAmHandler {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        // An IAM must be returned as a palloced struct, as opposed to a TAM which can be statically allocated
        let mut index_am_routine =
            unsafe { PgBox::<IndexAmRoutine>::alloc_node(pgrx::pg_sys::NodeTag::T_IndexAmRoutine) };

        index_am_routine.ambuild = Some(ambuild);
        index_am_routine.ambuildempty = Some(ambuildempty); // Not needed
        index_am_routine.aminsert = Some(aminsert);
        index_am_routine.aminsertcleanup = None; // Not needed
        index_am_routine.ambulkdelete = None; // Optional - for bulk deletes
        index_am_routine.amvacuumcleanup = None; // Optional - for VACUUM
        index_am_routine.amcanreturn = None; // Optional - index-only scans
        index_am_routine.amcostestimate = Some(amcostestimate); // Optional - custom cost estimation
        index_am_routine.amoptions = Some(amoptions);
        index_am_routine.amproperty = None; // Optional - index properties
        index_am_routine.ambuildphasename = None; // Optional - progress reporting
        index_am_routine.amvalidate = None; // Optional - index validation
        index_am_routine.amadjustmembers = None; // Optional - parallel scan
        index_am_routine.ambeginscan = Some(ambeginscan);
        index_am_routine.amrescan = Some(amrescan);
        index_am_routine.amgettuple = Some(amgettuple);
        index_am_routine.amendscan = None; // Optional - cleanup at scan end
        index_am_routine.ammarkpos = None; // Optional - mark/restore position
        index_am_routine.amrestrpos = None; // Optional - mark/restore position

        // Bitmap scans not supported
        index_am_routine.amgetbitmap = None;
        // Parallel scans not supported
        index_am_routine.amestimateparallelscan = None;
        index_am_routine.aminitparallelscan = None;
        index_am_routine.amparallelrescan = None;

        // Stategies:
        // 1: =
        index_am_routine.amstrategies = 1;

        index_am_routine.amsupport = 0;
        index_am_routine.amoptsprocnum = 0;
        index_am_routine.amcanorder = true;
        index_am_routine.amcanorderbyop = false;
        index_am_routine.amcanbackward = true;
        index_am_routine.amcanunique = true;
        index_am_routine.amcanmulticol = true;
        index_am_routine.amoptionalkey = false;
        index_am_routine.amsearcharray = false;
        index_am_routine.amsearchnulls = true;
        index_am_routine.amstorage = false;
        index_am_routine.amclusterable = false;
        index_am_routine.ampredlocks = false;
        index_am_routine.amcanparallel = false;
        index_am_routine.amcanbuildparallel = false;
        index_am_routine.amcaninclude = false;
        index_am_routine.amusemaintenanceworkmem = false;
        index_am_routine.amsummarizing = false;
        index_am_routine.amparallelvacuumoptions = 0;
        // Variable type of data stored in index
        index_am_routine.amkeytype = InvalidOid;

        fcinfo.return_raw_datum(index_am_routine.into_datum().unwrap())
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
