use std::borrow::Cow;
use std::ptr;

use foundationdb::tuple::Element;
use foundationdb::{future::FdbValue, tuple::unpack, FdbResult, RangeOption};
use futures::{stream::BoxStream, FutureExt, StreamExt};
use pg_sys::{
    bytea, clauselist_selectivity, get_quals_from_indexclauses, Cost, Datum, IndexAmRoutine,
    IndexBuildResult, IndexInfo, IndexPath, IndexScanDesc, IndexScanDescData, IndexUniqueCheck,
    InvalidOid, ItemPointer, JoinType::JOIN_INNER, PlannerInfo, Relation, ScanDirection, ScanKey,
    Selectivity,
};
use pgrx::itemptr::item_pointer_set_all;
use pgrx::prelude::*;
use pgrx::{callconv::BoxRet, pg_sys::panic::ErrorReportable};
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use std::task::{Context, Poll, Waker};

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
    
    CREATE OPERATOR CLASS pgfdb_idx_text
    DEFAULT FOR TYPE TEXT USING pgfdb_idx AS
    OPERATOR 1 = (TEXT, TEXT);
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
    index_relation: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    tid: ItemPointer,
    _heap_relation: Relation,
    _check_unique: IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut IndexInfo,
) -> bool {
    log!("IAM: Insert into index");

    // Get the number of attributes in the index
    let index_tuple_desc = unsafe { (*index_relation).rd_att };
    let natts = unsafe { (*index_tuple_desc).natts as usize };

    // Create a subspace for this index using the index relation OID
    let index_oid = unsafe { (*index_relation).rd_id };
    let index_subspace = crate::subspace::index(index_oid);

    // Get the current transaction
    let txn = crate::transaction::get_transaction();

    // Prepare tuple elements for the index key
    let mut key_elements = Vec::with_capacity(natts);

    for i in 0..natts {
        if unsafe { *isnull.add(i) } {
            // For NULL values, we'll use a special marker in the tuple
            key_elements.push(foundationdb::tuple::Element::Nil);
        } else {
            // Get the attribute type OID
            let attr = unsafe { (*index_tuple_desc).attrs.as_slice(natts)[i] };
            let type_oid = attr.atttypid;

            // Get the datum
            let datum = unsafe { *values.add(i) };

            // Encode the datum using our helper function
            // This will convert the Postgres datum to an FDB tuple element
            match encode_datum_for_index(datum, type_oid) {
                Some(element) => key_elements.push(element),
                None => {
                    log!("IAM: Failed to encode datum for index");
                    return false;
                }
            }
        }
    }

    // Get ID from TID
    let id = unsafe { pgrx::itemptr::item_pointer_get_block_number_no_check(*tid) };

    // Add the ID to the key elements as the last element
    key_elements.push(foundationdb::tuple::Element::Int(id as i64));

    // Create the key using the subspace and key elements (which now includes the ID)
    let key = index_subspace.pack(&key_elements);

    // Set an empty value since the ID is now part of the key
    txn.set(&key, &[]);

    true
}

// Helper function to encode a Postgres datum into an FDB tuple element
// This function will need to be implemented to handle different Postgres types
fn encode_datum_for_index<'a>(
    datum: Datum,
    type_oid: pg_sys::Oid,
) -> Option<foundationdb::tuple::Element<'a>> {
    match type_oid {
        // INT4/INTEGER (OID 23)
        pg_sys::INT4OID => {
            // Convert the datum to a Rust i32
            let value = unsafe { pg_sys::DatumGetInt64(datum) };
            Some(foundationdb::tuple::Element::Int(value))
        }
        // TEXT (OID 25) or VARCHAR (OID 1043)
        pg_sys::VARCHAROID | pg_sys::TEXTOID => {
            // Use pgrx's text_to_rust_str_unchecked to convert to a Rust string
            let varlena: PgVarlena<()> = unsafe { PgVarlena::from_datum(datum) };
            let text = unsafe { pgrx::text_to_rust_str_unchecked(varlena.into_pg()).to_string() };
            Some(foundationdb::tuple::Element::String(Cow::Owned(text)))
        }
        // Add more types as needed
        _ => {
            // Log unsupported types
            log!(
                "IAM: encode_datum_for_index not yet implemented for type OID: {}",
                type_oid.as_u32()
            );
            None
        }
    }
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
struct FdbIndexScan {
    // Must be first field to ensure proper casting
    base: IndexScanDescData,
    // Stream of values from FDB
    values: BoxStream<'static, FdbResult<FdbValue>>,
}

// Begin an index scan
unsafe extern "C" fn ambeginscan(
    index_relation: Relation,
    nkeys: i32,
    norderbys: i32,
) -> IndexScanDesc {
    log!("IAM: Begin scan");

    let mut scan = PgBox::<FdbIndexScan>::alloc();

    // Initialize the base IndexScanDescData
    scan.base.indexRelation = index_relation;
    scan.base.numberOfKeys = nkeys;
    scan.base.numberOfOrderBys = norderbys;
    scan.base.keyData = std::ptr::null_mut();
    scan.base.orderByData = std::ptr::null_mut();
    scan.base.xs_snapshot = std::ptr::null_mut();
    scan.base.xs_want_itup = false;
    scan.base.xs_temp_snap = false;

    // Create an empty stream initially - will be populated in rescan
    let empty_stream = futures::stream::empty().boxed();

    // We must use ptr::write to avoid dropping uninitialized memory
    unsafe {
        let scan_pointer = scan.as_ptr();
        std::ptr::write(&mut (*scan_pointer).values, empty_stream);
    }

    scan.into_pg() as IndexScanDesc
}

// Fetch next tuple from scan
unsafe extern "C" fn amgettuple(scan: IndexScanDesc, direction: ScanDirection::Type) -> bool {
    log!("IAM: Get tuple, direction={}", direction);

    // Only support forward scans for now
    if direction != ScanDirection::ForwardScanDirection {
        log!("IAM: Only forward scans are supported");
        return false;
    }

    let fdb_scan = scan as *mut FdbIndexScan;

    // Get the next key-value pair from the stream
    let mut next_fut = unsafe { (*fdb_scan).values.next() };
    let mut ctx = Context::from_waker(&Waker::noop());

    // Poll the future until it's ready
    let next = loop {
        match next_fut.poll_unpin(&mut ctx) {
            Poll::Ready(result) => {
                break result;
            }
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(1)),
        }
    };

    // If there's no more data, return false
    let Some(result) = next else {
        return false;
    };

    let Ok(key_value) = result else {
        return false;
    };

    // Unpack the key to get the tuple elements
    let key_tuple_elements: Vec<Element> = unpack(key_value.key()).unwrap_or_report();

    // The ID is the last element in the key tuple
    let id = key_tuple_elements.last().unwrap().as_i64().unwrap() as u32;

    // Use a fixed offset of 1 (first item in the block)
    let offset_num = 1u16;

    // Set the ItemPointer in the scan
    unsafe {
        item_pointer_set_all(&mut (*scan).xs_heaptid, id, offset_num);
    }

    true
}

// Restart a scan with new scan keys
unsafe extern "C" fn amrescan(
    scan: IndexScanDesc,
    keys: ScanKey,
    nkeys: ::std::os::raw::c_int,
    _orderbys: ScanKey,
    norderbys: ::std::os::raw::c_int,
) {
    log!(
        "IAM: Re-scan with {} keys and {} orderbys",
        nkeys,
        norderbys
    );

    let fdb_scan = scan as *mut FdbIndexScan;
    let index_relation = (*scan).indexRelation;
    let index_oid = (*index_relation).rd_id;
    let index_subspace = crate::subspace::index(index_oid);

    // Get the transaction
    let txn = crate::transaction::get_transaction();

    // Create a range based on the scan keys
    let range_option = if nkeys > 0 {
        // We have scan keys, so create a prefix-based range
        let index_tuple_desc = (*index_relation).rd_att;
        let mut prefix_elements = Vec::with_capacity(nkeys as usize);

        // Process each scan key to build our prefix
        for i in 0..nkeys {
            let scan_key = &*keys.add(i as usize);

            // Only handle equality operators (strategy number 1) for now
            if scan_key.sk_strategy != 1 {
                log!("IAM: Only equality operators are supported for index scans");
                continue;
            }

            // Get the attribute type OID
            let attr_num = scan_key.sk_attno as usize - 1; // Convert to 0-based index
            let attr = (*index_tuple_desc)
                .attrs
                .as_slice((*index_tuple_desc).natts as usize)[attr_num];
            let type_oid = attr.atttypid;

            // Encode the datum using our helper function
            if let Some(element) = encode_datum_for_index(scan_key.sk_argument, type_oid) {
                prefix_elements.push(element);
            } else {
                log!("IAM: Failed to encode scan key datum for index");
            }
        }

        // Create a prefix-based range
        if !prefix_elements.is_empty() {
            log!(
                "IAM: Using prefix-based range scan with {} elements",
                prefix_elements.len()
            );
            // Create a range that starts with our prefix and ends just before the next possible prefix
            RangeOption::from(index_subspace.subspace(&prefix_elements).range())
        } else {
            // Fall back to full range if we couldn't create a prefix
            RangeOption::from(index_subspace.range())
        }
    } else {
        // No scan keys, so scan the entire index
        RangeOption::from(index_subspace.range())
    };

    // Create a stream of key-value pairs from FDB
    let stream = txn.get_ranges_keyvalues(range_option, false).boxed();

    // Replace the existing stream
    // First, drop the old stream to avoid leaking resources
    let old_stream = std::ptr::replace(&mut (*fdb_scan).values, stream);
    drop(old_stream);
}

// End an index scan
unsafe extern "C" fn amendscan(scan: IndexScanDesc) {
    log!("IAM: End scan");

    let fdb_scan = scan as *mut FdbIndexScan;

    // Take ownership of the stream to drop it
    let stream = unsafe { std::ptr::read(&(*fdb_scan).values) };
    drop(stream);
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
        index_am_routine.amendscan = Some(amendscan);
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
