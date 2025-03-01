use std::task::{Context, Poll, Waker};

use foundationdb::tuple::Element;
use foundationdb::{future::FdbValue, tuple::unpack, FdbResult, RangeOption};
use futures::{stream::BoxStream, FutureExt, StreamExt};
use pg_sys::{
    clauselist_selectivity, get_quals_from_indexclauses, Cost, IndexPath, IndexScanDesc,
    IndexScanDescData, JoinType::JOIN_INNER, PlannerInfo, Relation, ScanDirection, ScanKey,
    Selectivity,
};
use pgrx::itemptr::item_pointer_set_all;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;

use crate::iam::utils::encode_datum_for_index;

#[repr(C)]
struct FdbIndexScan {
    // Must be first field to ensure proper casting
    base: IndexScanDescData,
    // Stream of values from FDB
    values: BoxStream<'static, FdbResult<FdbValue>>,
}

// https://www.postgresql.org/docs/current/index-cost-estimation.html
pub unsafe extern "C" fn amcostestimate(
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

// Begin an index scan
pub unsafe extern "C" fn ambeginscan(
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
pub unsafe extern "C" fn amgettuple(scan: IndexScanDesc, direction: ScanDirection::Type) -> bool {
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
pub unsafe extern "C" fn amrescan(
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
pub unsafe extern "C" fn amendscan(scan: IndexScanDesc) {
    log!("IAM: End scan");

    let fdb_scan = scan as *mut FdbIndexScan;

    // Take ownership of the stream to drop it
    let stream = unsafe { std::ptr::read(&(*fdb_scan).values) };
    drop(stream);
}
