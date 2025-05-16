use core::slice;

use foundationdb::future::{FdbSlice, FdbValues};
use foundationdb::tuple::{Element, Subspace};
use foundationdb::{FdbResult, RangeOption, tuple::unpack};
use foundationdb::{KeySelector, Transaction};
use futures::future::join_all;
use futures::stream::empty;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use futures::{Stream, TryFutureExt, TryStreamExt, stream};
use pg_sys::{
    Cost, IndexPath, IndexScanDesc, IndexScanDescData, JoinType::JOIN_INNER, PlannerInfo, Relation,
    ScanDirection, ScanKey, Selectivity, clauselist_selectivity, get_quals_from_indexclauses,
};
use pgrx::itemptr::item_pointer_set_all;
use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::pg_sys::{FormData_pg_attribute, SK_SEARCHNOTNULL, SK_SEARCHNULL, ScanKeyData};
use pgrx::prelude::*;
use pollster::FutureExt as _;

use crate::coding::Tuple;
use crate::errors::FdbErrorExt;
use crate::iam::utils::encode_datum_for_index;
use crate::tuple_cache;

#[repr(C)]
struct FdbIndexScan {
    // Must be first field to ensure proper casting
    base: IndexScanDescData,
    // Stream of values from FDB
    values: BoxStream<'static, FdbResult<(u32, FdbSlice)>>,
}

// https://www.postgresql.org/docs/current/index-cost-estimation.html
pub unsafe extern "C-unwind" fn amcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut Cost,
    index_total_cost: *mut Cost,
    index_selectivity: *mut Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    unsafe {
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
}

// Begin an index scan
pub unsafe extern "C-unwind" fn ambeginscan(
    index_relation: Relation,
    nkeys: i32,
    norderbys: i32,
) -> IndexScanDesc {
    unsafe {
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
        let scan_pointer = scan.as_ptr();
        std::ptr::write(&mut (*scan_pointer).values, empty_stream);

        scan.into_pg() as IndexScanDesc
    }
}

// Fetch next tuple from scan
#[pg_guard]
pub unsafe extern "C-unwind" fn amgettuple(
    scan: IndexScanDesc,
    direction: ScanDirection::Type,
) -> bool {
    log!("IAM: Get tuple, direction={}", direction);

    // Only support forward scans for now
    if direction != ScanDirection::ForwardScanDirection {
        log!("IAM: Only forward scans are supported");
        return false;
    }

    let fdb_scan = scan as *mut FdbIndexScan;

    // Get the next key-value pair from the stream
    let next = unsafe { (*fdb_scan).values.next() }.block_on();

    // If there's no more data, return false
    let Some(result) = next else {
        return false;
    };

    let (id, value) = result.unwrap_or_pg_error();

    // Our index scan doesn't just fetch the index row, it also fetches the corresponding table row.
    // This is to avoid the TAM having to look up each table row one by one, which gets very slow for large
    // index scans. Here we store the fetched table row in the tuple cache so that the TAM can use it in `index_fetch_tuple`.
    let tuple = Tuple::deserialize(&value);
    tuple_cache::populate(tuple);

    // Use a fixed offset of 1 (first item in the block)
    let offset_num = 1u16;

    unsafe {
        // Store back the ID to be looked up by the table access method
        item_pointer_set_all(&mut (*fdb_scan).base.xs_heaptid, id, offset_num);

        // Recheck is probbaly not necessary but the NULL handling right now probably requires it
        (*fdb_scan).base.xs_recheck = true;
        (*fdb_scan).base.xs_recheckorderby = true;

        // This we might be able to check more effectively
        (*fdb_scan).base.xs_heap_continue = true;
    }

    true
}

// Restart a scan with new scan keys
#[pg_guard]
pub unsafe extern "C-unwind" fn amrescan(
    scan: IndexScanDesc,
    keys: ScanKey,
    nkeys: ::std::os::raw::c_int,
    _orderbys: ScanKey,
    norderbys: ::std::os::raw::c_int,
) {
    unsafe {
        log!(
            "IAM: Re-scan with {} keys and {} orderbys",
            nkeys,
            norderbys
        );

        let fdb_scan = scan as *mut FdbIndexScan;

        let index_relation = (*scan).indexRelation;
        let index_tuple_desc = (*index_relation).rd_att;
        let attrs = (*index_tuple_desc)
            .attrs
            .as_slice((*index_tuple_desc).natts as usize);
        let scan_keys = slice::from_raw_parts(keys, nkeys as usize);

        // Construct a range option representing what part of the index we need to iterate over based on the scan keys
        let index_oid = (*index_relation).rd_id;
        let index_subspace = crate::subspace::index(index_oid);
        let range_options = range_options_for_scan(index_subspace, scan_keys, attrs);

        let table_oid = (*(*scan).heapRelation).rd_id;

        // Create a stream of key-value pairs from FDB from all the range options chained together
        let txn = crate::transaction::get_transaction();
        let stream = range_options
            .into_iter()
            .fold(empty().boxed(), |stream, range_option| {
                let index_scan = txn
                    .get_ranges(range_option, false)
                    .map_ok(move |values| {
                        index_values_to_table_lookups(
                            txn,
                            crate::subspace::table(table_oid),
                            values,
                        )
                    })
                    .try_flatten();

                stream.chain(index_scan).boxed()
            });

        // Replace the existing stream
        // First, drop the old stream to avoid leaking resources
        let old_stream = std::ptr::replace(&mut (*fdb_scan).values, stream);
        drop(old_stream);
    }
}

// Takes a list of FDB values from an index scan and performs point lookups against the table for those rows.
// The intent here is to schedule all those point lookups in parallel and then convert them into a stream of results
// with the full table row. This makes for more efficient index scans, compared to just scanning the index and then
// having the TAM look up each row one by one.
//
// This could likely be improved by using mapped ranges in FDB but currently they don't support Read Your Own Writes and
// will fail hard if reading a value that was written in the same transaction. Maybe one could implement mapped ranges still
// and fall back to the logic below if that error is encountered. Or alternatively, have some heuristic for chosing between
// a mapped range or the logic below. Maybe as simple as if any write has been done in the transaction, don't use mapped ranges?
fn index_values_to_table_lookups(
    txn: &'static Transaction,
    table_subspace: Subspace,
    values: FdbValues,
) -> impl Stream<Item = FdbResult<(u32, FdbSlice)>> {
    let ids: Vec<u32> = values
        .into_iter()
        .map(|value| {
            // Unpack the key to get the tuple elements
            let key_tuple_elements: Vec<Element> = unpack(value.key()).unwrap_or_report();

            // The ID is the last element in the key tuple
            let id = key_tuple_elements.last().unwrap().as_i64().unwrap() as u32;

            id
        })
        .collect();

    let future = join_all(ids.iter().map(|id| {
        let id = id.clone();
        txn.get(&table_subspace.pack(&id), false)
            .map_ok(move |result| result.map(|slice| (id, slice)))
    }));

    let nested_stream = stream::once(future.map(stream::iter));
    nested_stream.flatten().filter_map(async |item| match item {
        Ok(result) => match result {
            Some(result) => Some(Ok(result)),
            None => None,
        },
        Err(err) => Some(Err(err)),
    })
}

fn range_options_for_scan<'a>(
    index_subspace: Subspace,
    scan_keys: &'a [ScanKeyData],
    attrs: &'a [FormData_pg_attribute],
) -> Vec<RangeOption<'a>> {
    // This should never happen but if there are no scan keys, we scan the entire index
    let [head @ .., last] = scan_keys else {
        return vec![RangeOption::from(index_subspace.range())];
    };

    let mut head_tuple_elements: Vec<Element> = Vec::with_capacity(head.len());

    // If there are more than one scan key (i.e. multi-column index), then only the final scan key
    // can use a non-equality operator.
    for head_scan_key in head {
        // Must use equality operator (we can probably support inequality as well by splitting into more ranges)
        if head_scan_key.sk_strategy != 3 {
            panic!(
                "IAM: Only equality operators are supported for multi-column index scans on non-final scan keys"
            );
        }

        let attr = attrs[head_scan_key.sk_attno as usize - 1];
        let element = encode_datum_for_index(head_scan_key.sk_argument, attr.atttypid);
        head_tuple_elements.push(element);
    }

    // If we have a multi-column index and query, we will now have some `head_tuple_elements`
    // and can create a new prefix for our search
    let base_subspace = if head_tuple_elements.is_empty() {
        index_subspace
    } else {
        index_subspace.subspace(&head_tuple_elements)
    };

    // We are now at the final part of the range building for the final column of the scan keys.
    // Here we can support more than just equality by building different ranges to scan.
    let attr = attrs[last.sk_attno as usize - 1];

    let element = if last.sk_flags as u32 & SK_SEARCHNULL != 0
        || last.sk_flags as u32 & SK_SEARCHNOTNULL != 0
    {
        // If this is an IS NULL or IS NOT NULL scan, we shouldn't encode and instead just use the tuple nil value
        Element::Nil
    } else {
        encode_datum_for_index(last.sk_argument, attr.atttypid)
    };

    // Based on what the operator (strategy) is for the final key, construct the final search range
    match last.sk_strategy {
        // Strategy 1: Less than (<)
        1 => {
            let start_key = KeySelector::first_greater_or_equal(base_subspace.range().0);
            let end_key = KeySelector::last_less_than(base_subspace.subspace(&element).range().1);
            vec![RangeOption::from((start_key, end_key))]
        }
        // Strategy 2: Less than or equal (<=)
        2 => {
            let start_key = KeySelector::first_greater_or_equal(base_subspace.range().0);
            // End is exclusive in ranges so we need to use this key selector to include the element value
            let end_key =
                KeySelector::first_greater_than(base_subspace.subspace(&element).range().1);
            vec![RangeOption::from((start_key, end_key))]
        }
        // Strategy 3: Equality (=)
        // Also covers IS NULL scans
        i if i == 3 || last.sk_flags as u32 & SK_SEARCHNULL != 0 => {
            vec![RangeOption::from(base_subspace.subspace(&element).range())]
        }
        // Strategy 6: Not equals (!=)
        // Also covers IS NOT NULL scans
        i if i == 6 || last.sk_flags as u32 & SK_SEARCHNOTNULL != 0 => {
            // For not equals, we take the inverse of equals above and hence need two ranges:
            // 1. Everything from the start of the subspace up to (but not including) the start of the equals subspace
            // 2. Everything after the equals subspace to the end of the subspace
            let equals_subspace = base_subspace.subspace(&element);

            let before_range = RangeOption::from((
                KeySelector::first_greater_or_equal(base_subspace.range().0),
                KeySelector::first_greater_or_equal(equals_subspace.range().0),
            ));

            let after_range = RangeOption::from((
                KeySelector::first_greater_than(equals_subspace.range().1),
                KeySelector::first_greater_than(base_subspace.range().1),
            ));

            vec![before_range, after_range]
        }
        // Strategy 4: Greater than or equal (>=)
        4 => {
            // For greater than or equal, we start from the element itself
            let start_key =
                KeySelector::first_greater_or_equal(base_subspace.subspace(&element).range().0);
            let end_key = KeySelector::first_greater_than(base_subspace.range().1);
            vec![RangeOption::from((start_key, end_key))]
        }
        // Strategy 5: Greater than (>)
        5 => {
            // For greater than, we need to start from the element and go to the end of the subspace
            let start_key =
                KeySelector::first_greater_than(base_subspace.subspace(&element).range().1);
            let end_key = KeySelector::first_greater_than(base_subspace.range().1);
            vec![RangeOption::from((start_key, end_key))]
        }
        _ => panic!("Unsupported strategy for scan key {}", last.sk_strategy),
    }
}

// End an index scan
pub unsafe extern "C-unwind" fn amendscan(scan: IndexScanDesc) {
    log!("IAM: End scan");

    let fdb_scan = scan as *mut FdbIndexScan;

    // Take ownership of the stream to drop it
    let stream = unsafe { std::ptr::read(&(*fdb_scan).values) };
    drop(stream);
}
