use crate::iam::utils::encode_datum_for_index;
use pg_sys::{Datum, IndexBuildResult, IndexInfo, IndexUniqueCheck, ItemPointer, Relation};
use pgrx::prelude::*;

// Index build function - Called when CREATE INDEX is executed
pub unsafe extern "C" fn ambuild(
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

pub unsafe extern "C" fn ambuildempty(_heap_relation: Relation) {
    log!("IAM: Build empty index");
}

// Insert an index tuple
pub unsafe extern "C" fn aminsert(
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
    let txn = crate::transaction::get_transaction();
    txn.set(&key, &[]);

    true
}
