use std::slice::from_raw_parts_mut;

use crate::{errors::FdbErrorExt, iam::utils::encode_datum_for_index};
use foundationdb::RangeOption;
use futures::StreamExt;
use pg_sys::{Datum, IndexBuildResult, IndexInfo, IndexUniqueCheck, ItemPointer, Relation};
use pgrx::{
    pg_sys::{FormData_pg_attribute, Oid},
    prelude::*,
};
use pollster::FutureExt;

// Index build function - Called when CREATE INDEX is executed
pub unsafe extern "C" fn ambuild(
    heap_relation: Relation,
    index_relation: Relation,
    _index_info: *mut IndexInfo,
) -> *mut IndexBuildResult {
    log!("IAM: Build index");

    let mut num_rows = 0;

    let index_oid = unsafe { (*index_relation).rd_id };
    let index_tuple_desc = unsafe { (*index_relation).rd_att };
    let natts = unsafe { (*index_tuple_desc).natts as usize };
    let attrs = (*index_tuple_desc).attrs.as_slice(natts);

    let table_oid = unsafe { (*heap_relation).rd_id };
    let table_subspace = crate::subspace::table(table_oid);

    let txn = crate::transaction::get_transaction();
    let range_option = RangeOption::from(table_subspace.range());

    let mut stream = txn.get_ranges_keyvalues(range_option, false);
    while let Some(item) = stream.next().block_on() {
        let value = item.unwrap_or_pg_error();
        let tuple = crate::coding::Tuple::deserialize(value.value());

        let mut values: Vec<Datum> = Vec::with_capacity(tuple.datums.len());
        let mut isnull: Vec<bool> = Vec::with_capacity(tuple.datums.len());
        for (i, maybe_encoded_datum) in tuple.datums.into_iter().enumerate() {
            if let Some(mut encoded_datum) = maybe_encoded_datum {
                let datum = crate::coding::decode_datum(&mut encoded_datum, attrs[i].atttypid);
                values.push(datum);
                isnull.push(false);
            } else {
                values.push(Datum::null());
                isnull.push(true);
            }
        }

        let key = build_key_from_values(index_oid, tuple.id, natts, attrs, &values, &isnull);
        txn.set(&key, &[]);

        num_rows += 1;
    }

    let mut build_result = unsafe { PgBox::<IndexBuildResult>::alloc() };
    build_result.heap_tuples = num_rows.into();
    build_result.index_tuples = num_rows.into();
    build_result.into_pg()
}

pub unsafe extern "C" fn ambuildempty(_heap_relation: Relation) {
    log!("IAM: Build empty index");
}

// Insert an index tuple
pub unsafe extern "C" fn aminsert(
    index_relation: Relation,
    raw_values: *mut Datum,
    raw_isnull: *mut bool,
    tid: ItemPointer,
    _heap_relation: Relation,
    _check_unique: IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut IndexInfo,
) -> bool {
    log!("IAM: Insert into index");

    // Get ID from TID
    let id = unsafe { pgrx::itemptr::item_pointer_get_block_number_no_check(*tid) };

    // Get the number of attributes in the index
    let index_tuple_desc = unsafe { (*index_relation).rd_att };
    let natts = unsafe { (*index_tuple_desc).natts as usize };
    let index_oid = unsafe { (*index_relation).rd_id };
    let attrs = unsafe { (*index_tuple_desc).attrs.as_slice(natts) };

    let txn = crate::transaction::get_transaction();

    // If this was an update, we need to clear any existing index key
    // TODO: Using TUPLE_CACHE here seems like it might go wrong if we are not in an update
    // (although doing extra key clearing should still be correct)
    if let Some((_, existing_slot)) = crate::tuple_cache::get_with_id(id) {
        let old_values = from_raw_parts_mut((*existing_slot).tts_values, natts);
        let old_isnull = from_raw_parts_mut((*existing_slot).tts_isnull, natts);

        let key = build_key_from_values(index_oid, id, natts, attrs, &old_values, &old_isnull);
        txn.clear(&key);
    }

    // Insert a new key for the indexed values which points back to the row being indexed
    let values = from_raw_parts_mut(raw_values, natts);
    let isnull = from_raw_parts_mut(raw_isnull, natts);

    let key = build_key_from_values(index_oid, id, natts, attrs, values, isnull);
    txn.set(&key, &[]);

    true
}

fn build_key_from_values(
    index_oid: Oid,
    id: u32,
    natts: usize,
    attrs: &[FormData_pg_attribute],
    values: &[Datum],
    isnull: &[bool],
) -> Vec<u8> {
    let index_subspace = crate::subspace::index(index_oid);

    // Prepare tuple elements for the index key
    let mut key_elements = Vec::with_capacity(natts);

    for i in 0..natts {
        if isnull[i] {
            // For NULL values, we'll use a special marker in the tuple
            key_elements.push(foundationdb::tuple::Element::Nil);
        } else {
            // Get the attribute type OID
            let attr = attrs[i];
            let type_oid = attr.atttypid;

            // Get the datum
            let datum = values[i];

            // Encode the datum using our helper function
            // This will convert the Postgres datum to an FDB tuple element
            let element = encode_datum_for_index(datum, type_oid);
            key_elements.push(element);
        }
    }

    // Add the ID to the key elements as the last element
    key_elements.push(foundationdb::tuple::Element::Int(id as i64));

    // Create the key using the subspace and key elements (which now includes the ID)
    index_subspace.pack(&key_elements)
}
