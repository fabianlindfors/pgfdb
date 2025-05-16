use std::slice::from_raw_parts_mut;

use crate::{errors::FdbErrorExt, iam::utils::encode_datum_for_index};
use foundationdb::RangeOption;
use futures::StreamExt;
use pg_sys::{Datum, IndexBuildResult, IndexInfo, IndexUniqueCheck, ItemPointer, Relation};
use pgrx::{
    pg_sys::{FormData_pg_attribute, Oid, TupleTableSlot},
    prelude::*,
};
use pollster::FutureExt;

// Index build function - Called when CREATE INDEX is executed
pub unsafe extern "C-unwind" fn ambuild(
    heap_relation: Relation,
    index_relation: Relation,
    index_info: *mut IndexInfo,
) -> *mut IndexBuildResult {
    unsafe {
        log!("IAM: Build index");

        let mut num_rows = 0;
        let index_oid = (*index_relation).rd_id;
        let table_oid = (*heap_relation).rd_id;
        let table_subspace = crate::subspace::table(table_oid);

        let txn = crate::transaction::get_transaction();
        let range_option = RangeOption::from(table_subspace.range());

        // Create a slot for the heap tuple
        let heap_tuple_desc = (*heap_relation).rd_att;
        let heap_slot =
            pgrx::pg_sys::MakeSingleTupleTableSlot(heap_tuple_desc, &pgrx::pg_sys::TTSOpsVirtual);

        let mut stream = txn.get_ranges_keyvalues(range_option, false);
        while let Some(item) = stream.next().block_on() {
            let value = item.unwrap_or_pg_error();
            let mut tuple = crate::coding::Tuple::deserialize(value.value());
            let id = tuple.id;

            // Load the tuple into the heap slot
            tuple.load_into_tts(heap_slot.as_mut().unwrap());

            // Build and set the index key
            let key =
                build_key_from_table_tuple(index_oid, id, index_relation, heap_slot, index_info);
            txn.set(&key, &[]);

            num_rows += 1;
        }

        // Free the heap slot
        pgrx::pg_sys::ExecDropSingleTupleTableSlot(heap_slot);

        let mut build_result = PgBox::<IndexBuildResult>::alloc();
        build_result.heap_tuples = num_rows.into();
        build_result.index_tuples = num_rows.into();
        build_result.into_pg()
    }
}

pub unsafe extern "C-unwind" fn ambuildempty(_heap_relation: Relation) {
    log!("IAM: Build empty index");
}

// Insert an index tuple
pub unsafe extern "C-unwind" fn aminsert(
    index_relation: Relation,
    raw_values: *mut Datum,
    raw_isnull: *mut bool,
    tid: ItemPointer,
    heap_relation: Relation,
    _check_unique: IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut IndexInfo,
) -> bool {
    unsafe {
        log!("IAM: Insert into index");

        // Get ID from TID
        let id = pgrx::itemptr::item_pointer_get_block_number_no_check(*tid);

        // Get the number of attributes in the index
        let index_tuple_desc = (*index_relation).rd_att;
        let natts = (*index_tuple_desc).natts as usize;
        let index_oid = (*index_relation).rd_id;
        let attrs = (*index_tuple_desc).attrs.as_slice(natts);

        let txn = crate::transaction::get_transaction();

        // If this was an update, we need to clear any existing index key
        // We directly use build_key_from_table_tuple like in tuple_delete
        // TODO: It might be dangerous to rely on the tuple cache here
        if let Some(mut tuple) = crate::tuple_cache::get_with_id(id) {
            let heap_tuple_desc = (*heap_relation).rd_att;
            let heap_slot = pgrx::pg_sys::MakeSingleTupleTableSlot(
                heap_tuple_desc,
                &pgrx::pg_sys::TTSOpsVirtual,
            );

            //  Load the tuple into the heap slot
            tuple.load_into_tts(heap_slot.as_mut().unwrap());

            // Build and clear the index key using existing slot
            let index_info = pg_sys::BuildIndexInfo(index_relation);
            let key = build_key_from_table_tuple(
                index_oid,
                tuple.id,
                index_relation,
                heap_slot,
                index_info,
            );
            txn.clear(&key);

            // Free the slot
            pgrx::pg_sys::ExecDropSingleTupleTableSlot(heap_slot);
        }

        // Insert a new key for the indexed values which points back to the row being indexed
        let values = from_raw_parts_mut(raw_values, natts);
        let isnull = from_raw_parts_mut(raw_isnull, natts);

        let key = build_key_from_index_values(index_oid, id, natts, attrs, values, isnull);
        txn.set(&key, &[]);

        true
    }
}

pub fn build_key_from_table_tuple(
    index_oid: Oid,
    row_id: u32,
    index_rel: Relation,
    table_slot: *mut TupleTableSlot,
    index_info: *mut pg_sys::IndexInfo,
) -> Vec<u8> {
    // Get index tuple descriptor
    let index_tuple_desc = unsafe { (*index_rel).rd_att };
    let natts = unsafe { (*index_tuple_desc).natts as usize };

    // Create a new slot for the index tuple
    let index_slot = unsafe {
        pgrx::pg_sys::MakeSingleTupleTableSlot(index_tuple_desc, &pgrx::pg_sys::TTSOpsVirtual)
    };

    // Generate the index tuple values from the heap tuple
    unsafe {
        // Create a new estate
        let estate = pgrx::pg_sys::CreateExecutorState();

        pgrx::pg_sys::FormIndexDatum(
            index_info,
            table_slot,
            estate,
            (*index_slot).tts_values,
            (*index_slot).tts_isnull,
        );

        // Free the estate
        pgrx::pg_sys::FreeExecutorState(estate);
    }

    // Now extract the values from the index slot and create our key
    let values = unsafe { std::slice::from_raw_parts((*index_slot).tts_values, natts) };
    let isnull = unsafe { std::slice::from_raw_parts((*index_slot).tts_isnull, natts) };
    let attrs = unsafe { (*index_tuple_desc).attrs.as_slice(natts) };

    let index_key = build_key_from_index_values(index_oid, row_id, natts, attrs, values, isnull);

    // Free the slot
    unsafe { pgrx::pg_sys::ExecDropSingleTupleTableSlot(index_slot) };

    index_key
}

pub fn build_key_from_index_values(
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
