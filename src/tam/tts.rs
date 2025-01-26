// This module is not used
// I started out by building a custom table slot but realised that the virtual one would work just as well

use std::ptr;

use foundationdb::tuple::{unpack, Element};
use pgrx::{
    itemptr::item_pointer_set_all,
    log, pg_guard,
    pg_sys::{
        heap_form_minimal_tuple, heap_form_tuple, Datum, HeapTuple, MinimalTuple, TupleTableSlot,
        TupleTableSlotOps, TTS_FLAG_EMPTY,
    },
};

use crate::tam::coding;

use super::coding::Tuple;

#[repr(C)]
pub struct FdbTupleTableSlot {
    pub base: TupleTableSlot,
    pub data: FdbTuple,
}

impl FdbTupleTableSlot {
    pub fn store_fdb_value(&mut self, key: &[u8], value: &[u8]) {
        self.base.tts_flags &= !TTS_FLAG_EMPTY as u16;
        let tuple_desc = self.base.tts_tupleDescriptor;
        let attrs = unsafe {
            (*tuple_desc)
                .attrs
                .as_slice((*tuple_desc).natts.try_into().unwrap())
        };

        // Get the key from the key tuple (last part, encoded as u64)
        let key: Vec<Element> = unpack(key).unwrap();
        let id = key
            .last()
            .unwrap()
            .as_i64()
            .expect("expected final key element to be an integer row ID") as u32;
        item_pointer_set_all(&mut self.base.tts_tid, id, 1);

        // Decode the value
        let mut tuple: Tuple = serde_cbor::from_slice(value).unwrap();

        let Value::InMemory(nulls, datums) = &mut self.data.value else {
            panic!("unexpected non-decoded value on tuple table slot");
        };

        // We expect the number of tuples to match the number of columns on the table
        // This might not hold after some DDL though
        assert_eq!(unsafe { (*tuple_desc).natts as usize }, tuple.datums_len());

        // Clear any existing values (capacity should remain at the same size as the number of columns)
        nulls.clear();
        datums.clear();

        // Iterate over all the datums in the serialized data and decode them
        for i in 0..tuple.datums_len() {
            // To be able to decode, we must know the Oid of the attribute in question
            let attr_type_oid = attrs[i as usize].atttypid;
            match tuple.decode_datum(i, attr_type_oid) {
                Some(datum) => {
                    nulls.push(false);
                    datums.push(datum);
                }
                None => {
                    nulls.push(true);
                    datums.push(Datum::null());
                }
            }
        }

        self.base.tts_nvalid = tuple.datums_len() as i16;
    }
}

#[derive(Debug)]
pub struct FdbTuple {
    pub value: Value,
}

#[derive(Debug)]
pub enum Value {
    Encoded(Vec<u8>),
    // These aren't directly used but there are pointers to them from `FdbTupleTableSlot` which
    // are accessed by Postgres
    #[allow(dead_code)]
    InMemory(Vec<bool>, Vec<Datum>),
}

impl FdbTuple {
    pub fn as_slice(&self) -> &[u8] {
        if let Value::Encoded(in_memory_val) = &self.value {
            return in_memory_val;
        }

        panic!("couldn't get data of FDB tuple");
    }
}

// More details in https://github.com/postgres/postgres/blob/master/src/include/executor/tuptable.h
pub static CUSTOM_SLOT_OPS: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: std::mem::size_of::<FdbTupleTableSlot>(),
    init: Some(custom_init),
    release: Some(custom_release),
    clear: Some(custom_clear),
    getsomeattrs: Some(custom_getsomeattrs),
    getsysattr: Some(custom_getsysattr),
    materialize: Some(custom_materialize),
    copyslot: Some(custom_copyslot),
    is_current_xact_tuple: Some(custom_is_current_xact_tuple),
    get_heap_tuple: None,
    copy_heap_tuple: Some(custom_copy_heap_tuple),
    get_minimal_tuple: None,
    copy_minimal_tuple: Some(custom_copy_minimal_tuple),
};

/// Initialize a newly created slot. Sets up initial state and allocates custom data.
/// The slot starts empty (TTS_FLAG_EMPTY) with no valid values.
#[pg_guard]
unsafe extern "C" fn custom_init(slot: *mut TupleTableSlot) {
    let natts = (*(*slot).tts_tupleDescriptor).natts;

    log!("TTS({:p}): Init slot, natts={}", slot, natts);

    // Postgres wants a tuple table slot to have two vectors, one for nulls and one for datums
    let mut nulls: Vec<bool> = Vec::with_capacity(natts as usize);
    let mut datums: Vec<Datum> = Vec::with_capacity(natts as usize);

    (*slot).tts_flags = TTS_FLAG_EMPTY as u16;
    (*slot).tts_nvalid = 0;

    // These are the values that Postgres will read from our TTS
    // We use references here but the data is owned by the TTS itself
    (*slot).tts_isnull = nulls.as_mut_ptr();
    (*slot).tts_values = datums.as_mut_ptr();

    // Postgres expects the TTS to have some storage ready for datums and nulls that it can write to
    let custom_slot = slot as *mut FdbTupleTableSlot;

    // We move the vectors so they belong to the TTS and will be dropped when it's dropped
    // This way the references will remain valid
    (*custom_slot).data = FdbTuple {
        value: Value::InMemory(nulls, datums),
    };
}

/// Clean up and free resources when slot is destroyed.
/// Responsible for freeing the custom_data but not the slot itself.
#[pg_guard]
unsafe extern "C" fn custom_release(slot: *mut TupleTableSlot) {
    log!("TTS({:p}): Release", slot);

    let custom_slot = slot as *mut FdbTupleTableSlot;

    // Drop the slot to release the FdbValue it holds a reference to
    let custom_slot_full = ptr::read(custom_slot);
    drop(custom_slot_full);
}

/// Clear the contents of the slot but keep the tuple descriptor.
/// Sets the slot to empty state and resets the number of valid values.
#[pg_guard]
unsafe extern "C" fn custom_clear(slot: *mut TupleTableSlot) {
    let natts = (*(*slot).tts_tupleDescriptor).natts;

    log!("TTS({:p}): Clear", slot);

    let custom_slot = slot as *mut FdbTupleTableSlot;

    let mut nulls: Vec<bool> = Vec::with_capacity(natts as usize);
    let mut datums: Vec<Datum> = Vec::with_capacity(natts as usize);

    (*custom_slot).base.tts_flags = TTS_FLAG_EMPTY as u16;
    (*custom_slot).base.tts_nvalid = 0;
    (*custom_slot).base.tts_isnull = nulls.as_mut_ptr();
    (*custom_slot).base.tts_values = datums.as_mut_ptr();

    // Will drop any existing data if it exists and free it
    (*custom_slot).data = FdbTuple {
        value: Value::InMemory(nulls, datums),
    };
}

/// Fill the slot's tts_values/tts_isnull arrays for the first natts attributes.
/// May be called with natts > number of available attributes.
/// Must set tts_nvalid to actual number of valid values returned.
#[pg_guard]
unsafe extern "C" fn custom_getsomeattrs(slot: *mut TupleTableSlot, _nattss: i32) {
    log!("TTS({:p}): Get some attributes", slot);
    // Implement attribute loading logic here
}

/// Get a system attribute value as a Datum and set isnull flag.
/// Should error if slot type doesn't support system attributes.
/// Currently returns null for all system attributes.
#[pg_guard]
unsafe extern "C" fn custom_getsysattr(
    slot: *mut TupleTableSlot,
    _attnum: i32,
    isnull: *mut bool,
) -> Datum {
    log!("TTS({:p}): Get system attributes", slot);
    *isnull = true;
    Datum::from(0)
}

/// Make slot contents independent of external resources.
/// After this call, slot should not depend on buffers, memory contexts etc.
/// No-op for this implementation since data is already self-contained.
#[pg_guard]
unsafe extern "C" fn custom_materialize(slot: *mut TupleTableSlot) {
    log!("TTS({:p}): Materialize", slot);

    let custom_slot = slot as *mut FdbTupleTableSlot;
    let base = (*custom_slot).base;
    let tupledesc = base.tts_tupleDescriptor;
    let attrs = (*tupledesc)
        .attrs
        .as_slice(base.tts_nvalid.try_into().unwrap());

    let mut tuple = coding::Tuple::new(base.tts_nvalid);

    for i in 0..(base.tts_nvalid as isize) {
        if *base.tts_isnull.offset(i) {
            tuple.add_null();
            continue;
        }

        let datum = base.tts_values.offset(i);
        tuple.add_datum(datum, attrs[i as usize].atttypid);
    }

    (*custom_slot).data.value = Value::Encoded(serde_cbor::to_vec(&tuple).unwrap());
}

/// Copy source slot's contents into destination slot's context.
/// Slots must have same number of attributes.
/// Currently only copies flags and number of valid values.
#[pg_guard]
unsafe extern "C" fn custom_copyslot(dst: *mut TupleTableSlot, src: *mut TupleTableSlot) {
    log!("TTS({:p}): Copy slot", src);

    (*dst).tts_flags = (*src).tts_flags;
    (*dst).tts_nvalid = (*src).tts_nvalid;
    (*dst).tts_isnull = (*src).tts_isnull;
    (*dst).tts_values = (*src).tts_values;
}

/// Get the value and null flag for a specific attribute by number.
/// Attribute numbers are 1-based. Returns 0/null for out of range attributes.
#[pg_guard]
unsafe extern "C" fn custom_getattr(
    slot: *mut TupleTableSlot,
    _attnum: i32,
    _isnull: *mut bool,
) -> Datum {
    log!("TTS({:p}): Get attribute", slot);

    let _custom_slot = slot as *mut FdbTupleTableSlot;
    Datum::from(0)
}

/// Get the null flag for a specific attribute by number.
/// Returns true for out of range attributes.
#[pg_guard]
unsafe extern "C" fn custom_get_isnull(slot: *mut TupleTableSlot, _attnum: i32) -> bool {
    log!("TTS({:p}): Get is null", slot);

    let _custom_slot = slot as *mut FdbTupleTableSlot;
    false
}

/// Get direct access to the slot's values array.
/// Used for bulk access to attribute values.
#[pg_guard]
unsafe extern "C" fn custom_get_values(slot: *mut TupleTableSlot) -> *mut Datum {
    log!("TTS({:p}): Get values", slot);

    (*slot).tts_values
}

/// Check if tuple was created by current transaction.
/// Returns false since custom slots don't track transaction visibility.
#[pg_guard]
unsafe extern "C" fn custom_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    log!("TTS({:p}): Is current xact tuple", slot);

    // For custom slots, we typically return false as we don't track transaction visibility
    false
}

/// Return a copy of slot contents as a minimal tuple.
/// Returns null since custom slots don't support minimal tuples.
#[pg_guard]
unsafe extern "C" fn custom_copy_minimal_tuple(slot: *mut TupleTableSlot) -> MinimalTuple {
    let custom_slot = slot as *mut FdbTupleTableSlot;

    log!(
        "TTS({:p}): Copy minimal tuple, value={:?}",
        slot,
        (*custom_slot).data.value
    );

    heap_form_minimal_tuple(
        (*slot).tts_tupleDescriptor,
        (*slot).tts_values,
        (*slot).tts_isnull,
    )
}

/// Return a copy of slot contents as a heap tuple.
/// Returns null since custom slots don't support heap tuples.
#[pg_guard]
unsafe extern "C" fn custom_copy_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    let custom_slot = slot as *mut FdbTupleTableSlot;

    log!(
        "TTS({:p}): Copy heap tuple, value={:?}",
        slot,
        (*custom_slot).data.value
    );

    heap_form_tuple(
        (*slot).tts_tupleDescriptor,
        (*slot).tts_values,
        (*slot).tts_isnull,
    )
}
