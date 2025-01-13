use pgrx::{
    pg_guard,
    pg_sys::{Datum, HeapTuple, MinimalTuple, TupleTableSlot, TupleTableSlotOps, TTS_FLAG_EMPTY},
};

#[repr(C)]
pub struct CustomSlot {
    base: TupleTableSlot,
    custom_data: *mut CustomData,
}

pub struct CustomData {
    // Add your custom data fields here
    values: Vec<Datum>,
    nulls: Vec<bool>,
}

// More details in https://github.com/postgres/postgres/blob/master/src/include/executor/tuptable.h
pub static CUSTOM_SLOT_OPS: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: std::mem::size_of::<CustomSlot>(),
    init: Some(custom_init),
    release: Some(custom_release),
    clear: Some(custom_clear),
    getsomeattrs: Some(custom_getsomeattrs),
    getsysattr: Some(custom_getsysattr),
    materialize: Some(custom_materialize),
    copyslot: Some(custom_copyslot),
    is_current_xact_tuple: Some(custom_is_current_xact_tuple),
    get_heap_tuple: Some(custom_get_heap_tuple),
    copy_heap_tuple: Some(custom_copy_heap_tuple),
    get_minimal_tuple: Some(custom_get_minimal_tuple),
    copy_minimal_tuple: Some(custom_copy_minimal_tuple),
};

/// Initialize a newly created slot. Sets up initial state and allocates custom data.
/// The slot starts empty (TTS_FLAG_EMPTY) with no valid values.
#[pg_guard]
unsafe extern "C" fn custom_init(slot: *mut TupleTableSlot) {
    (*slot).tts_flags = TTS_FLAG_EMPTY as u16;
    (*slot).tts_nvalid = 0;
    (*slot).tts_values = std::ptr::null_mut();
    (*slot).tts_isnull = std::ptr::null_mut();

    let custom_slot = slot as *mut CustomSlot;
    (*custom_slot).custom_data = Box::into_raw(Box::new(CustomData {
        values: Vec::new(),
        nulls: Vec::new(),
    }));
}

/// Clean up and free resources when slot is destroyed.
/// Responsible for freeing the custom_data but not the slot itself.
#[pg_guard]
unsafe extern "C" fn custom_release(slot: *mut TupleTableSlot) {
    let custom_slot = slot as *mut CustomSlot;
    if !(*custom_slot).custom_data.is_null() {
        drop(Box::from_raw((*custom_slot).custom_data));
    }
}

/// Clear the contents of the slot but keep the tuple descriptor.
/// Sets the slot to empty state and resets the number of valid values.
#[pg_guard]
unsafe extern "C" fn custom_clear(slot: *mut TupleTableSlot) {
    (*slot).tts_flags = TTS_FLAG_EMPTY as u16;
    (*slot).tts_nvalid = 0;
}

/// Fill the slot's tts_values/tts_isnull arrays for the first natts attributes.
/// May be called with natts > number of available attributes.
/// Must set tts_nvalid to actual number of valid values returned.
#[pg_guard]
unsafe extern "C" fn custom_getsomeattrs(slot: *mut TupleTableSlot, natts: i32) {
    // Implement attribute loading logic here
}

/// Get a system attribute value as a Datum and set isnull flag.
/// Should error if slot type doesn't support system attributes.
/// Currently returns null for all system attributes.
#[pg_guard]
unsafe extern "C" fn custom_getsysattr(
    slot: *mut TupleTableSlot,
    attnum: i32,
    isnull: *mut bool,
) -> Datum {
    *isnull = true;
    Datum::from(0)
}

/// Make slot contents independent of external resources.
/// After this call, slot should not depend on buffers, memory contexts etc.
/// No-op for this implementation since data is already self-contained.
#[pg_guard]
unsafe extern "C" fn custom_materialize(slot: *mut TupleTableSlot) {}

/// Copy source slot's contents into destination slot's context.
/// Slots must have same number of attributes.
/// Currently only copies flags and number of valid values.
#[pg_guard]
unsafe extern "C" fn custom_copyslot(dst: *mut TupleTableSlot, src: *mut TupleTableSlot) {
    (*dst).tts_flags = (*src).tts_flags;
    (*dst).tts_nvalid = (*src).tts_nvalid;
}

/// Get the value and null flag for a specific attribute by number.
/// Attribute numbers are 1-based. Returns 0/null for out of range attributes.
#[pg_guard]
unsafe extern "C" fn custom_getattr(
    slot: *mut TupleTableSlot,
    attnum: i32,
    isnull: *mut bool,
) -> Datum {
    let custom_slot = slot as *mut CustomSlot;
    let custom_data = &*(*custom_slot).custom_data;

    let idx = (attnum - 1) as usize;
    if idx < custom_data.values.len() {
        *isnull = custom_data.nulls[idx];
        custom_data.values[idx]
    } else {
        *isnull = true;
        Datum::from(0)
    }
}

/// Get the null flag for a specific attribute by number.
/// Returns true for out of range attributes.
#[pg_guard]
unsafe extern "C" fn custom_get_isnull(slot: *mut TupleTableSlot, attnum: i32) -> bool {
    let custom_slot = slot as *mut CustomSlot;
    let custom_data = &*(*custom_slot).custom_data;

    let idx = (attnum - 1) as usize;
    custom_data.nulls.get(idx).copied().unwrap_or(true)
}

/// Get direct access to the slot's values array.
/// Used for bulk access to attribute values.
#[pg_guard]
unsafe extern "C" fn custom_get_values(slot: *mut TupleTableSlot) -> *mut Datum {
    (*slot).tts_values
}

/// Check if tuple was created by current transaction.
/// Returns false since custom slots don't track transaction visibility.
#[pg_guard]
unsafe extern "C" fn custom_is_current_xact_tuple(slot: *mut TupleTableSlot) -> bool {
    // For custom slots, we typically return false as we don't track transaction visibility
    false
}

/// Return a heap tuple "owned" by the slot.
/// Returns null since custom slots don't store heap tuples.
#[pg_guard]
unsafe extern "C" fn custom_get_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    // We don't store heap tuples directly in our custom slot
    std::ptr::null_mut()
}

/// Return a minimal tuple "owned" by the slot.
/// Returns null since custom slots don't store minimal tuples.
#[pg_guard]
unsafe extern "C" fn custom_get_minimal_tuple(slot: *mut TupleTableSlot) -> MinimalTuple {
    // We don't store minimal tuples in our custom slot
    std::ptr::null_mut()
}

/// Return a copy of slot contents as a minimal tuple.
/// Returns null since custom slots don't support minimal tuples.
#[pg_guard]
unsafe extern "C" fn custom_copy_minimal_tuple(slot: *mut TupleTableSlot) -> MinimalTuple {
    // We don't support minimal tuples in our custom slot
    std::ptr::null_mut()
}

/// Return a copy of slot contents as a heap tuple.
/// Returns null since custom slots don't support heap tuples.
#[pg_guard]
unsafe extern "C" fn custom_copy_heap_tuple(slot: *mut TupleTableSlot) -> HeapTuple {
    // We don't support heap tuples in our custom slot
    std::ptr::null_mut()
}
