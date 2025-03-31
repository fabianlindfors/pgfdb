use pgrx::pg_sys::TupleTableSlot;
use std::cell::RefCell;

struct TupleCache {
    id: Option<u32>,
    tuple: *mut TupleTableSlot,
}

static mut TUPLE_CACHE: RefCell<TupleCache> = RefCell::new(TupleCache {
    id: None,
    tuple: std::ptr::null_mut(),
});

pub fn get_with_id(id: u32) -> Option<(u32, *mut TupleTableSlot)> {
    #[allow(static_mut_refs)]
    let cache = unsafe { TUPLE_CACHE.borrow() };

    let Some(stored_id) = cache.id else {
        return None;
    };

    if stored_id != id {
        return None;
    }

    return Some((stored_id, cache.tuple));
}

pub fn populate(id: u32, tuple: *mut TupleTableSlot) {
    #[allow(static_mut_refs)]
    let mut cache = unsafe { TUPLE_CACHE.borrow_mut() };
    cache.id = Some(id);
    cache.tuple = tuple;
}

pub fn clear() {
    #[allow(static_mut_refs)]
    let mut cache = unsafe { TUPLE_CACHE.borrow_mut() };
    cache.id = None;
    cache.tuple = std::ptr::null_mut();
}
