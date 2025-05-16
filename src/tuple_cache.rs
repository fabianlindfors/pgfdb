use std::cell::RefCell;

use crate::coding::Tuple;

struct TupleCache {
    value: Option<(u32, Tuple)>,
}

static mut TUPLE_CACHE: RefCell<TupleCache> = RefCell::new(TupleCache { value: None });

pub fn get_with_id(id: u32) -> Option<Tuple> {
    #[allow(static_mut_refs)]
    let cache = unsafe { TUPLE_CACHE.borrow() };

    let Some((stored_id, _)) = &cache.value else {
        return None;
    };

    if stored_id != &id {
        return None;
    }

    std::mem::drop(cache);

    unsafe {
        #[allow(static_mut_refs)]
        TUPLE_CACHE
            .replace(TupleCache { value: None })
            .value
            .map(|(_, tuple)| tuple)
    }
}

pub fn populate(tuple: Tuple) {
    #[allow(static_mut_refs)]
    let mut cache = unsafe { TUPLE_CACHE.borrow_mut() };
    cache.value = Some((tuple.id, tuple));
}

pub fn clear() {
    #[allow(static_mut_refs)]
    let mut cache = unsafe { TUPLE_CACHE.borrow_mut() };
    cache.value = None;
}
