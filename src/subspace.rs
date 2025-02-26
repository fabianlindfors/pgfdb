use foundationdb::tuple::{pack, Subspace};
use pgrx::pg_sys::Oid;

pub fn table(oid: Oid) -> Subspace {
    let prefix = pack(&("tables", oid.as_u32()));
    Subspace::from_bytes(prefix)
}

pub fn index(oid: Oid) -> Subspace {
    let prefix = pack(&("indexes", oid.as_u32()));
    Subspace::from_bytes(prefix)
}
