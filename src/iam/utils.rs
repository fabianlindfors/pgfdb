use foundationdb::tuple::Element;
use foundationdb::tuple::Uuid;
use pg_sys::{Datum, Oid};
use pgrx::prelude::*;
use std::borrow::Cow;

// Helper function to encode a Postgres datum into an FDB tuple element
// This function will need to be implemented to handle different Postgres types
pub fn encode_datum_for_index<'a>(datum: Datum, type_oid: Oid) -> Element<'a> {
    match type_oid {
        // INT4/INTEGER (OID 23)
        pg_sys::INT4OID => {
            // Convert the datum to a Rust i32
            let value = unsafe { pg_sys::DatumGetInt64(datum) };
            Element::Int(value)
        }
        // SMALLINT (OID 21)
        pg_sys::INT2OID => {
            // Convert the datum to a Rust i16, then to i64 for storage
            let value = unsafe { pg_sys::DatumGetInt16(datum) as i64 };
            Element::Int(value)
        }
        // BIGINT (OID 20)
        pg_sys::INT8OID => {
            // Convert the datum to a Rust i64
            let value = unsafe { pg_sys::DatumGetInt64(datum) };
            Element::Int(value)
        }
        // TEXT (OID 25) or VARCHAR (OID 1043)
        pg_sys::VARCHAROID | pg_sys::TEXTOID => {
            // Use pgrx's text_to_rust_str_unchecked to convert to a Rust string
            let varlena: PgVarlena<()> = unsafe { PgVarlena::from_datum(datum) };
            let text = unsafe { pgrx::text_to_rust_str_unchecked(varlena.into_pg()).to_string() };
            Element::String(Cow::Owned(text))
        }
        // REAL/FLOAT4 (OID 700)
        pg_sys::FLOAT4OID => {
            // Convert the datum to a Rust f32, then to f64 for storage in FDB
            let value = unsafe { pg_sys::DatumGetFloat4(datum) as f64 };
            Element::Double(value)
        }
        // DOUBLE PRECISION/FLOAT8 (OID 701)
        pg_sys::FLOAT8OID => {
            // Convert the datum to a Rust f64 for storage in FDB
            let value = unsafe { pg_sys::DatumGetFloat8(datum) };
            Element::Double(value)
        }
        // UUID (OID 2950)
        pg_sys::UUIDOID => {
            let uuid: pgrx::Uuid = unsafe { pgrx::Uuid::from_datum(datum, false).unwrap() };
            Element::Uuid(Uuid::from_bytes(uuid.as_bytes().clone()))
        }
        // Add more types as needed
        _ => {
            // Log unsupported types
            panic!(
                "IAM: encode_datum_for_index not yet implemented for type OID: {}",
                type_oid.as_u32()
            );
        }
    }
}
