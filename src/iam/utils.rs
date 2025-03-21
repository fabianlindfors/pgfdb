use std::borrow::Cow;
use pg_sys::{Datum, Oid};
use pgrx::prelude::*;
use foundationdb::tuple::Element;

// Helper function to encode a Postgres datum into an FDB tuple element
// This function will need to be implemented to handle different Postgres types
pub fn encode_datum_for_index<'a>(
    datum: Datum,
    type_oid: Oid,
) -> Option<Element<'a>> {
    match type_oid {
        // INT4/INTEGER (OID 23)
        pg_sys::INT4OID => {
            // Convert the datum to a Rust i32
            let value = unsafe { pg_sys::DatumGetInt64(datum) };
            Some(Element::Int(value))
        }
        // SMALLINT (OID 21)
        pg_sys::INT2OID => {
            // Convert the datum to a Rust i16, then to i64 for storage
            let value = unsafe { pg_sys::DatumGetInt16(datum) as i64 };
            Some(Element::Int(value))
        },
        // BIGINT (OID 20)
        pg_sys::INT8OID => {
            // Convert the datum to a Rust i64
            let value = unsafe { pg_sys::DatumGetInt64(datum) };
            Some(Element::Int(value))
        }
        // TEXT (OID 25) or VARCHAR (OID 1043)
        pg_sys::VARCHAROID | pg_sys::TEXTOID => {
            // Use pgrx's text_to_rust_str_unchecked to convert to a Rust string
            let varlena: PgVarlena<()> = unsafe { PgVarlena::from_datum(datum) };
            let text = unsafe { pgrx::text_to_rust_str_unchecked(varlena.into_pg()).to_string() };
            Some(Element::String(Cow::Owned(text)))
        }
        // REAL/FLOAT4 (OID 700)
        pg_sys::FLOAT4OID => {
            // Convert the datum to a Rust f32, then to f64 for storage in FDB
            let value = unsafe { pg_sys::DatumGetFloat4(datum) as f64 };
            Some(Element::Double(value))
        },
        // Add more types as needed
        _ => {
            // Log unsupported types
            log!(
                "IAM: encode_datum_for_index not yet implemented for type OID: {}",
                type_oid.as_u32()
            );
            None
        }
    }
}
