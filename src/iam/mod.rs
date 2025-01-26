use std::ptr::addr_of_mut;

use pg_sys::{Datum, InvalidOid};
use pgrx::callconv::BoxRet;
use pgrx::pg_sys::IndexAmRoutine;
use pgrx::prelude::*;
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};

#[pg_extern(sql = "
    -- We need to use custom SQL to define our IAM handler function as Postgres requires the function signature
    -- to be: `(internal) -> index_am_handler`
    CREATE OR REPLACE FUNCTION pgfdb_iam_handler(internal)
    RETURNS table_am_handler AS 'MODULE_PATHNAME', $function$pgfdb_iam_handler_wrapper$function$
    LANGUAGE C STRICT;

    -- Create the corresponding index access method from the just-registered IAM handler
    CREATE ACCESS METHOD pgfdb TYPE INDEX HANDLER pgfdb_iam_handler;
    ")]
pub fn pgfdb_iam_handler() -> IndexAmHandler {
    IndexAmHandler
}

pub struct IndexAmHandler;

unsafe impl BoxRet for IndexAmHandler {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        fcinfo.return_raw_datum(Datum::from(addr_of_mut!(FDB_INDEX_AM_ROUTINE)))
    }
}

unsafe impl SqlTranslatable for IndexAmHandler {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::literal("index_am_handler"))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("index_am_handler")))
    }
}

// https://www.postgresql.org/docs/current/index-api.html
static mut FDB_INDEX_AM_ROUTINE: IndexAmRoutine = IndexAmRoutine {
    type_: pgrx::pg_sys::NodeTag::T_IndexAmRoutine,
    ambuild: todo!(),
    ambuildempty: todo!(),
    aminsert: todo!(),
    aminsertcleanup: todo!(),
    ambulkdelete: todo!(),
    amvacuumcleanup: todo!(),
    amcanreturn: todo!(),
    amcostestimate: todo!(),
    amoptions: todo!(),
    amproperty: todo!(),
    ambuildphasename: todo!(),
    amvalidate: todo!(),
    amadjustmembers: todo!(),
    ambeginscan: todo!(),
    amrescan: todo!(),
    amgettuple: todo!(),
    amendscan: todo!(),
    ammarkpos: todo!(),
    amrestrpos: todo!(),

    // Bitmap scans not supported
    amgetbitmap: None,
    // Parallel scans not supported
    amestimateparallelscan: None,
    aminitparallelscan: None,
    amparallelrescan: None,

    amstrategies: 0,
    amsupport: 0,
    amoptsprocnum: 0,
    amcanorder: true,
    amcanorderbyop: false,
    amcanbackward: true,
    amcanunique: true,
    amcanmulticol: true,
    amoptionalkey: false,
    amsearcharray: false,
    amsearchnulls: true,
    amstorage: false,
    amclusterable: false,
    ampredlocks: false,
    amcanparallel: false,
    amcanbuildparallel: false,
    amcaninclude: false,
    amusemaintenanceworkmem: false,
    amsummarizing: false,
    amparallelvacuumoptions: 0,
    // Variable type of data stored in index
    amkeytype: InvalidOid,
};
