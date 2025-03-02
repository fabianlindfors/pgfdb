mod build;
mod scan;
mod utils;

use pg_sys::{bytea, Datum, IndexAmRoutine, InvalidOid};
use pgrx::callconv::BoxRet;
use pgrx::prelude::*;
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use std::ptr;

#[pg_extern(sql = "
    -- We need to use custom SQL to define our IAM handler function as Postgres requires the function signature
    -- to be: `(internal) -> index_am_handler`
    CREATE OR REPLACE FUNCTION pgfdb_iam_handler(internal)
    RETURNS index_am_handler AS 'MODULE_PATHNAME', $function$pgfdb_iam_handler_wrapper$function$
    LANGUAGE C STRICT;

    -- Create the corresponding index access method from the just-registered IAM handler
    CREATE ACCESS METHOD pgfdb_idx TYPE INDEX HANDLER pgfdb_iam_handler;

    -- Operator classes
    CREATE OPERATOR CLASS pgfdb_idx_integer 
    DEFAULT FOR TYPE INTEGER USING pgfdb_idx AS
    OPERATOR 1 = (INTEGER, INTEGER),
    OPERATOR 2 > (INTEGER, INTEGER),
    OPERATOR 3 >= (INTEGER, INTEGER);
    
    CREATE OPERATOR CLASS pgfdb_idx_text
    DEFAULT FOR TYPE TEXT USING pgfdb_idx AS
    OPERATOR 1 = (TEXT, TEXT);
    ")]
pub fn pgfdb_iam_handler() -> IndexAmHandler {
    IndexAmHandler
}

// https://www.postgresql.org/docs/current/index-api.html
// Index build function - Called when CREATE INDEX is executed

unsafe extern "C" fn amoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    // Null for default behaviour
    // We don't support any options on the index yet
    ptr::null_mut()
}

pub struct IndexAmHandler;

unsafe impl BoxRet for IndexAmHandler {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        // An IAM must be returned as a palloced struct, as opposed to a TAM which can be statically allocated
        let mut index_am_routine =
            unsafe { PgBox::<IndexAmRoutine>::alloc_node(pgrx::pg_sys::NodeTag::T_IndexAmRoutine) };

        index_am_routine.ambuild = Some(build::ambuild);
        index_am_routine.ambuildempty = Some(build::ambuildempty);
        index_am_routine.aminsert = Some(build::aminsert);
        index_am_routine.aminsertcleanup = None; // Not needed
        index_am_routine.ambulkdelete = None; // Optional - for bulk deletes
        index_am_routine.amvacuumcleanup = None; // Optional - for VACUUM
        index_am_routine.amcanreturn = None; // Optional - index-only scans
        index_am_routine.amcostestimate = Some(scan::amcostestimate); // Optional - custom cost estimation
        index_am_routine.amoptions = Some(amoptions);
        index_am_routine.amproperty = None; // Optional - index properties
        index_am_routine.ambuildphasename = None; // Optional - progress reporting
        index_am_routine.amvalidate = None; // Optional - index validation
        index_am_routine.amadjustmembers = None; // Optional - parallel scan
        index_am_routine.ambeginscan = Some(scan::ambeginscan);
        index_am_routine.amrescan = Some(scan::amrescan);
        index_am_routine.amgettuple = Some(scan::amgettuple);
        index_am_routine.amendscan = Some(scan::amendscan);
        index_am_routine.ammarkpos = None; // Optional - mark/restore position
        index_am_routine.amrestrpos = None; // Optional - mark/restore position

        // Bitmap scans not supported
        index_am_routine.amgetbitmap = None;
        // Parallel scans not supported
        index_am_routine.amestimateparallelscan = None;
        index_am_routine.aminitparallelscan = None;
        index_am_routine.amparallelrescan = None;

        // Strategies:
        // 1: =
        // 2: >
        // 3: >=
        index_am_routine.amstrategies = 3;

        index_am_routine.amsupport = 0;
        index_am_routine.amoptsprocnum = 0;
        index_am_routine.amcanorder = true;
        index_am_routine.amcanorderbyop = false;
        index_am_routine.amcanbackward = true;
        index_am_routine.amcanunique = true;
        index_am_routine.amcanmulticol = true;
        index_am_routine.amoptionalkey = false;
        index_am_routine.amsearcharray = false;
        index_am_routine.amsearchnulls = true;
        index_am_routine.amstorage = false;
        index_am_routine.amclusterable = false;
        index_am_routine.ampredlocks = false;
        index_am_routine.amcanparallel = false;
        index_am_routine.amcanbuildparallel = false;
        index_am_routine.amcaninclude = false;
        index_am_routine.amusemaintenanceworkmem = false;
        index_am_routine.amsummarizing = false;
        index_am_routine.amparallelvacuumoptions = 0;
        // Variable type of data stored in index
        index_am_routine.amkeytype = InvalidOid;

        fcinfo.return_raw_datum(index_am_routine.into_datum().unwrap())
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
