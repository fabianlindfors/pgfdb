use std::boxed;
use std::sync::OnceLock;

use foundationdb::api::NetworkAutoStop;
use pgrx::prelude::*;

static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();
// static RUNTIME: OnceLock<Runtime> = OnceLock::new();
// static DATABASE: OnceLock<FdbDatabase> = OnceLock::new();
// static mut TRANSACTION: OnceLock<FdbTransaction> = OnceLock::new();

#[pg_guard]
pub(crate) fn init_database() {
    // if DATABASE.get().is_some() {
    //     return;
    // }

    let network = unsafe { foundationdb::boot() };
    let _ = NETWORK.set(network);

    eprintln!("Setting up FDB");

    // let temp_fdb_database = fdb::open_database("/usr/local/etc/foundationdb/fdb.cluster");
    // if let Err(err) = temp_fdb_database {
    //     eprintln!("FBD error: {:?}", err.code());
    // }
    // DATABASE.set(temp_fdb_database.clone()).unwrap();
}

// fn get_transaction() -> &'static FdbTransaction {
//     unsafe {
//         TRANSACTION.get_or_init(|| {
//             let database = DATABASE.get().unwrap();
//
//             let transaction = database.create_transaction().unwrap();
//             transaction
//         })
//     }
// }
//
// pub fn commit_transaction() {
//     let start = Instant::now();
//     unsafe {
//         if let Some(txn) = TRANSACTION.take() {
//             txn.commit().block_on().unwrap();
//
//             let elapsed = Instant::now() - start;
//             log!("Commited transaction to FDB in {elapsed:?}");
//         }
//     }
// }
//
// pub fn abort_transaction() {
//     unsafe {
//         if let Some(txn) = TRANSACTION.take() {
//             txn.cancel();
//
//             log!("Aborted FDB transaction");
//         }
//     }
// }
//
// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub enum Cell {
//     Bool(bool),
//     I8(i8),
//     I16(i16),
//     F32(f32),
//     I32(i32),
//     F64(f64),
//     I64(i64),
//     Numeric(AnyNumeric),
//     String(String),
//     Date(Date),
//     Timestamp(Timestamp),
// }
//
// impl Cell {
//     pub fn add_to_tuple(&self, tuple: &mut Tuple) {
//         match self {
//             Cell::Bool(v) => tuple.push_back(v.clone()),
//             Cell::I8(v) => tuple.push_back(v.clone()),
//             Cell::I16(v) => tuple.push_back(v.clone()),
//             Cell::F32(v) => tuple.push_back(v.clone()),
//             Cell::I32(v) => tuple.push_back(v.clone()),
//             Cell::F64(v) => tuple.push_back(v.clone()),
//             Cell::I64(v) => tuple.push_back(v.clone()),
//             Cell::Numeric(v) => panic!("Numeric not supported for tuples"),
//             Cell::String(v) => tuple.push_back(v.clone()),
//             Cell::Date(v) => panic!("Date cell not supported for tuples"),
//             Cell::Timestamp(v) => panic!("Timestamps not supported for tuples"),
//         }
//     }
// }
//
// impl IntoDatum for Cell {
//     fn into_datum(self) -> Option<Datum> {
//         match self {
//             Cell::Bool(v) => v.into_datum(),
//             Cell::I8(v) => v.into_datum(),
//             Cell::I16(v) => v.into_datum(),
//             Cell::F32(v) => v.into_datum(),
//             Cell::I32(v) => v.into_datum(),
//             Cell::F64(v) => v.into_datum(),
//             Cell::I64(v) => v.into_datum(),
//             Cell::Numeric(v) => v.into_datum(),
//             Cell::String(v) => v.into_datum(),
//             Cell::Date(v) => v.into_datum(),
//             Cell::Timestamp(v) => v.into_datum(),
//         }
//     }
//
//     fn type_oid() -> Oid {
//         Oid::INVALID
//     }
//
//     fn is_compatible_with(other: Oid) -> bool {
//         Self::type_oid() == other
//             || other == pg_sys::BOOLOID
//             || other == pg_sys::CHAROID
//             || other == pg_sys::INT2OID
//             || other == pg_sys::FLOAT4OID
//             || other == pg_sys::INT4OID
//             || other == pg_sys::FLOAT8OID
//             || other == pg_sys::INT8OID
//             || other == pg_sys::NUMERICOID
//             || other == pg_sys::TEXTOID
//             || other == pg_sys::DATEOID
//             || other == pg_sys::TIMESTAMPOID
//             || other == pg_sys::JSONBOID
//     }
// }
//
// impl FromDatum for Cell {
//     unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self>
//     where
//         Self: Sized,
//     {
//         if is_null {
//             return None;
//         }
//         let oid = PgOid::from(typoid);
//         match oid {
//             PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
//                 Some(Cell::Bool(bool::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
//                 Some(Cell::I8(i8::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
//                 Some(Cell::I16(i16::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
//                 Some(Cell::F32(f32::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
//                 Some(Cell::I32(i32::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
//                 Some(Cell::F64(f64::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
//                 Some(Cell::I64(i64::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
//                 Some(Cell::Numeric(AnyNumeric::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
//                 Some(Cell::String(String::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
//                 Some(Cell::Date(Date::from_datum(datum, false).unwrap()))
//             }
//             PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => Some(Cell::Timestamp(
//                 Timestamp::from_datum(datum, false).unwrap(),
//             )),
//             _ => None,
//         }
//     }
// }
//
// #[derive(Debug)]
// pub struct Row {
//     pub id: i32,
//     cells: Vec<Option<Cell>>,
//     relation: Relation,
// }
//
// #[derive(Serialize, Deserialize)]
// pub struct FdbRow {
//     pub id: i32,
//     cells: Vec<Option<Cell>>,
// }

// impl Row {
//     pub fn from_pg(relation: Relation, slot: *mut TupleTableSlot) -> Row {
//         let mut rng = rand::thread_rng();
//
//         let tuple_desc = unsafe { (*relation).rd_att };
//
//         let cells = unsafe {
//             let num_values: usize = (*slot).tts_nvalid.try_into().unwrap();
//
//             let attributes = (*tuple_desc).attrs.as_slice(num_values);
//             let values = from_raw_parts((*slot).tts_values, num_values);
//             let nulls = from_raw_parts((*slot).tts_isnull, num_values);
//
//             values
//                 .iter()
//                 .zip(attributes)
//                 .zip(nulls)
//                 .map(|((value, attribute), is_null)| {
//                     if *is_null {
//                         None
//                     } else {
//                         Some(
//                             Cell::from_polymorphic_datum(*value, false, attribute.atttypid)
//                                 .unwrap(),
//                         )
//                     }
//                 })
//                 .collect()
//         };
//
//         Row {
//             id: rng.gen_range(0..=i32::MAX),
//             cells,
//             relation,
//         }
//     }
//
//     pub fn from_pg_with_id(relation: Relation, slot: *mut TupleTableSlot, tid: ItemPointer) -> Row {
//         let tuple_desc = unsafe { (*relation).rd_att };
//
//         let cells = unsafe {
//             let num_values: usize = (*slot).tts_nvalid.try_into().unwrap();
//
//             let attributes = (*tuple_desc).attrs.as_slice(num_values);
//             let values = from_raw_parts((*slot).tts_values, num_values);
//             let nulls = from_raw_parts((*slot).tts_isnull, num_values);
//
//             values
//                 .iter()
//                 .zip(attributes)
//                 .zip(nulls)
//                 .map(|((value, attribute), is_null)| {
//                     if *is_null {
//                         None
//                     } else {
//                         Some(
//                             Cell::from_polymorphic_datum(*value, false, attribute.atttypid)
//                                 .unwrap(),
//                         )
//                     }
//                 })
//                 .collect()
//         };
//
//         let id = unsafe { item_pointer_get_block_number(tid).try_into().unwrap() };
//
//         Row {
//             id,
//             cells,
//             relation,
//         }
//     }
//
//     pub fn from_fdb(relation: Relation, value_raw: Value, b: &mut Benchmarker) -> Row {
//         // Deserialize cells from value
//         // let m = b.start_measure("reading");
//         let value_bytes: Bytes = value_raw.into();
//         // m.complete();
//
//         // let m = b.start_measure("cells");
//         let fdb_row: FdbRow = bincode::deserialize_from(&value_bytes[..]).unwrap();
//         // m.complete();
//
//         Row {
//             id: fdb_row.id,
//             cells: fdb_row.cells,
//             relation,
//         }
//     }
//
//     pub fn into_tuple_slot(self, slot: *mut TupleTableSlot) {
//         let n = self.cells.len();
//         for (i, cell) in self.cells.into_iter().enumerate() {
//             let (value_output, null_output) = unsafe {
//                 let values = from_raw_parts_mut((*slot).tts_values, n);
//                 let nulls = from_raw_parts_mut((*slot).tts_isnull, n);
//                 (values, nulls)
//             };
//
//             if let Some(cell) = cell {
//                 value_output[i] = cell.into_datum().unwrap();
//                 null_output[i] = false;
//             } else {
//                 null_output[i] = true;
//             }
//         }
//
//         unsafe {
//             item_pointer_set_all(&mut (*slot).tts_tid, self.id.try_into().unwrap(), 1);
//             // (*slot).tts_flags = 0;
//         }
//     }
//
//     pub fn fdb_key_tuple(&self) -> Tuple {
//         Self::fdb_key_tuple_for_row_id((*self).relation, self.id)
//     }
//
//     pub fn fdb_key_tuple_for_row_id(rel: Relation, id: i32) -> Tuple {
//         let mut tuple = Tuple::new();
//
//         tuple.push_back("table".to_string());
//
//         let table_oid = unsafe { (*rel).rd_id };
//         let table_oid_i32: i32 = table_oid.as_u32().try_into().unwrap();
//         tuple.push_back(table_oid_i32);
//
//         tuple.push_back(id);
//
//         tuple
//     }
//
//     pub fn serialized_value(&self) -> Bytes {
//         let mut bytes = BytesMut::new().writer();
//         let fdb_row = FdbRow {
//             id: self.id,
//             cells: self.cells.clone(),
//         };
//         bincode::serialize_into(&mut bytes, &fdb_row);
//         bytes.into_inner().freeze()
//     }
// }
//
// pub struct IndexRow {
//     pub id: i32,
//     cells: Vec<Option<Cell>>,
//     relation: Relation,
// }
//
// impl IndexRow {
//     pub fn from_pg(
//         relation: Relation,
//         tid: ItemPointer,
//         values: &[Datum],
//         nulls: &[bool],
//     ) -> IndexRow {
//         let tuple_desc = unsafe { (*relation).rd_att };
//
//         let cells = unsafe {
//             let attributes = (*tuple_desc).attrs.as_slice(values.len());
//
//             values
//                 .iter()
//                 .zip(attributes)
//                 .zip(nulls)
//                 .map(|((value, attribute), is_null)| {
//                     if *is_null {
//                         None
//                     } else {
//                         Some(
//                             Cell::from_polymorphic_datum(*value, false, attribute.atttypid)
//                                 .unwrap(),
//                         )
//                     }
//                 })
//                 .collect()
//         };
//
//         let id: i32 = unsafe { item_pointer_get_block_number(tid).try_into().unwrap() };
//
//         IndexRow {
//             id,
//             cells,
//             relation,
//         }
//     }
//
//     pub fn fdb_key_tuple(&self) -> Tuple {
//         let mut tuple = Tuple::new();
//
//         tuple.push_back("idx".to_string());
//
//         let index_oid = unsafe { (*self.relation).rd_id };
//         let index_oid_i32: i32 = index_oid.as_u32().try_into().unwrap();
//         tuple.push_back(index_oid_i32);
//
//         for cell in &self.cells {
//             if let Some(value) = cell {
//                 value.add_to_tuple(&mut tuple);
//             } else {
//                 tuple.push_back(Null);
//             }
//         }
//
//         tuple.push_back(self.id);
//
//         tuple
//     }
// }
//
// pub(crate) fn insert_row(row: &Row) {
//     let txn = get_transaction();
//
//     let key = row.fdb_key_tuple();
//     let value = row.serialized_value();
//     txn.set(key.pack(), value);
// }
//
// pub(crate) fn insert_index_row(row: &IndexRow) {
//     let txn = get_transaction();
//
//     let key = row.fdb_key_tuple();
//     txn.set(key.pack(), Bytes::new());
// }
//
// pub(crate) fn prepare_scan(relation: Relation) -> FdbStreamKeyValue {
//     let txn = get_transaction();
//
//     let mut tuple = Tuple::new();
//
//     tuple.push_back("table".to_string());
//
//     let table_oid = unsafe { (*relation).rd_id };
//     let table_oid_i32: i32 = table_oid.as_u32().try_into().unwrap();
//     tuple.push_back(table_oid_i32);
//     log!("Table oid {}", table_oid_i32);
//
//     let range = tuple.range(Bytes::new());
//     let mut range_options = RangeOptions::default();
//     range_options.set_mode(fdb::range::StreamingMode::WantAll);
//
//     range.into_stream(txn, range_options)
// }
//
// pub(crate) fn get_next_row(scan_desc: *mut ScanDesc, slot: *mut TupleTableSlot) -> Option<()> {
//     // let m1 = unsafe { (*scan_desc).benchmarker.start_measure("get_next_row") };
//
//     // let m = unsafe { (*scan_desc).benchmarker.start_measure("get_txn") };
//     let txn = get_transaction();
//     // m.complete();
//
//     // let m = unsafe { (*scan_desc).benchmarker.start_measure("get_stream") };
//     let stream = unsafe { &mut (*scan_desc).stream };
//     // m.complete();
//
//     // let m = unsafe { (*scan_desc).benchmarker.start_measure("fdb") };
//     let raw = stream.next().block_on();
//     // m.complete();
//
//     // let m_deser = unsafe { (*scan_desc).benchmarker.start_measure("deser") };
//     let res = raw.map(|raw_row| {
//         //let m2 = unsafe { (*scan_desc).benchmarker.start_measure("deser_inner") };
//
//         let relation = unsafe { (*scan_desc).rs_base.rs_rd };
//
//         let b = unsafe { &mut (*scan_desc).benchmarker };
//
//         // let m3 = unsafe { (*scan_desc).benchmarker.start_measure("row_value") };
//         let value = raw_row.unwrap().into_value();
//         // m3.complete();
//
//         let row = Row::from_fdb(relation, value, b);
//
//         let res = row.into_tuple_slot(slot);
//         // m2.complete();
//         res
//     });
//     // m_deser.complete();
//
//     // m1.complete();
//
//     res
// }
//
// pub(crate) fn get_row_by_id(rel: Relation, id: i32, slot: *mut TupleTableSlot) -> Option<()> {
//     let txn = get_transaction();
//
//     let key = Row::fdb_key_tuple_for_row_id(rel, id);
//     let value = txn.get(key.pack()).block_on().unwrap().unwrap();
//
//     let mut b = Benchmarker::new();
//     let row = Row::from_fdb(rel, value, &mut b);
//     row.into_tuple_slot(slot);
//
//     Some(())
// }
//
// pub(crate) fn delete_row_by_id(rel: Relation, id: i32) -> Option<()> {
//     let txn = get_transaction();
//
//     let key = Row::fdb_key_tuple_for_row_id(rel, id);
//     txn.clear(key.pack());
//
//     Some(())
// }
//
// pub(crate) fn get_num_rows_in_table(relation: Relation) -> u64 {
//     let txn = get_transaction();
//
//     let snapshot = txn.snapshot();
//
//     let mut tuple = Tuple::new();
//
//     tuple.push_back("table".to_string());
//
//     let table_oid = unsafe { (*relation).rd_id };
//     let table_oid_i32: i32 = table_oid.as_u32().try_into().unwrap();
//     tuple.push_back(table_oid_i32);
//
//     let range = tuple.range(Bytes::new());
//     let mut range_options = RangeOptions::default();
//     // range_options.set_mode(fdb::range::StreamingMode::WantAll);
//
//     let mut stream = range.into_stream(&snapshot, range_options);
//
//     let mut i = 0;
//     while let Some(_) = stream.next().block_on() {
//         i += 1;
//     }
//
//     i
// }
//
// pub(crate) fn get_size_estimate_for_table(relation: Relation) -> i64 {
//     let txn = get_transaction();
//
//     let snapshot = txn.snapshot();
//
//     let mut tuple = Tuple::new();
//
//     tuple.push_back("table".to_string());
//
//     let table_oid = unsafe { (*relation).rd_id };
//     let table_oid_i32: i32 = table_oid.as_u32().try_into().unwrap();
//     tuple.push_back(table_oid_i32);
//
//     let range = tuple.range(Bytes::new());
//
//     txn.get_estimated_range_size_bytes(range)
//         .block_on()
//         .unwrap()
// }
//
