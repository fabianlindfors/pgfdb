use std::{mem::MaybeUninit, ptr, slice::from_raw_parts};

use pgrx::{
    ffi::c_char,
    itemptr::item_pointer_set_all,
    pg_sys::{
        fmgr_info, getTypeBinaryInputInfo, getTypeBinaryOutputInfo, Datum, ExecClearTuple,
        ExecStoreVirtualTuple, FmgrInfo, Oid, ReceiveFunctionCall, SendFunctionCall,
        StringInfoData, TupleTableSlot,
    },
    varlena_to_byte_slice,
};
use serde::{Deserialize, Serialize};

// This struct can proabably be optimized by using zero-copy serde
#[derive(Serialize, Deserialize, Debug)]
pub struct Tuple {
    pub id: u32,
    pub datums: Vec<Option<Vec<u8>>>,
}

// Serialization and deserialization
impl Tuple {
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(100);

        // ID: 4 bytes little endian integer
        let id_encoded = self.id.to_le_bytes();
        bytes.extend_from_slice(&id_encoded);

        // Number of attributes: 8 bytes little endian integer
        let num_datums_encoded = self.datums.len().to_le_bytes();
        bytes.extend_from_slice(&num_datums_encoded);

        for maybe_datum in &self.datums {
            // Number of bytes in datum: 8 bytes little endian integer
            // 0 represents null datum
            let Some(datum) = maybe_datum else {
                let zero = (0 as usize).to_le_bytes();
                bytes.extend_from_slice(&zero);
                continue;
            };

            let datum_len = datum.len().to_le_bytes();
            bytes.extend_from_slice(&datum_len);

            // Binary encoded datum
            bytes.extend_from_slice(datum);
        }

        bytes
    }

    pub fn deserialize(encoded: &[u8]) -> Tuple {
        let id: u32 = u32::from_le_bytes(encoded[0..4].try_into().unwrap());

        let num_datums = usize::from_le_bytes(encoded[4..12].try_into().unwrap());

        let mut datums = Vec::with_capacity(num_datums);

        let mut i = 12;
        while i < encoded.len() {
            let datum_len = usize::from_le_bytes(encoded[i..i + 8].try_into().unwrap());
            if datum_len == 0 {
                datums.push(None);
            } else {
                let datum = encoded[i + 8..i + 8 + datum_len].to_vec();
                datums.push(Some(datum));
            }
            i = i + 8 + datum_len
        }

        Tuple { id, datums }
    }
}

impl Tuple {
    pub fn from_tts(id: u32, tts: &TupleTableSlot) -> Tuple {
        let tupledesc = tts.tts_tupleDescriptor;
        let attrs = unsafe {
            (*tupledesc)
                .attrs
                .as_slice(tts.tts_nvalid.try_into().unwrap())
        };

        let mut tuple = Tuple {
            id,
            datums: Vec::with_capacity(tts.tts_nvalid as usize),
        };

        let nulls = unsafe { from_raw_parts(tts.tts_isnull, tts.tts_nvalid as usize) };
        let datums = unsafe { from_raw_parts(tts.tts_values, tts.tts_nvalid as usize) };

        for i in 0..(tts.tts_nvalid as usize) {
            if nulls[i] {
                tuple.datums.push(None);
            } else {
                tuple
                    .datums
                    .push(Some(encode_datum(&datums[i], attrs[i].atttypid)));
            }
        }

        tuple
    }

    pub fn load_into_tts(self, tts: &mut TupleTableSlot) {
        // Ensure the decoded tuple has the same number of attributes as we expect
        let num_atts = unsafe { (*tts.tts_tupleDescriptor).natts as usize };
        assert_eq!(self.datums.len(), num_atts);

        // The procedure for populating a virtual TTS is explained like in the Postgres source:
        //  1. Call ExecClearTuple to mark the slot empty
        //  2. Store data into the Datum/isnull arrays
        //  3. Call ExecStoreVirtualTuple to mark the slot valid
        unsafe { ExecClearTuple(tts) };

        // Save TID from Tuple
        item_pointer_set_all(&mut tts.tts_tid, self.id, 1);

        // Store decoded values and nulls into TTS
        let attrs = unsafe { (*tts.tts_tupleDescriptor).attrs.as_slice(num_atts) };
        for (i, maybe_encoded_datum) in self.datums.into_iter().enumerate() {
            let Some(mut encoded_datum) = maybe_encoded_datum else {
                unsafe {
                    *tts.tts_isnull.offset(i as isize) = true;
                    *tts.tts_values.offset(i as isize) = Datum::null();
                }
                continue;
            };

            // Decode datum and store it on the TTS
            let datum = decode_datum(&mut encoded_datum, attrs[i].atttypid);
            unsafe {
                *tts.tts_isnull.offset(i as isize) = false;
                *tts.tts_values.offset(i as isize) = datum;
            }
        }

        // Mark the TTS as valid and populated
        unsafe {
            ExecStoreVirtualTuple(tts);
        }
    }
}

fn encode_datum(datum: &Datum, type_oid: Oid) -> Vec<u8> {
    // Get the binary serialisation function oid for the datum type
    let mut function_oid = MaybeUninit::<Oid>::uninit();
    let mut is_varlena = MaybeUninit::<bool>::uninit();
    unsafe {
        getTypeBinaryOutputInfo(type_oid, function_oid.as_mut_ptr(), is_varlena.as_mut_ptr())
    };

    // Get details on the callback function based on its oid
    let mut fmgr = MaybeUninit::<FmgrInfo>::uninit();
    unsafe { fmgr_info(function_oid.assume_init_read(), fmgr.as_mut_ptr()) }

    // Serialise the datum into its binary format and save it to the tuple
    let encoded_pg = unsafe { SendFunctionCall(fmgr.as_mut_ptr(), ptr::read(datum)) };
    let encoded = unsafe { varlena_to_byte_slice(encoded_pg) };

    encoded.to_vec()
}

pub fn decode_datum(encoded_datum: &mut [u8], type_oid: Oid) -> Datum {
    // Get the binary deserialization function oid for the datum type
    let mut function_oid = MaybeUninit::<Oid>::uninit();
    let mut io_param = MaybeUninit::<Oid>::uninit();
    unsafe { getTypeBinaryInputInfo(type_oid, function_oid.as_mut_ptr(), io_param.as_mut_ptr()) };

    // Get details on the callback function based on its oid
    let mut fmgr = MaybeUninit::<FmgrInfo>::uninit();
    unsafe { fmgr_info(function_oid.assume_init_read(), fmgr.as_mut_ptr()) }

    let mut string_info = StringInfoData {
        data: encoded_datum.as_mut_ptr() as *mut c_char,
        len: encoded_datum.len() as i32,
        maxlen: encoded_datum.len() as i32,
        cursor: 0,
    };

    let datum = unsafe {
        ReceiveFunctionCall(
            fmgr.as_mut_ptr(),
            ptr::from_mut(&mut string_info),
            io_param.assume_init(),
            -1,
        )
    };

    return datum;
}
