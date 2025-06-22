// KVRecord / KVWriteRecord are separate from the WAL record because
// we should be able to reuse this serialisation in the SSTables.
//
//
// A KVRecord consists of a header and then the key and value data
// verbatim. The header contains a magic byte and version byte. The
// version byte can be later used to support different versions, eg,
// if we add a checksum. Then there is the length of the key and the
// value, which are used to read the key and value respectively.
//
// 0          1            2            4              8            9
// | u8 magic | u8 version | u16 keylen | u32 valuelen | u8 deleted | key | value |
//   --------------------------------------------------------------   ---   -----
//             KV header                                               |      |
//                                                            keylen bytes    |
//                                                                            |
//                                                                 valuelen bytes
//
// Versions:
// 1: initial version, magic, version, keylen, valuelen, key, value
// 2: added deleted

use std::io::{Error, Read};

const KV_MAGIC: u8 = b'k';
const KV_VERSION: u8 = 2;
const KV_DELETED: u8 = 1;
const KV_LIVE: u8 = 0;

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) enum KVValue {
    Deleted,
    Some(Vec<u8>),
}

#[derive(PartialEq, Debug, Clone)]
/// Read-optimised KVRecord. It uses Vec<u8> for data to
/// allow the structure to own the data.
pub(crate) struct KVRecord {
    // magic: u8,
    // keylen: u16,
    // valuelen: u32,
    pub(crate) key: Vec<u8>,
    pub(crate) value: KVValue,
}
impl KVRecord {
    /// Attempt to read a KVRecord from a Read stream.
    pub(crate) fn read_one<T: Read>(
        r: &mut T,
    ) -> Result<Option<KVRecord>, Error> {
        let mut header = [0u8; 9];
        r.read_exact(&mut header)?;

        // This might be clearer using byteorder and a reader
        assert_eq!(header[0], KV_MAGIC, "Unexpected magic byte");
        assert_eq!(header[1], KV_VERSION, "Unexpected version byte");
        let keylen = u16::from_be_bytes(header[2..4].try_into().unwrap());
        let valuelen = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let deleted = header[8] != 0;

        if deleted {
            assert_eq!(valuelen, 0);
        }

        let mut key = Vec::with_capacity(keylen as usize);
        r.by_ref().take(keylen as u64).read_to_end(&mut key)?;

        let kv = KVRecord {
            key,
            value: if deleted {
                KVValue::Deleted
            } else {
                let mut value = Vec::with_capacity(valuelen as usize);
                r.by_ref().take(valuelen as u64).read_to_end(&mut value)?;
                KVValue::Some(value)
            },
        };

        Ok(Some(kv))
    }
}

impl<'a> From<&'a KVValue> for KVWriteValue<'a> {
    fn from(value: &'a KVValue) -> Self {
        match value {
            KVValue::Deleted => KVWriteValue::Deleted,
            KVValue::Some(v) => KVWriteValue::Some(&v),
        }
    }
}

#[derive(Debug)]
/// Write-optimised version of KVRecord. It uses slices for
/// data to avoid copying.
pub(crate) enum KVWriteValue<'a> {
    Deleted,
    Some(&'a [u8]),
}

#[derive(Debug)]
/// Write-optimised version of KVRecord. It uses slices for
/// data to avoid copying.
pub(crate) struct KVWriteRecord<'a> {
    // magic: u8,
    // keylen: u16,
    // valuelen: u32,
    pub(crate) key: &'a [u8],
    pub(crate) value: KVWriteValue<'a>,
}
impl<'a> KVWriteRecord<'a> {
    /// Serialise the KVWriteRecord to a buffer.
    ///
    /// We choose to produce a buffer here rather than use a write_one
    /// approach to allow KVRecord data to be embedded into other
    /// serialised formats more easily.
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        buf.push(KV_MAGIC);
        buf.push(KV_VERSION);
        buf.extend((self.key.len() as u16).to_be_bytes());

        match self.value {
            KVWriteValue::Deleted => {
                buf.extend((0 as u32).to_be_bytes()); // no value
                buf.push(KV_DELETED);
                buf.extend(self.key);
            }
            KVWriteValue::Some(v) => {
                buf.extend((v.len() as u32).to_be_bytes());
                buf.push(KV_LIVE);
                buf.extend(self.key);
                buf.extend(v);
            }
        }
        buf
    }
}
