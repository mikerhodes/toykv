// TODO KVRecord / KVWriteRecord are separate from the WAL record because
// we should be able to reuse this serialisation in the SSTables.

use std::io::{Error, Read};

const KV_MAGIC: u8 = b'k';

#[derive(Debug)]
/// Read-optimised KVRecord. It uses Vec<u8> for data to
/// allow the structure to own the data.
pub(crate) struct KVRecord {
    // magic: u8,
    // keylen: u16,
    // valuelen: u32,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}
impl KVRecord {
    /// Attempt to read a KVRecord from a Read stream.
    pub(crate) fn read_one<T: Read>(r: &mut T) -> Result<Option<KVRecord>, Error> {
        let mut header = [0u8; 7];
        let n = r.read(&mut header)?;
        if n < 7 {
            // Is this really only Ok if we read zero?
            // 0 < n < 7 probably actually means a corrupt file.
            return Ok(None);
        }

        // This might be clearer using byteorder and a reader
        let magic = header[0];
        assert_eq!(magic, KV_MAGIC, "Unexpected magic byte");
        let keylen = u16::from_be_bytes(header[1..3].try_into().unwrap());
        let valuelen = u32::from_be_bytes(header[3..7].try_into().unwrap());

        let mut key = Vec::with_capacity(keylen as usize);
        r.by_ref().take(keylen as u64).read_to_end(&mut key)?;
        let mut value = Vec::with_capacity(valuelen as usize);
        r.by_ref().take(valuelen as u64).read_to_end(&mut value)?;

        let kv = KVRecord { key, value };

        Ok(Some(kv))
    }
}

#[derive(Debug)]
/// Write-optimised version of KVRecord. It uses slices for
/// data to avoid copying.
pub(crate) struct KVWriteRecord<'a> {
    // magic: u8,
    // keylen: u16,
    // valuelen: u32,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
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
        buf.extend((self.key.len() as u16).to_be_bytes());
        buf.extend((self.value.len() as u32).to_be_bytes());
        buf.extend(self.key);
        buf.extend(self.value);
        buf
    }
}
