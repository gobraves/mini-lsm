//#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
//#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
#[derive(Default, Clone, Debug)]
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let data_count = self.offsets.len();
        let mut data = self.data.clone();
        for value in self.offsets.iter() {
            data.put_u16(*value);
        }
        data.put_u16(data_count as u16);

        Bytes::from(data)
    }

    // parse bytes to a block
    // 1. parse offsets, count,
    // 2. 计算出offsets开始的位置
    pub fn decode(data: &[u8]) -> Self {
        let count = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        //let count = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let offsets_start = data.len() - (count + 1) * SIZEOF_U16;
        let block_data = data[0..offsets_start].to_vec();
        let mut block_offsets: Vec<u16> = Vec::with_capacity(count);

        let mut tmp = 0;

        for (index, item) in data[offsets_start..data.len() - 2].iter().enumerate() {
            if index % 2 == 0 {
                tmp = *item as u16 * 256;
            } else {
                tmp += *item as u16;
                block_offsets.push(tmp);
            }
        }

        Block {
            data: block_data.to_vec(),
            offsets: block_offsets,
        }
    }

    pub fn parse_block_data_item(&self, offset: usize) -> Vec<u8> {
        let item_len =
            usize::from(((self.data[offset] as u16) << 8) + self.data[offset + 1] as u16);
        self.data[offset + SIZEOF_U16..offset + SIZEOF_U16 + item_len].to_vec()
    }

    //pub fn parse_block_data_value(&self, offset: usize, key_len: usize) -> Vec<u8> {
    //let value_len =
    //usize::from(((self.data[offset + key_len + SIZEOF_U16] as u16) << 8) + self.data[offset + key_len + 1] as u16);
    //self.data[offset + SIZEOF_U16 * 2 + key_len..offset + SIZEOF_U16 * 2 + key_len + value_len].to_vec()
    //}
}

#[cfg(test)]
mod tests;
