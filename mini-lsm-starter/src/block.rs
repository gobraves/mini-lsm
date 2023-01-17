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
        let block_offsets = data[offsets_start..data.len() - 2]
            .chunks(2)
            .map(|mut x| x.get_u16())
            .collect();

        Block {
            data: block_data.to_vec(),
            offsets: block_offsets,
        }
    }

    pub fn parse_block_data_item(&self, offset: usize) -> Vec<u8> {
        let item_len = (&self.data[offset..]).get_u16() as usize;
        self.data[offset + SIZEOF_U16..offset + SIZEOF_U16 + item_len].to_vec()
    }
}

#[cfg(test)]
mod tests;
