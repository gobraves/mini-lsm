//#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
//#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use super::Block;
use super::SIZEOF_U16;

/// Iterates on a block.
#[derive(Debug)]
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    pub idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let count = block.offsets.len();
        let mut block_iterator = Self::new(block);
        if count < 1 {
            return block_iterator;
        }
        block_iterator.seek_to_first();
        block_iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let count = block.offsets.len();
        let mut block_iterator = Self::new(block);
        if count < 1 {
            return block_iterator;
        }

        block_iterator.seek_to_key(key);

        block_iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    // 根据index判断valid要小心。
    // index是0，没有办法判断index本身就是0，还是只是Block的默认值且block不存在
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.key = Vec::new();
        self.value = Vec::new();
        self.idx = 0;
        let key = self.block.parse_block_data_item(0);
        self.key
            .extend(self.block.data[SIZEOF_U16..SIZEOF_U16 + key.len()].iter());
        let value = self.block.parse_block_data_item(SIZEOF_U16 + key.len());

        self.value.extend(
            self.block.data[SIZEOF_U16 * 2 + key.len()..SIZEOF_U16 * 2 + key.len() + value.len()]
                .iter(),
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        self.seek_to_offset(idx);
    }

    fn seek_to_offset(&mut self, idx: usize) {
        let offset = self.block.offsets[idx];
        let key = self.block.parse_block_data_item(offset as usize);
        let value = self
            .block
            .parse_block_data_item((offset as usize) + SIZEOF_U16 + key.len());
        self.key = key;
        self.value = value;
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            let offset = self.block.offsets[mid] as usize;
            let mid_key = self.block.parse_block_data_item(offset);

            match mid_key.cmp(&key.to_vec()) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    let value = self
                        .block
                        .parse_block_data_item(offset + SIZEOF_U16 + mid_key.len());
                    self.key = mid_key;
                    self.value = value;
                    return;
                }
            }
        }
        // 如果在data中查找一个不存在的数k，且需要返回>=查找的数
        // [low, high),  data[low] < k < data[high],
        // 此时循环结束只有可能是low=high，且而需要返回>= key的（key,
        // value）,因此返回low及对应的value即可
        // 同时需要考虑low是否可能超出index。比如[1,2,3,4] 查找5
        self.seek_to(low);
    }
}
