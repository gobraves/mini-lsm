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
    idx: usize,
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
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len() - 1
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
        if self.is_valid() {
            self.idx += 1;
            let offset = self.block.offsets[self.idx];
            let key = self.block.parse_block_data_item(offset as usize);
            let value = self
                .block
                .parse_block_data_item((offset as usize) + SIZEOF_U16 + key.len());
            self.key = key;
            self.value = value;
        }
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        for (idx, offset) in self.block.offsets.iter().enumerate() {
            self.idx = idx;
            let tmp_key = self.block.parse_block_data_item(*offset as usize);
            let value = self
                .block
                .parse_block_data_item((*offset as usize) + SIZEOF_U16 + tmp_key.len());

            if tmp_key < (*key).to_vec() {
                continue;
            } else {
                self.key = tmp_key;
                self.value = value;
                break;
            }
        }
    }
}
