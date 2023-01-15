//#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
//#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            block_size,
            data: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // current size + key size + value size + key 2 Byte + value 2 Byte + new offset 2 Byte
        let data_size = self.current_ussage() + key.len() + value.len() + SIZEOF_U16 * 3;

        if data_size > self.block_size && !self.is_empty() {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    pub fn current_ussage(&self) -> usize {
        // offsets size + data + num size
        self.data.len() + SIZEOF_U16 + self.offsets.len() * SIZEOF_U16
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
