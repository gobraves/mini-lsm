#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::block::BlockIterator;
use anyhow::Result;

use super::SsTable;
use crate::iterators::StorageIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    block_idx: usize,
    block_iterator: BlockIterator,
    table: Arc<SsTable>,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_iterator = BlockIterator::create_and_seek_to_first(table.read_block(0)?);
        let iter = Self {
            block_idx: 0,
            block_iterator,
            table,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block_iterator = BlockIterator::create_and_seek_to_first(self.table.read_block(0)?);
        self.block_idx = 0;
        self.block_iterator = block_iterator;
        Ok(())
    }

    fn seek_key(table: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        let mut idx = table.find_block_idx(key);
        let mut block_iterator = BlockIterator::create_and_seek_to_key(table.read_block(idx)?, key);
        if !block_iterator.is_valid() {
            idx += 1;
            if idx < table.num_of_blocks() {
                block_iterator = BlockIterator::create_and_seek_to_first(table.read_block(idx)?);
            }
        }
        Ok((idx, block_iterator))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (idx, block_iterator) = Self::seek_key(&table, key)?;
        let iter = Self {
            block_idx: 0,
            block_iterator,
            table,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (idx, block_iterator) = Self::seek_key(&self.table, key)?;
        self.block_idx = idx;
        self.block_iterator = block_iterator;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if !self.block_iterator.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                let block_data = self.table.read_block(self.block_idx)?;

                self.block_iterator = BlockIterator::create_and_seek_to_first(block_data);
            }
        }
        Ok(())
    }
}
