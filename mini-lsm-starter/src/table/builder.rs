#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    // Add other fields you need.
    first_key: Vec<u8>,
    data: Vec<u8>,
    block_size: usize,
    block_builder: BlockBuilder,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            meta: Vec::new(),
            first_key: Vec::new(),
            data: Vec::new(),
            block_size,
            block_builder: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }

        if self.block_builder.add(key, value) {
            return;
        }

        self.finish_block();
        assert!(self.block_builder.add(key, value));
        self.first_key = key.to_vec();
    }

    fn finish_block(&mut self) {
        let builder =
            std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into(),
        });

        self.data.extend(encoded_block);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf).unwrap();
        Ok(SsTable {
            id,
            file,
            block_metas: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
