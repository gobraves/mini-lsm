#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = 0;
        for meta in block_meta {
            estimated_size += std::mem::size_of::<u32>();
            estimated_size += std::mem::size_of::<u16>();
            estimated_size += meta.first_key.len();
        }
        buf.reserve(estimated_size);
        let original_len = buf.len();
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            block_meta.push(BlockMeta { offset, first_key });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(FileObject(data.into()))
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

pub struct SsTable {
    id: usize,
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
    //
    block_cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let meta_data_offset_data = file.read(file.size() - 4, 4)?;
        let meta_data_offset = (&meta_data_offset_data[..]).get_u32() as u64;
        let block_metas_bytes = file.read(meta_data_offset, file.size() - 4 - meta_data_offset)?;
        let block_metas = BlockMeta::decode_block_meta(&block_metas_bytes[..]);
        Ok(Self {
            id,
            file,
            block_metas,
            block_meta_offset: meta_data_offset as usize,
            block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset;
        let mut offset_end = self.block_meta_offset;
        if block_idx + 1 < self.block_metas.len() {
            offset_end = self.block_metas[block_idx + 1].offset;
        }
        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;

        Ok(Arc::new(Block::decode(&block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut idx: usize = 0;
        for (block_meta_idx, block_meta) in self.block_metas.iter().enumerate() {
            match key.cmp(&block_meta.first_key) {
                std::cmp::Ordering::Less => break,
                std::cmp::Ordering::Greater => idx = block_meta_idx,
                std::cmp::Ordering::Equal => {
                    idx = block_meta_idx;
                    break;
                }
            }
        }
        idx
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
