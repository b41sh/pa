//! APIs to read Arrow's IPC format.
//!
//! The two important structs here are the [`FileReader`](reader::FileReader),
//! which provides arbitrary access to any of its messages, and the
//! [`StreamReader`](stream::StreamReader), which only supports reading
//! data in the order it was written in.

mod array;
pub mod deserialize;
mod read_basic;
use std::io::BufReader;
pub mod reader;

pub trait NativeReadBuf: std::io::BufRead {
    fn buffer_bytes(&self) -> &[u8];
}

impl<R: std::io::Read> NativeReadBuf for BufReader<R> {
    fn buffer_bytes(&self) -> &[u8] {
        self.buffer()
    }
}

impl<T: AsRef<[u8]>> NativeReadBuf for std::io::Cursor<T> {
    fn buffer_bytes(&self) -> &[u8] {
        let len = self.position().min(self.get_ref().as_ref().len() as u64);
        &self.get_ref().as_ref()[(len as usize)..]
    }
}

impl<B: NativeReadBuf + ?Sized> NativeReadBuf for Box<B> {
    fn buffer_bytes(&self) -> &[u8] {
        (**self).buffer_bytes()
    }
}

pub trait PageIterator {
    fn swap_buffer(&mut self, buffer: &mut Vec<u8>);
}

#[derive(Debug)]
pub struct PageInfo {
    pub length: usize,
    pub num_values: usize,
    pub is_skip: bool,
}

impl PageInfo {
    #[inline]
    pub fn new(length: usize, num_values: usize, is_skip: bool) -> Self {
        Self {
            length,
            num_values,
            is_skip,
        }
    }
}
