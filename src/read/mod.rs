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

use arrow::error::Result;
use arrow::io::parquet::read::NestedState;
use arrow::array::Array;

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

pub trait SkipIterator {
    fn skip_page(&mut self, n: usize);
}


// 读取数据的时候需要为每种类型的数据生成一个结构体，例如 BooleanIter, BinaryIter 等。
// 这个结构体实现 Iterator trait 的 next 方法
// 在读取数据的时候，有些 page 是可以跳过的，因此又实现了 advance_by 方法，覆盖了 Iterator trait 的默认实现。

// 这些结构体返回的时候会转成一个 Box<dyn Iterator> 的类型，这时调用这个类型的 advance_by 就会调到默认实现，
// 而不是我们自己实现的方法。

// 有没有办法使用自己的实现




// 不加 dyn 不行
// trait objects must include the `dyn` keyword

// 不加 box 不行
// doesn't have a size known at compile-time

/// [`DynIter`] is an implementation of a single-threaded, dynamically-typed iterator.
///
/// This implementation is object safe.
pub struct DynIter<'a, V> {
    iter: Box<dyn Iterator<Item = V> + 'a + Send + Sync>,
}

impl<'a, V> Iterator for DynIter<'a, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        println!("----------this is nth 0000");
        self.iter.nth(n)
    }
}

impl<'a, V> DynIter<'a, V> {
    /// Returns a new [`DynIter`], boxing the incoming iterator
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = V> + 'a + Send + Sync,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}




#[derive(Debug)]
pub struct NestedIter<I>
where
    I: Iterator<Item = Result<(NestedState, Box<dyn Array>)>> + Send + Sync,
{
    iter: I,
}

impl<I> NestedIter<I>
where
    I: Iterator<Item = Result<(NestedState, Box<dyn Array>)>> + Send + Sync,
{
    pub fn new(
        iter: I,
    ) -> Self {
        Self {
            iter,
        }
    }
}

impl<I> Iterator for NestedIter<I>
where
    I: Iterator<Item = Result<(NestedState, Box<dyn Array>)>> + Send + Sync,
{
    type Item = Result<Box<dyn Array>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((_, item))) => Some(Ok(item)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((_, item))) => Some(Ok(item)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}
