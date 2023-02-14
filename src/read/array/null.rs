use crate::read::{PageInfo, PageIterator};
use arrow::{array::NullArray, datatypes::DataType, error::Result};

#[derive(Debug)]
pub struct NullIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    iter: I,
    data_type: DataType,
}

impl<I> NullIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    pub fn new(iter: I, data_type: DataType) -> Self {
        Self { iter, data_type }
    }
}

impl<I> Iterator for NullIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    type Item = Result<NullArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let (page_infos, mut buffer) = match self.iter.next() {
            Some(Ok((page_infos, buffer))) => (page_infos, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };
        let length: usize = page_infos
            .iter()
            .map(|p| if p.is_skip { 0 } else { p.num_values })
            .sum();

        self.iter.swap_buffer(&mut buffer);
        Some(NullArray::try_new(self.data_type.clone(), length))
    }
}
