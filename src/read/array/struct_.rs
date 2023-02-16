use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow::error::Result;
use arrow::io::parquet::read::{NestedArrayIter, NestedState};

use crate::read::DynIter;

/// An iterator adapter over [`NestedArrayIter`] assumed to be encoded as Struct arrays
pub struct StructIterator<'a> {
    //iters: Vec<NestedArrayIter<'a>>,
    iters: Vec<DynIter<'a, Result<(NestedState, Box<dyn Array>)>>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    /// Creates a new [`StructIterator`] with `iters` and `fields`.
    pub fn new(
        iters: Vec<DynIter<'a, Result<(NestedState, Box<dyn Array>)>>>,
        fields: Vec<Field>
    ) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        println!("this is struct nth n={:?}", n);
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.nth(n))
            .collect::<Vec<_>>();

        if values.iter().any(|x| x.is_none()) {
            return None;
        }

        // todo: unzip of Result not yet supportted in stable Rust
        let mut nested = vec![];
        let mut new_values = vec![];
        for x in values {
            match x.unwrap() {
                Ok((nest, values)) => {
                    new_values.push(values);
                    nested.push(nest);
                }
                Err(e) => return Some(Err(e)),
            }
        }
        let mut nested = nested.pop().unwrap();
        let (_, validity) = nested.nested.pop().unwrap().inner();
/**
        Some(Ok((
            nested,
            Box::new(StructArray::new(
                DataType::Struct(self.fields.clone()),
                new_values,
                validity.and_then(|x| x.into()),
            )),
        )))
*/
        let array = Box::new(StructArray::new(
                DataType::Struct(self.fields.clone()),
                new_values,
                validity.and_then(|x| x.into()),
        ));

        println!("sarray={:?}", array);
        println!("sarray.len={:?}", array.len());
        Some(Ok((nested, array)))
    }

    fn next(&mut self) -> Option<Self::Item> {
        println!("this is struct next");
        self.nth(0)
    }
}
