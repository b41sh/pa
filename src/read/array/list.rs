use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow::error::Result;
use arrow::io::parquet::read::{create_list, NestedArrayIter, NestedState};

use crate::read::DynIter;

/// An iterator adapter over [`NestedArrayIter`] assumed to be encoded as Struct arrays
pub struct ListIterator<'a> {
    //iters: Vec<NestedArrayIter<'a>>,
    iter: DynIter<'a, Result<(NestedState, Box<dyn Array>)>>,
    field: Field,
}

impl<'a> ListIterator<'a> {
    /// Creates a new [`StructIterator`] with `iters` and `fields`.
    pub fn new(
        iter: DynIter<'a, Result<(NestedState, Box<dyn Array>)>>,
        field: Field,
    ) -> Self {
        Self { iter, field }
    }
}

impl<'a> Iterator for ListIterator<'a> {
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        println!("this is list nth n={:?}", n);

        let (mut nested, values) = match self.iter.nth(n) {
            Some(Ok((nested, values))) => (nested, values),
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };
        println!("nested={:?}", nested);
        println!("values={:?}", values);
        println!("data_type={:?}", self.field.data_type());

        let _ = nested.nested.pop().unwrap();

        let array = create_list(self.field.data_type().clone(), &mut nested, values);
        println!("array={:?}", array);
        println!("array.len={:?}", array.len());
        Some(Ok((nested, array)))
    }

    fn next(&mut self) -> Option<Self::Item> {
        println!("this is list next");
        self.nth(0)
    }
}
