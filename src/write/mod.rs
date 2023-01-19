//! APIs to write to Arrow's IPC format.
pub(crate) mod common;
//mod pages;
mod serialize;
pub(crate) mod writer;

pub use common::WriteOptions;
pub use serialize::write;
pub use writer::NativeWriter;

pub(crate) mod common_sync;
