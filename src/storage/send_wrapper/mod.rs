//! This module implements a wrapper around a non-Send version of the Storage and StorageTxn
//! traits, exposing implementations of those traits.
//!
//! The wrapper uses an actor model: the wrapper uses channels to communicate with a single
//! instance of the wrapped type, running in a dedicated thread.

mod actor;

mod traits;
pub(super) use traits::*;

mod wrapper;
pub(super) use wrapper::Wrapper;

#[cfg(test)]
mod test;
