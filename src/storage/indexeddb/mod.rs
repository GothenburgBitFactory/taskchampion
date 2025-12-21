#[cfg(not(target_arch = "wasm32"))]
compile_error!("IndexdDBStorage is only available on WASM targets");

mod storage;
pub use storage::IndexedDbStorage;

mod schema;
