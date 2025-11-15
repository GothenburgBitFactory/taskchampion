#![cfg(target_os = "linux")]
//! Utilities for testing TLS behaviors.
//!
//! This package intercepts calls to libc's `open64` method, and checks for paths containing
//! `/ssl/`. This is useful to determine whether some deeply nested library is using native TLS
//! certificates (which are typically at pathnames containing `/ssl`) or built-in certificate lists
//! (which are embedded in the binary and not read from disk).
//!
//! This is likely to only work on GNU libc on Linux, but that's sufficient to verify that TC's
//! use of libraries is correctly communicating a desire to use, or not use, native certificates.
use libc::{c_char, c_int, dlsym, RTLD_NEXT};
use std::ffi::CStr;

static mut INTERCEPTING: bool = false;
static mut SEEN_SSL: bool = false;

/// Intercept the libc `open64` call, and set a flag when a path containing `/ssl/` is seen.
///
/// # SAFETY
///
/// This works on Linux, enough to perform some tests.
#[no_mangle]
pub unsafe extern "C" fn open64(path: *const c_char, flags: c_int) -> c_int {
    if unsafe { INTERCEPTING } {
        let path_rust = CStr::from_ptr(path)
            .to_str()
            .expect("path is not valid utf-8");
        if path_rust.contains("/ssl/") {
            unsafe { SEEN_SSL = true };
        }
        println!("open64({path_rust:?}, ..)");
    }
    type Open64Fn = unsafe extern "C" fn(*const c_char, c_int) -> c_int;
    let original_open64: Open64Fn = {
        let ptr = dlsym(RTLD_NEXT, c"open64".as_ptr());
        assert!(!ptr.is_null());
        std::mem::transmute(ptr)
    };
    original_open64(path, flags)
}

/// Return true if a file containing `/ssl/` has been opened since the
/// last call to `reset_seen_ssl_file`.
pub fn have_seen_ssl_file() -> bool {
    unsafe { SEEN_SSL }
}

/// Reset the flag returned by `have_seen_ssl_file` to false.
pub fn reset_seen_ssl_file() {
    unsafe {
        SEEN_SSL = false;
        INTERCEPTING = true;
    }
}

/// Assert that an SSL file was seen if the `tls-native-roots` feature
/// is enabled, and otherwise assert that no such file was seen.
pub fn assert_expected_seen_ssl_file() {
    #[cfg(feature = "tls-native-roots")]
    assert!(have_seen_ssl_file(), "Expected something to open a filename containing /ssl/ to load native certs, since `tls-native-roots` is enabled.");
    #[cfg(not(feature = "tls-native-roots"))]
    assert!(!have_seen_ssl_file(), "Did not expect anything to open a filename containing /ssl/ to load native certs, since `tls-native-roots` is not enabled.");
}
