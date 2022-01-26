use crate::string::TCString;
use libc;
use taskchampion::Uuid;

/// TCUuid is used as a task identifier.  Uuids do not contain any pointers and need not be freed.
/// Uuids are typically treated as opaque, but the bytes are available in big-endian format.
///
/// cbindgen:field-names=[bytes]
#[repr(C)]
pub struct TCUuid([u8; 16]);

impl From<Uuid> for TCUuid {
    fn from(uuid: Uuid) -> TCUuid {
        TCUuid(*uuid.as_bytes())
    }
}

impl From<TCUuid> for Uuid {
    fn from(uuid: TCUuid) -> Uuid {
        Uuid::from_bytes(uuid.0)
    }
}

/// Create a new, randomly-generated UUID.
#[no_mangle]
pub extern "C" fn tc_uuid_new_v4() -> TCUuid {
    Uuid::new_v4().into()
}

/// Create a new UUID with the nil value.
#[no_mangle]
pub extern "C" fn tc_uuid_nil() -> TCUuid {
    Uuid::nil().into()
}

/// Length, in bytes, of a C string containing a TCUuid.
// TODO: why not a const?
#[no_mangle]
pub static TC_UUID_STRING_BYTES: usize = ::uuid::adapter::Hyphenated::LENGTH;

/// Write the string representation of a TCUuid into the given buffer, which must be
/// at least TC_UUID_STRING_BYTES long.  No NUL terminator is added.
#[no_mangle]
pub extern "C" fn tc_uuid_to_buf<'a>(uuid: TCUuid, buf: *mut libc::c_char) {
    debug_assert!(!buf.is_null());
    let buf: &'a mut [u8] = unsafe {
        std::slice::from_raw_parts_mut(buf as *mut u8, ::uuid::adapter::Hyphenated::LENGTH)
    };
    let uuid: Uuid = uuid.into();
    uuid.to_hyphenated().encode_lower(buf);
}

/// Write the string representation of a TCUuid into the given buffer, which must be
/// at least TC_UUID_STRING_BYTES long.  No NUL terminator is added.
#[no_mangle]
pub extern "C" fn tc_uuid_to_str(uuid: TCUuid) -> *mut TCString<'static> {
    let uuid: Uuid = uuid.into();
    let s = uuid.to_string();
    TCString::from(s).return_val()
}

/// Parse the given value as a UUID.  The value must be exactly TC_UUID_STRING_BYTES long.  Returns
/// false on failure.
#[no_mangle]
pub extern "C" fn tc_uuid_from_str<'a>(s: *mut TCString, uuid_out: *mut TCUuid) -> bool {
    debug_assert!(!s.is_null());
    debug_assert!(!uuid_out.is_null());
    let s = TCString::from_arg(s);
    if let Ok(s) = s.as_str() {
        if let Ok(u) = Uuid::parse_str(s) {
            unsafe { *uuid_out = u.into() };
            return true;
        }
    }
    false
}