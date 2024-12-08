//! Tests for cloud services.
//!
//! This tests that the various service methods, and especially `compare_and_swap`,
//! satisfy their requirements.
//!
//! The server must also satisfy:
//!  - `list`: Use a page size of 6 for n `#[cfg(test)]`
//!  - `compare_and_swap`: if the object name ends with `-racing-delete`, delete the
//!    object between the "compare" and "swap" phases of the operation
//!  - `compare_and_swap`: if the object name ends with `-racing-put`, put  the
//!    object between the "compare" and "swap" phases of the operation, with value
//!    `b"CHANGED"`.

use crate::errors::Result;
use crate::server::cloud::service::Service;
use pretty_assertions::assert_eq;

/// Define a collection of cloud service tests that apply to all service implementations.
macro_rules! service_tests {
    ($service:expr) => {
        fn make_pfx() -> impl Fn(&str) -> Vec<u8> {
            let prefix = uuid::Uuid::new_v4();
            move |n: &_| format!("{}-{}", prefix.as_simple(), n).into_bytes()
        }

        #[test]
        fn put_and_get() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::put_and_get(service, make_pfx())
        }
        #[test]
        fn get_missing() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::get_missing(service, make_pfx())
        }
        #[test]
        fn del() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::del(service, make_pfx())
        }
        #[test]
        fn del_missing() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::del_missing(service, make_pfx())
        }
        #[test]
        fn list() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::list(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_create() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_create(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_matches() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_matches(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_expected_no_file() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_expected_no_file(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_old_value() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_old_value(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_changes() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_changes(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_disappears() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_disappears(service, make_pfx())
        }
        #[test]
        fn compare_and_swap_appears() -> $crate::errors::Result<()> {
            let Some(service) = $service else {
                return Ok(());
            };
            $crate::server::cloud::test::compare_and_swap_appears(service, make_pfx())
        }
    };
}

pub(crate) use service_tests;

pub(super) fn put_and_get(mut svc: impl Service, pfx: impl Fn(&str) -> Vec<u8>) -> Result<()> {
    svc.put(&pfx("testy"), b"foo")?;
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, Some(b"foo".to_vec()));

    // Clean up.
    svc.del(&pfx("testy"))?;
    Ok(())
}

pub(super) fn get_missing(mut svc: impl Service, pfx: impl Fn(&str) -> Vec<u8>) -> Result<()> {
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, None);
    Ok(())
}

pub(super) fn del(mut svc: impl Service, pfx: impl Fn(&str) -> Vec<u8>) -> Result<()> {
    svc.put(&pfx("testy"), b"data")?;
    svc.del(&pfx("testy"))?;
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, None);
    Ok(())
}

pub(super) fn del_missing(mut svc: impl Service, pfx: impl Fn(&str) -> Vec<u8>) -> Result<()> {
    // Deleting an object that does not exist is not an error.
    assert!(svc.del(&pfx("testy")).is_ok());
    Ok(())
}

pub(super) fn list(mut svc: impl Service, pfx: impl Fn(&str) -> Vec<u8>) -> Result<()> {
    let mut names: Vec<_> = (0..20).map(|i| pfx(&format!("pp-{i:02}"))).collect();
    names.sort();
    // Create 20 objects that will be listed.
    for n in &names {
        svc.put(n, b"data")?;
    }
    // And another object that should not be included in the list.
    svc.put(&pfx("xxx"), b"data")?;

    let got_objects: Vec<_> = svc.list(&pfx("pp-")).collect::<Result<_>>()?;
    let mut got_names: Vec<_> = got_objects.into_iter().map(|oi| oi.name).collect();
    got_names.sort();
    assert_eq!(
        got_names
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect::<Vec<_>>(),
        names
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect::<Vec<_>>()
    );

    // Clean up.
    for n in got_names {
        svc.del(&n)?;
    }
    svc.del(&pfx("xxx"))?;
    Ok(())
}

pub(super) fn compare_and_swap_create(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    assert!(svc.compare_and_swap(&pfx("testy"), None, b"bar".to_vec())?);
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, Some(b"bar".to_vec()));

    // Clean up.
    svc.del(&pfx("testy"))?;
    Ok(())
}

pub(super) fn compare_and_swap_matches(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    // Create the existing file, with two different values over time.
    svc.put(&pfx("testy"), b"foo1")?;
    svc.put(&pfx("testy"), b"foo2")?;
    // A compare_and_swap for the latest value succeeds.
    assert!(svc.compare_and_swap(&pfx("testy"), Some(b"foo2".to_vec()), b"bar".to_vec())?);
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, Some(b"bar".to_vec()));

    // Clean up.
    svc.del(&pfx("testy"))?;
    Ok(())
}

pub(super) fn compare_and_swap_expected_no_file(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    svc.put(&pfx("testy"), b"foo1")?;
    assert!(!svc.compare_and_swap(&pfx("testy"), None, b"bar".to_vec())?);
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, Some(b"foo1".to_vec()));

    // Clean up.
    svc.del(&pfx("testy"))?;
    Ok(())
}

pub(super) fn compare_and_swap_old_value(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    // Create the existing file, with two different values over time.
    svc.put(&pfx("testy"), b"foo1")?;
    svc.put(&pfx("testy"), b"foo2")?;
    // A compare_and_swap for the old value fails.
    assert!(!svc.compare_and_swap(&pfx("testy"), Some(b"foo1".to_vec()), b"bar".to_vec())?);
    let got = svc.get(&pfx("testy"))?;
    assert_eq!(got, Some(b"foo2".to_vec()));

    // Clean up.
    svc.del(&pfx("testy"))?;
    Ok(())
}

pub(super) fn compare_and_swap_changes(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    // Create the existing object, but since it is named "racing-put" its value will change
    // just before the `put_object` call. This tests the "compare" part of `compare_and_swap`.
    svc.put(&pfx("racing-put"), b"foo1")?;
    assert!(!svc.compare_and_swap(&pfx("racing-put"), Some(b"foo1".to_vec()), b"bar".to_vec())?);
    let got = svc.get(&pfx("racing-put"))?;
    assert_eq!(got, Some(b"CHANGED".to_vec()));
    Ok(())
}

pub(super) fn compare_and_swap_disappears(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    // Create the existing object, but since it is named "racing-delete" it will disappear just
    // before the `put_object` call. This tests the case where the exists when
    // `compare_and_swap` calls `get_object` but is deleted when it calls `put_object`.
    svc.put(&pfx("racing-delete"), b"foo1")?;
    assert!(!svc.compare_and_swap(
        &pfx("racing-delete"),
        Some(b"foo1".to_vec()),
        b"bar".to_vec()
    )?);
    let got = svc.get(&pfx("racing-delete"))?;
    assert_eq!(got, None);
    Ok(())
}

pub(super) fn compare_and_swap_appears(
    mut svc: impl Service,
    pfx: impl Fn(&str) -> Vec<u8>,
) -> Result<()> {
    // Create the existing object, but since it is named "racing-put" the object will appear just
    // before the `put_object` call. This tests the case where the object does not exist when
    // `compare_and_swap` calls `get_object`, but does exist when it calls `put_object`.
    assert!(!svc.compare_and_swap(&pfx("racing-put"), None, b"bar".to_vec())?);
    let got = svc.get(&pfx("racing-put"))?;
    assert_eq!(got, Some(b"CHANGED".to_vec()));
    Ok(())
}
