use chrono::{offset::LocalResult, DateTime, TimeZone, Utc};
#[cfg(feature = "iterative-tasks")]
use rrule::Tz;

pub(crate) type Timestamp = DateTime<Utc>;

pub fn utc_timestamp(secs: i64) -> Timestamp {
    match Utc.timestamp_opt(secs, 0) {
        LocalResult::Single(tz) => tz,
        // The other two variants are None and Ambiguous, which both are caused by DST.
        _ => unreachable!("We're requesting UTC so daylight saving time isn't a factor."),
    }
}
/// Returns the local timezone for rrule scheduling.
/// On WASM, `chrono::Local` is unavailable, so UTC is used as a fallback.
#[cfg(feature = "iterative-tasks")]
pub(crate) fn local_tz() -> Tz {
    #[cfg(not(target_arch = "wasm32"))]
    return Tz::Local(chrono::Local);
    #[cfg(target_arch = "wasm32")]
    return Tz::UTC;
}

#[cfg(not(test))]
pub(crate) fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
pub(crate) mod mock_time {
    use chrono::{DateTime, Utc};
    use std::cell::Cell;
    thread_local! {
        static T: Cell<Option<i64>> = const {Cell::new(None)};
    }
    pub(crate) fn utc_now() -> DateTime<Utc> {
        T.with(|t| match t.get() {
            Some(secs) => DateTime::from_timestamp(secs, 0).unwrap(),
            None => Utc::now(),
        })
    }
    pub(crate) fn set(t: DateTime<Utc>) {
        T.with(|c| c.set(Some(t.timestamp())));
    }
    pub(crate) fn reset() {
        T.with(|c| c.set(None));
    }
}
#[cfg(test)]
pub(crate) use mock_time::utc_now;
