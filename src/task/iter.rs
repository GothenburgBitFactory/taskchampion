use crate::errors::{Error, Result};
use chrono::DateTime;
pub(crate) use rrule::RRule;
use rrule::{Frequency, NWeekday, RRuleSet, Tz, Unvalidated, Weekday};
use std::str::FromStr;
use strum_macros::{Display, EnumString};

/// The iteration type of a task.
#[derive(Debug, PartialEq, Eq, Clone, Display, EnumString)]
pub(crate) enum IterType {
    #[strum(to_string = "fixed", serialize = "fx")]
    Fixed,
    #[strum(to_string = "fixed+", serialize = "f+", serialize = "fp")]
    FixedPlus,
    #[strum(to_string = "chained", serialize = "ch")]
    Chained,
}

enum DayKind {
    Weekday,
    Weekend,
}

/// Whether a rule carries a BY* part to select inside its own period
pub(super) fn selects_within_period(rule: &RRule<Unvalidated>) -> bool {
    !rule.get_by_set_pos().is_empty()
        || !rule.get_by_month().is_empty()
        || !rule.get_by_month_day().is_empty()
        || !rule.get_by_year_day().is_empty()
        || !rule.get_by_week_no().is_empty()
        || !rule.get_by_weekday().is_empty()
        || !rule.get_by_hour().is_empty()
        || !rule.get_by_minute().is_empty()
        || !rule.get_by_second().is_empty()
}

/// A copy of `rule` with any `COUNT` removed.
pub(super) fn without_count(rule: &RRule<Unvalidated>) -> Result<RRule<Unvalidated>> {
    if rule.get_count().is_none() {
        return Ok(rule.clone());
    }
    let stripped = rule
        .to_string()
        .split(';')
        .filter(|part| !part.starts_with("COUNT="))
        .collect::<Vec<_>>()
        .join(";");
    RRule::<Unvalidated>::from_str(&stripped).map_err(|e| {
        Error::Usage(format!(
            "Could not rebuild rule {stripped:?} without its COUNT: {e}"
        ))
    })
}

/// Parse an `iter` value and check that it can make a usable schedule
/// from `anchor`
pub(super) fn bake(iter: &str, anchor: DateTime<Tz>) -> Result<(RRule<Unvalidated>, RRuleSet)> {
    if iter.trim().is_empty() {
        return Err(Error::Usage(
            "Iterative tasks require a non-empty 'iter' value.".into(),
        ));
    }
    let unvalidated = str2rrule(iter)?;
    let rule = unvalidated
        .clone()
        .validate(anchor)
        .map_err(|e| Error::Usage(format!("Couldn't validate rrule: {e}")))?;
    Ok((unvalidated, RRuleSet::new(anchor).rrule(rule)))
}

/// Converts an iteration description string to a RRule.
///
/// String format check order:
/// 1. Direct RRULE
/// 2. TaskWarrior-style shorthand
/// 3. ISO-8601 duration
/// 4. Natural language via `text2rrule`.
pub(crate) fn str2rrule(value: &str) -> Result<RRule<Unvalidated>> {
    if let Ok(rule) = RRule::<Unvalidated>::from_str(value) {
        return Ok(rule);
    }
    if let Ok(rule) = tw_shorthand_to_rrule(value) {
        return Ok(rule);
    }
    if let Ok(rule) = iso8601_to_rrule(value) {
        return Ok(rule);
    }
    if let Ok(rule) = text2rrule::text2rrule(value) {
        if let Ok(rule) = RRule::<Unvalidated>::from_str(&rule) {
            return Ok(rule);
        }
    }
    Err(Error::Usage(format!(
        "Could not parse iteration value {value:?}"
    )))
}

/// Parse a TaskWarrior-style shorthand iteration string into a RRule.
fn tw_shorthand_to_rrule(value: &str) -> Result<RRule<Unvalidated>> {
    // Most TW iteration strings are of the form:
    // nPP where n is the interval number and PP is the period.
    // e.g. 3wks -> every three weeks.
    // If n is missing, it is assumed to be 1.
    // Steps:
    // 1) Normalize string (2 WeEKs -> 2weeks)
    // 2) Look for interval number (2week -> (2, week), mo -> (1, mo))
    // 3) Parse the period into a frequency and an interval multiplier
    //    (wk -> (Weekly, 1), qtr -> (Monthly, 3))
    // 4) Generate RRule ( (2, week) -> FREQ=WEEKLY;INTERVAL=2)

    // Normalize. Internal whitespace is dropped so "3 wks" and "3wks" are equivalent.
    let value: String = value
        .chars()
        .filter(|c| !c.is_whitespace())
        .map(|c| c.to_ascii_lowercase())
        .collect();

    // Split into interval and period.
    let num_str: String = value.chars().take_while(|c| c.is_ascii_digit()).collect();
    // An empty number means "1" (e.g. "week" -> every 1 week). A non-empty number
    // that doesn't fit in a u16 is an error.
    let interval = if num_str.is_empty() {
        1
    } else {
        num_str
            .parse::<u16>()
            .map_err(|e| Error::Usage(format!("Could not parse interval {num_str:?}: {e}")))?
    };
    let period = &value[num_str.len()..];

    // Parse the period into a frequency plus a multiplier on the interval, since
    // some periods are a multiple of their frequency - a fortnight is two weeks,
    // a quarter is three months, and so on.
    let mut day_kind: Option<DayKind> = None;
    let (freq, multiplier) = match period {
        "s" | "se" | "sec" | "secs" | "second" | "seconds" | "secondly" => (Frequency::Secondly, 1),
        "mi" | "min" | "mins" | "minute" | "minutes" | "minutely" => (Frequency::Minutely, 1),
        "h" | "hr" | "hrs" | "hour" | "hours" | "hourly" => (Frequency::Hourly, 1),
        "d" | "day" | "days" | "daily" => (Frequency::Daily, 1),
        "w" | "wk" | "wks" | "week" | "weeks" | "weekly" | "wkly" => (Frequency::Weekly, 1),
        "wkd" | "weekday" | "weekdays" | "weekdaily" => {
            day_kind = Some(DayKind::Weekday);
            (Frequency::Weekly, 1)
        }
        "wknd" | "weekend" | "weekends" | "weekendly" => {
            day_kind = Some(DayKind::Weekend);
            (Frequency::Weekly, 1)
        }
        "fortnight" | "fortnightly" | "sennight" | "biweekly" => (Frequency::Weekly, 2),
        "m" | "mo" | "mth" | "mths" | "mnths" | "month" | "months" | "monthly" => {
            (Frequency::Monthly, 1)
        }
        "bimonthly" => (Frequency::Monthly, 2),
        "q" | "qtr" | "qtrs" | "qrtr" | "qrtrs" | "quarter" | "quarters" | "quarterly" => {
            (Frequency::Monthly, 3)
        }
        "semiannual" => (Frequency::Monthly, 6),
        "y" | "yr" | "yrs" | "year" | "years" | "yearly" | "annual" => (Frequency::Yearly, 1),
        "biannual" | "biannually" | "biyearly" | "biyear" => (Frequency::Yearly, 2),
        _ => return Err(Error::Usage(format!("Could not parse period {}.", period))),
    };

    let interval = interval
        .checked_mul(multiplier)
        .ok_or_else(|| Error::Usage(format!("Interval {interval} {period} is too large")))?;

    // Generate the RRule.
    let rule = RRule::new(freq).interval(interval);
    let rule = match day_kind {
        None => rule,
        Some(DayKind::Weekday) => rule.by_weekday(vec![
            NWeekday::Every(Weekday::Mon),
            NWeekday::Every(Weekday::Tue),
            NWeekday::Every(Weekday::Wed),
            NWeekday::Every(Weekday::Thu),
            NWeekday::Every(Weekday::Fri),
        ]),
        Some(DayKind::Weekend) => rule.by_weekday(vec![
            NWeekday::Every(Weekday::Sat),
            NWeekday::Every(Weekday::Sun),
        ]),
    };
    Ok(rule)
}

fn iso8601_components(part: &str, designators: [char; 3]) -> Result<[u32; 3]> {
    let mut values = [0; 3];
    let mut next = 0;
    let mut digits = String::new();
    for c in part.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        let offset = designators[next..]
            .iter()
            .position(|d| *d == c)
            .ok_or_else(|| Error::Usage(format!("Unexpected {c:?} in ISO-8601 duration.")))?;
        values[next + offset] = digits
            .parse::<u32>()
            .map_err(|e| Error::Usage(format!("Could not parse value before {c:?}: {e}")))?;
        next += offset + 1;
        digits.clear();
    }
    if !digits.is_empty() {
        return Err(Error::Usage(format!(
            "Value {digits:?} in ISO-8601 duration has no unit."
        )));
    }
    Ok(values)
}

/// Parse an ISO-8601 duration (`PnW` or `P[nY][nM][nD][T[nH][nM][nS]]`) into a
/// RRule.
fn iso8601_to_rrule(value: &str) -> Result<RRule<Unvalidated>> {
    let value = value.trim().to_ascii_uppercase();
    let body = value
        .strip_prefix('P')
        .ok_or_else(|| Error::Usage(format!("{value:?} is not an ISO-8601 duration.")))?;

    let (freq, interval) = if let Some(weeks) = body.strip_suffix('W') {
        // ISO-8601 doesn't allow weeks be combined with anything else, so if
        // there is left over here the parse fails.
        let weeks: u64 = weeks
            .parse()
            .map_err(|e| Error::Usage(format!("Could not parse weeks in {value:?}: {e}")))?;
        if weeks == 0 {
            return Err(Error::Usage(format!("Duration {value:?} is zero.")));
        }
        (Frequency::Weekly, weeks)
    } else {
        let (date, time) = body.split_once('T').unwrap_or((body, ""));
        let [years, months, days] = iso8601_components(date, ['Y', 'M', 'D'])?;
        let [hours, minutes, seconds] = iso8601_components(time, ['H', 'M', 'S'])?;

        // Reduce each kind to its smallest unit
        let months = u64::from(years) * 12 + u64::from(months);
        let seconds = u64::from(days) * 86400
            + u64::from(hours) * 3600
            + u64::from(minutes) * 60
            + u64::from(seconds);

        // Use the largest unit that divides the duration evenly
        match (months, seconds) {
            (0, 0) => return Err(Error::Usage(format!("Duration {value:?} is zero."))),
            (_, 0) if months % 12 == 0 => (Frequency::Yearly, months / 12),
            (_, 0) => (Frequency::Monthly, months),
            (0, _) if seconds % 86400 == 0 => (Frequency::Daily, seconds / 86400),
            (0, _) if seconds % 3600 == 0 => (Frequency::Hourly, seconds / 3600),
            (0, _) if seconds % 60 == 0 => (Frequency::Minutely, seconds / 60),
            (0, _) => (Frequency::Secondly, seconds),
            _ => {
                return Err(Error::Usage(format!(
                    "Duration {value:?} mixes months or years with smaller units, \
                     which has no repetition equivalent."
                )))
            }
        }
    };

    let interval = u16::try_from(interval)
        .map_err(|_| Error::Usage(format!("Duration {value:?} is too large.")))?;
    Ok(RRule::new(freq).interval(interval))
}

#[cfg(test)]
mod test {
    use super::*;
    use rrule::{Tz, Validated};
    use std::str::FromStr;

    /// Validate an rrule string input and return its validated form.
    fn validate_rrule(input: &str) -> RRule<Validated> {
        let dt_start = chrono::Utc::now().with_timezone(&Tz::Local(chrono::Local));
        let rule = str2rrule(input).unwrap();
        rule.validate(dt_start).unwrap()
    }

    #[test]
    fn basic_daily() {
        let rule = validate_rrule("daily");
        assert_eq!(rule.get_freq(), Frequency::Daily);
        assert_eq!(rule.get_interval(), 1);
    }

    #[test]
    fn basic_weekly() {
        let rule = validate_rrule("weekly");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 1);
    }

    #[test]
    fn basic_monthly() {
        let rule = validate_rrule("month");
        assert_eq!(rule.get_freq(), Frequency::Monthly);
        assert_eq!(rule.get_interval(), 1);
    }

    #[test]
    fn basic_yearly() {
        let rule = validate_rrule("year");
        assert_eq!(rule.get_freq(), Frequency::Yearly);
        assert_eq!(rule.get_interval(), 1);
    }

    #[test]
    fn interval_prefix() {
        let rule = validate_rrule("3wk");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 3);
    }

    #[test]
    fn special_fortnight() {
        let rule = validate_rrule("fortnight");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 2);
    }

    #[test]
    fn special_biweekly() {
        let rule = validate_rrule("biweekly");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 2);
    }

    #[test]
    fn special_semiannual() {
        let rule = validate_rrule("semiannual");
        assert_eq!(rule.get_freq(), Frequency::Monthly);
        assert_eq!(rule.get_interval(), 6);
    }

    #[test]
    fn special_biannual() {
        let rule = validate_rrule("biannual");
        assert_eq!(rule.get_freq(), Frequency::Yearly);
        assert_eq!(rule.get_interval(), 2);
    }

    #[test]
    fn quarter_single() {
        let rule = validate_rrule("qtr");
        assert_eq!(rule.get_freq(), Frequency::Monthly);
        assert_eq!(rule.get_interval(), 3);
    }

    #[test]
    fn quarter_interval() {
        let rule = validate_rrule("3qtrs");
        assert_eq!(rule.get_freq(), Frequency::Monthly);
        assert_eq!(rule.get_interval(), 9);
    }

    #[test]
    fn weekday() {
        let rule = validate_rrule("weekdays");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 1);
        let days = rule.get_by_weekday();
        assert!(days.contains(&NWeekday::Every(Weekday::Mon)));
        assert!(days.contains(&NWeekday::Every(Weekday::Tue)));
        assert!(days.contains(&NWeekday::Every(Weekday::Wed)));
        assert!(days.contains(&NWeekday::Every(Weekday::Thu)));
        assert!(days.contains(&NWeekday::Every(Weekday::Fri)));
        assert!(!days.contains(&NWeekday::Every(Weekday::Sat)));
        assert!(!days.contains(&NWeekday::Every(Weekday::Sun)));
    }

    #[test]
    fn weekday_interval_counts_weeks() {
        // An interval on "weekdays" counts weeks, so "2weekdays" is every other
        // week's weekdays.
        let rule = str2rrule("2weekdays").unwrap();
        assert_eq!(
            rule.to_string(),
            "FREQ=WEEKLY;INTERVAL=2;BYDAY=MO,TU,WE,TH,FR"
        );
    }

    #[test]
    fn weekend() {
        let rule = validate_rrule("weekend");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 1);
        let days = rule.get_by_weekday();
        assert!(!days.contains(&NWeekday::Every(Weekday::Mon)));
        assert!(!days.contains(&NWeekday::Every(Weekday::Tue)));
        assert!(!days.contains(&NWeekday::Every(Weekday::Wed)));
        assert!(!days.contains(&NWeekday::Every(Weekday::Thu)));
        assert!(!days.contains(&NWeekday::Every(Weekday::Fri)));
        assert!(days.contains(&NWeekday::Every(Weekday::Sat)));
        assert!(days.contains(&NWeekday::Every(Weekday::Sun)));
    }

    #[test]
    fn case_insensitive() {
        let rule = validate_rrule("DAily");
        assert_eq!(rule.get_freq(), Frequency::Daily);
        assert_eq!(rule.get_interval(), 1);
        let rule = validate_rrule("3wK");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 3);
    }

    #[test]
    fn trim_whitespace() {
        let rule = validate_rrule("  daily  ");
        assert_eq!(rule.get_freq(), Frequency::Daily);
        assert_eq!(rule.get_interval(), 1);
    }

    #[test]
    fn invalid_period() {
        let result = str2rrule("3blarg");
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[test]
    fn empty_period() {
        let result = str2rrule("");
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[test]
    fn interval_overflow() {
        // An interval that doesn't fit in a u16 must be an error, not a silent
        // fall back to interval 1.
        let result = str2rrule("100000week");
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[test]
    fn quarter_interval_overflow() {
        let result = str2rrule("60000qtr");
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[test]
    fn raw_rrule() {
        let rule = str2rrule("FREQ=WEEKLY;INTERVAL=2;BYDAY=FR").unwrap();
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 2);
        assert!(rule
            .get_by_weekday()
            .contains(&NWeekday::Every(Weekday::Fri)));
    }

    #[test]
    fn text2rrule_every_other_friday() {
        // "every two weeks on friday" is handled by the text2rrule fallback,
        // not the TW shorthand parser.
        let rule = validate_rrule("every two weeks on friday");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert_eq!(rule.get_interval(), 2);
        assert!(rule
            .get_by_weekday()
            .contains(&NWeekday::Every(Weekday::Fri)));
    }

    #[test]
    fn text2rrule_every_mon_wed_thur() {
        let rule = validate_rrule("every mon, wed, and thur");
        assert_eq!(rule.get_freq(), Frequency::Weekly);
        assert!(rule
            .get_by_weekday()
            .contains(&NWeekday::Every(Weekday::Mon)));
        assert!(rule
            .get_by_weekday()
            .contains(&NWeekday::Every(Weekday::Wed)));
        assert!(rule
            .get_by_weekday()
            .contains(&NWeekday::Every(Weekday::Thu)));
    }

    #[test]
    fn iter_type_from_str() {
        assert_eq!(IterType::from_str("fixed").unwrap(), IterType::Fixed);
        assert_eq!(IterType::from_str("fx").unwrap(), IterType::Fixed);
        assert_eq!(IterType::from_str("fixed+").unwrap(), IterType::FixedPlus);
        assert_eq!(IterType::from_str("f+").unwrap(), IterType::FixedPlus);
        assert_eq!(IterType::from_str("fp").unwrap(), IterType::FixedPlus);
        assert_eq!(IterType::from_str("chained").unwrap(), IterType::Chained);
        assert_eq!(IterType::from_str("ch").unwrap(), IterType::Chained);
    }

    /// Assert that every string in `cases` parses correctly.
    fn assert_all(cases: &[&str], freq: Frequency, interval: u16) {
        for case in cases {
            let rule = validate_rrule(case);
            assert_eq!(rule.get_freq(), freq, "wrong frequency for {case:?}");
            assert_eq!(rule.get_interval(), interval, "wrong interval for {case:?}");
        }
    }

    /// Assert that every string in `cases` is rejected.
    fn assert_all_rejected(cases: &[&str]) {
        for case in cases {
            assert!(
                matches!(str2rrule(case), Err(Error::Usage(_))),
                "expected {case:?} to be rejected"
            );
        }
    }

    /// Every example from https://taskwarrior.org/docs/durations/
    mod duration_examples {
        use super::*;

        mod seconds {
            use super::*;

            #[test]
            fn five_seconds() {
                assert_all(
                    &[
                        "5 seconds",
                        "5 second",
                        "5 secs",
                        "5 sec",
                        "5 s",
                        "5seconds",
                        "5second",
                        "5secs",
                        "5sec",
                        "5s",
                    ],
                    Frequency::Secondly,
                    5,
                );
            }

            #[test]
            fn one_second() {
                assert_all(&["second", "sec"], Frequency::Secondly, 1);
            }
        }

        mod minutes {
            use super::*;

            #[test]
            fn five_minutes() {
                assert_all(
                    &[
                        "5 minutes",
                        "5 minute",
                        "5 mins",
                        "5 min",
                        "5minutes",
                        "5minute",
                        "5mins",
                        "5min",
                    ],
                    Frequency::Minutely,
                    5,
                );
            }

            #[test]
            fn one_minute() {
                assert_all(&["minute", "min"], Frequency::Minutely, 1);
            }
        }

        mod hours {
            use super::*;

            #[test]
            fn three_hours() {
                assert_all(
                    &[
                        "3 hours", "3 hour", "3 hrs", "3 hr", "3 h", "3hours", "3hour", "3hrs",
                        "3hr", "3h",
                    ],
                    Frequency::Hourly,
                    3,
                );
            }

            #[test]
            fn one_hour() {
                assert_all(&["hour", "hr"], Frequency::Hourly, 1);
            }
        }

        mod days {
            use super::*;

            #[test]
            fn two_days() {
                assert_all(
                    &["2 days", "2 day", "2 d", "2days", "2day", "2d"],
                    Frequency::Daily,
                    2,
                );
            }

            #[test]
            fn one_day() {
                assert_all(&["daily", "day"], Frequency::Daily, 1);
            }
        }

        mod weeks {
            use super::*;

            #[test]
            fn three_weeks() {
                assert_all(
                    &[
                        "3 weeks", "3 week", "3 wks", "3 wk", "3 w", "3weeks", "3week", "3wks",
                        "3wk", "3w",
                    ],
                    Frequency::Weekly,
                    3,
                );
            }

            #[test]
            fn one_week() {
                assert_all(&["weekly", "week", "wk"], Frequency::Weekly, 1);
            }

            #[test]
            fn weekdays() {
                let rule = validate_rrule("weekdays");
                assert_eq!(rule.get_freq(), Frequency::Weekly);
                assert_eq!(rule.get_interval(), 1);
                assert_eq!(
                    rule.get_by_weekday(),
                    &[
                        NWeekday::Every(Weekday::Mon),
                        NWeekday::Every(Weekday::Tue),
                        NWeekday::Every(Weekday::Wed),
                        NWeekday::Every(Weekday::Thu),
                        NWeekday::Every(Weekday::Fri),
                    ]
                );
            }
        }

        mod fortnights {
            use super::*;

            /// Twenty eight days
            #[test]
            fn two_fortnights() {
                assert_all(
                    &["2 fortnight", "2 sennight", "2fortnight", "2sennight"],
                    Frequency::Weekly,
                    4,
                );
            }

            /// Fourteen days.
            #[test]
            fn one_fortnight() {
                assert_all(&["biweekly", "fortnight", "sennight"], Frequency::Weekly, 2);
            }
        }

        mod months {
            use super::*;

            #[test]
            fn five_months() {
                assert_all(
                    &[
                        "5 months", "5 month", "5 mnths", "5 mths", "5 mth", "5 mo", "5 m",
                        "5months", "5month", "5mnths", "5mths", "5mth", "5mo", "5m",
                    ],
                    Frequency::Monthly,
                    5,
                );
            }

            #[test]
            fn one_month() {
                assert_all(&["monthly", "month", "mth", "mo"], Frequency::Monthly, 1);
            }

            #[test]
            fn bimonthly() {
                assert_all(&["bimonthly"], Frequency::Monthly, 2);
            }
        }

        mod quarters {
            use super::*;

            #[test]
            fn one_quarter_with_ordinal() {
                assert_all(
                    &[
                        "1 quarterly",
                        "1 quarters",
                        "1 quarter",
                        "1 qrtrs",
                        "1 qrtr",
                        "1 qtr",
                        "1 q",
                        "1quarterly",
                        "1quarters",
                        "1quarter",
                        "1qrtrs",
                        "1qrtr",
                        "1qtr",
                        "1q",
                    ],
                    Frequency::Monthly,
                    3,
                );
            }

            #[test]
            fn one_quarter() {
                assert_all(
                    &["quarterly", "quarter", "qrtr", "qtr"],
                    Frequency::Monthly,
                    3,
                );
            }
        }

        mod semiannual {
            use super::*;

            #[test]
            fn semiannual() {
                assert_all(&["semiannual"], Frequency::Monthly, 6);
            }
        }

        mod years {
            use super::*;

            #[test]
            fn one_year_with_ordinal() {
                assert_all(
                    &[
                        "1 years", "1 year", "1 yrs", "1 yr", "1 y", "1years", "1year", "1yrs",
                        "1yr", "1y",
                    ],
                    Frequency::Yearly,
                    1,
                );
            }

            #[test]
            fn one_year() {
                assert_all(&["annual", "yearly", "year", "yr"], Frequency::Yearly, 1);
            }

            #[test]
            fn two_years() {
                assert_all(&["biannual", "biyearly"], Frequency::Yearly, 2);
            }
        }

        mod iso8601 {
            use super::*;

            #[test]
            fn date_durations() {
                assert_all(&["P3D"], Frequency::Daily, 3);
                assert_all(&["P1000D"], Frequency::Daily, 1000);
                assert_all(&["P2M"], Frequency::Monthly, 2);
                assert_all(&["P1Y"], Frequency::Yearly, 1);
                assert_all(&["P1Y2M"], Frequency::Monthly, 14);
            }

            #[test]
            fn week_durations() {
                assert_all(&["P1W"], Frequency::Weekly, 1);
                assert_all(&["P2W"], Frequency::Weekly, 2);
                assert_all(&["P52W"], Frequency::Weekly, 52);
            }

            #[test]
            fn weeks_not_combinable() {
                assert_all_rejected(&[
                    "P1Y2W",
                    "P2W3D",
                    "P2WT1H",
                    "PT2W",
                    "PW",
                    "P0W",
                    "P2W2W",
                    "P70000W",
                    "P2Wgarbage",
                ]);
            }

            #[test]
            fn time_durations() {
                assert_all(&["PT50S"], Frequency::Secondly, 50);
                assert_all(&["PT40M"], Frequency::Minutely, 40);
                assert_all(&["PT40M50S"], Frequency::Secondly, 2450);
                assert_all(&["PT12H"], Frequency::Hourly, 12);
                assert_all(&["PT12H50S"], Frequency::Secondly, 43250);
                assert_all(&["PT12H40M"], Frequency::Minutely, 760);
                assert_all(&["PT12H40M50S"], Frequency::Secondly, 45650);
            }

            /// Days, hours, minutes and seconds are all exact, so they combine.
            #[test]
            fn combined_exact_units() {
                assert_all(&["P3DT12H"], Frequency::Hourly, 84);
            }

            /// The length of a month varies, so a duration mixing months or years
            /// with smaller units cannot be reduced to one frequency and interval.
            #[test]
            fn mixed_units_rejected() {
                assert_all_rejected(&["P2M3D", "P1Y3D", "P1Y2M3D", "P1Y2M3DT12H40M50S"]);
            }

            #[test]
            fn malformed_rejected() {
                assert_all_rejected(&[
                    "P",       // no components
                    "PT",      // no components
                    "P0D",     // zero duration
                    "T50S",    // no leading P
                    "P3",      // value with no unit
                    "PD",      // unit with no value
                    "PT3D",    // date unit in the time half
                    "P3H",     // time unit in the date half
                    "P3D2Y",   // units out of order
                    "P3D2D",   // repeated unit
                    "P70000D", // interval too large for a rule
                    "P3Dgarbage",
                ]);
            }
        }

        /// Time doesn't run backwards, so negative durations are rejected.
        mod negative {
            use super::*;

            #[test]
            fn negative_durations() {
                assert_all_rejected(&[
                    "-PT30S",
                    "-PT40M",
                    "-PT12H",
                    "-P3D",
                    "-P2W",
                    "-P2M",
                    "-P1Y",
                    "-P1Y2M3DT12H40M50S",
                ]);
            }
        }
    }
}
