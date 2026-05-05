use crate::errors::{Error, Result};
use rrule::{Frequency, NWeekday, RRule, Unvalidated, Weekday};
use strum_macros::{Display, EnumString};

/// The iteration type of a task.
#[derive(Debug, PartialEq, Eq, Clone, Display, EnumString)]
#[repr(C)]
pub enum IterType {
    #[strum(serialize = "fixed", serialize = "fx")]
    Fixed,
    #[strum(serialize = "fixed+", serialize = "f+", serialize = "fp")]
    FixedPlus,
    #[strum(serialize = "chained", serialize = "ch")]
    Chained,
    /// Unknown signifies an iter type in the task DB that was not
    /// recognized.  This supports forward-compatibility if a
    /// new type is added.  Tasks with unknown iter types should
    /// be ignored (but not deleted).
    Unknown(String),
}

enum SpecialDays {
    Weekday,
    Weekend,
}
/// Converts an iteration description string to a RRule.
///
/// For now, only handles the standard TaskWarrior style descriptions.
pub fn str2rrule(value: &str) -> Result<RRule<Unvalidated>> {
    // Most TW iteration strings are of the form:
    // nPP where n is the interval number and PP is the period.
    // e.g. 3wks -> every three weeks.
    // If n is missing, it is assumed to be 1.
    // Steps:
    // 1) Normalize string (2WeEKs -> 2weeks)
    // 2) Expand special terms (annual -> 1year, fortnight -> 2week)
    // 2) Look for interval number (2week -> (2, week), mo -> (1, mo))
    // 3) Parse intervals into tokens (wk -> Week)
    // 4) Generate RRule ( (2, week) -> FREQ=WEEKLY;INTERVAL=2)

    // Normalize.
    let value = value.trim().to_ascii_lowercase();

    // Special terms.
    let value = match value.as_str() {
        "fortnight" | "fortnightly" | "biweekly" => "2week",
        "semiannual" => "6month",
        "annual" => "1year",
        "biannual" | "biannually" | "biyearly" | "biyear" => "2year",
        _ => value.as_str(),
    };

    // Split into interval and period.
    let num_str: String = value.chars().take_while(|c| c.is_ascii_digit()).collect();
    let mut interval = num_str.parse::<u16>().unwrap_or(1);
    let period = &value[num_str.len()..];

    // Parse period into enum.
    let mut special_days: Option<SpecialDays> = None;
    let freq = match period {
        "se" | "sec" | "second" | "seconds" | "secondly" => Frequency::Secondly,
        "mi" | "min" | "minute" | "minutes" | "minutley" => Frequency::Minutely,
        "hr" | "hour" | "hours" | "hourly" => Frequency::Hourly,
        "day" | "days" | "daily" => Frequency::Daily,
        "wk" | "week" | "weekly" | "wkly" => Frequency::Weekly,
        "wkd" | "weekday" | "weekdays" | "weekdaily" => {
            special_days = Some(SpecialDays::Weekday);
            Frequency::Daily
        }
        "wknd" | "weekend" | "weekends" | "weekendly" => {
            special_days = Some(SpecialDays::Weekend);
            Frequency::Daily
        }
        "mo" | "month" | "months" | "monthly" => Frequency::Monthly,
        "qtr" | "qtrs" | "quarter" | "quarterly" => {
            interval *= 3;
            Frequency::Monthly
        }
        "yr" | "year" | "yearly" | "annual" => Frequency::Yearly,
        _ => return Err(Error::Usage(format!("Could not parse period {}.", period))),
    };

    // Generate the RRule.
    let rule = RRule::new(freq).interval(interval);
    let rule = match special_days {
        None => rule,
        Some(SpecialDays::Weekday) => rule.by_weekday(vec![
            NWeekday::Every(Weekday::Mon),
            NWeekday::Every(Weekday::Tue),
            NWeekday::Every(Weekday::Wed),
            NWeekday::Every(Weekday::Thu),
            NWeekday::Every(Weekday::Fri),
        ]),
        Some(SpecialDays::Weekend) => rule.by_weekday(vec![
            NWeekday::Every(Weekday::Sat),
            NWeekday::Every(Weekday::Sun),
        ]),
    };
    Ok(rule)
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
        let validated = rule.validate(dt_start).unwrap();
        validated
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
        let rule = validate_rrule("2qtrs");
        assert_eq!(rule.get_freq(), Frequency::Monthly);
        assert_eq!(rule.get_interval(), 6);
    }

    #[test]
    fn weekday() {
        let rule = validate_rrule("weekdays");
        assert_eq!(rule.get_freq(), Frequency::Daily);
        assert_eq!(rule.get_interval(), 1);
        let days = rule.get_by_weekday();
        assert!(days.contains(&NWeekday::Every(Weekday::Mon)));
        assert!(days.contains(&NWeekday::Every(Weekday::Tue)));
        assert!(days.contains(&NWeekday::Every(Weekday::Wed)));
        assert!(days.contains(&NWeekday::Every(Weekday::Thu)));
        assert!(days.contains(&NWeekday::Every(Weekday::Fri)));
    }

    #[test]
    fn weekend() {
        let rule = validate_rrule("weekdays");
        assert_eq!(rule.get_freq(), Frequency::Daily);
        assert_eq!(rule.get_interval(), 1);
        let days = rule.get_by_weekday();
        assert!(days.contains(&NWeekday::Every(Weekday::Sun)));
        assert!(days.contains(&NWeekday::Every(Weekday::Mon)));
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
    fn iter_type_from_str() {
        assert_eq!(IterType::from_str("fixed").unwrap(), IterType::Fixed);
        assert_eq!(IterType::from_str("fx").unwrap(), IterType::Fixed);
        assert_eq!(IterType::from_str("fixed+").unwrap(), IterType::FixedPlus);
        assert_eq!(IterType::from_str("f+").unwrap(), IterType::FixedPlus);
        assert_eq!(IterType::from_str("fp").unwrap(), IterType::FixedPlus);
        assert_eq!(IterType::from_str("chained").unwrap(), IterType::Chained);
        assert_eq!(IterType::from_str("ch").unwrap(), IterType::Chained);
    }
}
