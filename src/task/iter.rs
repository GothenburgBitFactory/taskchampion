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
            NWeekday::Every(Weekday::Thu),
            NWeekday::Every(Weekday::Fri),
        ]),
    };
    Ok(rule)
}
