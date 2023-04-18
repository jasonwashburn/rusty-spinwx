use super::NUM_EXPECTED_FORECASTS;
use crate::s3_utils::{build_grib_key_prefix, fetch_list_of_grib_keys, parse_list_bucket_result};
use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};

pub fn gfs_run_is_complete(run: DateTime<Utc>) -> bool {
    let grib_prefix = build_grib_key_prefix(&run);
    let grib_keys = fetch_list_of_grib_keys(&grib_prefix).unwrap();
    let list_result = parse_list_bucket_result(&grib_keys).unwrap();
    if let Some(contents) = list_result.contents {
        let available_forecasts: Vec<_> = contents
            .iter()
            .filter(|content| (!content.key.ends_with(".anl") & !content.key.ends_with(".idx")))
            .collect();
        let num_forecasts = available_forecasts.len();
        println!("S3 ListBucketResult contains {} keys.", num_forecasts);
        (num_forecasts as i32) >= NUM_EXPECTED_FORECASTS
    } else {
        false
    }
}
pub fn determine_latest_possible_run(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let most_recent_run_hour = (now.hour() / 6) * 6;
    now.with_hour(most_recent_run_hour)?
        .with_minute(0)?
        .with_second(0)?
        .with_nanosecond(0)
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IdxRecord {
    index: i32,
    pub start_byte: i32,
    pub stop_byte: Option<i32>,
    model_run: String,
    pub parameter: String,
    pub level: String,
    forecast_type: String,
}

pub struct IdxCollection {
    pub records: Vec<IdxRecord>,
}

impl IdxCollection {
    pub fn new() -> IdxCollection {
        IdxCollection {
            records: Vec::new(),
        }
    }
    pub fn add_entry(&mut self, entry: IdxRecord) {
        self.records.push(entry);
    }
}

pub fn parse_idx_file(idx_data: &str) -> Result<IdxCollection> {
    let mut idx_collection = IdxCollection::new();
    let mut next_stop_byte: Option<i32> = None;
    for line in idx_data.lines().rev() {
        let split_line: Vec<&str> = line.split(':').collect();
        let index = split_line[0];
        let start_byte = split_line[1];
        let model_run = split_line[2].trim_start_matches("d=");
        let parameter = split_line[3];
        let level = split_line[4];
        let forecast_type = split_line[5];

        idx_collection.add_entry(IdxRecord {
            index: index.parse::<i32>().unwrap(),
            start_byte: start_byte.parse::<i32>().unwrap(),
            stop_byte: next_stop_byte,
            model_run: model_run.to_string(),
            parameter: parameter.to_string(),
            level: level.to_string(),
            forecast_type: forecast_type.to_string(),
        });
        next_stop_byte = start_byte.parse::<i32>().unwrap().checked_sub(1);
    }
    idx_collection.records.reverse();
    Ok(idx_collection)
}
