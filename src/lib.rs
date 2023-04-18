use anyhow::Result;
use chrono::prelude::*;
use chrono::DateTime;
use spin_sdk::{
    http::{Params, Request, Response, Router},
    http_component,
};
use std::collections::HashMap;

const MODEL_HOUR_INTERVAL: i32 = 6;
const NUM_EXPECTED_FORECASTS: i32 = 209;
const S3_BUCKET: &str = "noaa-gfs-bdp-pds";

#[http_component]
fn handle_route(req: Request) -> Result<Response> {
    let mut router = Router::new();
    router.get("/gfs/latest", api::route_gfs_latest);
    router.get("/gfs/idx", api::route_gfs_idx);
    router.get(
        "/gfs/idx/:year/:month/:day/:hour/:forecast",
        api::route_gfs_idx_info,
    );
    router.any("/*", api::route_echo_wildcard);
    router.handle(req)
}

mod api {

    use std::ops::Sub;

    use crate::s3_utils::build_grib_idx_key;

    use super::*;
    use gfs::gfs_run_is_complete;
    use s3_utils::{build_grib_key_prefix, fetch_list_of_grib_keys};
    use spin_sdk::http::internal_server_error;

    pub fn route_gfs_latest(_req: Request, _params: Params) -> Result<Response> {
        let mut response: HashMap<String, String> = HashMap::new();
        let now = Utc::now();
        let mut runs_to_try: Vec<DateTime<Utc>> = Vec::new();
        let mut latest_run: Option<DateTime<Utc>> = None;
        let latest_possible_run = gfs::determine_latest_possible_run(now);
        if let Some(latest_possible_run) = latest_possible_run {
            for i in 0..3 {
                runs_to_try.push(
                    latest_possible_run
                        .sub(chrono::Duration::hours((MODEL_HOUR_INTERVAL * i) as i64)),
                );
            }
            for run in runs_to_try {
                if gfs_run_is_complete(run) {
                    latest_run = Some(run);
                    break;
                }
            }
            match latest_run {
                Some(latest) => {
                    response.insert(String::from("latest_run"), latest.to_string());
                    match serde_json::to_string(&response) {
                        Ok(json) => Ok(http::Response::builder()
                            .status(http::StatusCode::OK)
                            .body(Some(json.into()))?),
                        Err(_) => internal_server_error(),
                    }
                }
                None => {
                    println!("Returning 500: No complete runs were found.");
                    internal_server_error()
                }
            }
        } else {
            println!("Returning 500: Unable to determine latest run.");
            internal_server_error()
        }
    }

    pub fn route_gfs_idx(_req: Request, _params: Params) -> Result<Response> {
        let now = Utc::now();
        // TODO: Fix this to actually use latest run or add case to route_gfs_idx_info handler
        let latest_run = dbg!(gfs::determine_latest_possible_run(now));
        if let Some(latest_run) = latest_run {
            let grib_prefix = build_grib_key_prefix(&latest_run);
            let grib_keys = fetch_list_of_grib_keys(&grib_prefix).unwrap();
            let list_result = s3_utils::parse_list_bucket_result(&grib_keys).unwrap();
            match serde_json::to_string(&list_result) {
                Ok(json) => Ok(http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(Some(json.into()))?),
                Err(_) => internal_server_error(),
            }
        } else {
            internal_server_error()
        }
    }

    pub fn route_gfs_idx_info(req: Request, params: Params) -> Result<Response> {
        let year = params
            .get("year")
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();
        let month = params
            .get("month")
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();
        let day = params
            .get("day")
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();
        let hour = params
            .get("hour")
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();
        let forecast = params
            .get("forecast")
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();
        dbg!(&req);
        dbg!(&params);
        let mut query_map: HashMap<String, String> = HashMap::new();
        let uri = req.uri().to_string();
        if let Some(query_string) = uri.split_once('?') {
            dbg!(&query_string);
            query_map = parse_query_string(query_string.1);
            dbg!(&query_map);
        }

        let idx_key = build_grib_idx_key(year, month, day, hour, forecast);
        let idx_data = s3_utils::get_s3_object(S3_BUCKET, &idx_key).unwrap();
        let mut idx_collection = gfs::parse_idx_file(&idx_data).unwrap();

        // Make me an external function
        for (key, value) in query_map {
            match key.as_str() {
                "level" => {
                    idx_collection.records.retain(|entry| entry.level == value);
                }
                "parameter" => {
                    idx_collection
                        .records
                        .retain(|entry| entry.parameter == value.to_uppercase());
                }
                _ => {}
            }
        }

        match serde_json::to_string(&idx_collection.records) {
            Ok(json) => Ok(http::Response::builder()
                .status(http::StatusCode::OK)
                .body(Some(json.into()))?),
            Err(_) => internal_server_error(),
        }
    }

    pub fn route_echo_wildcard(_req: Request, params: Params) -> Result<Response> {
        let capture = params.wildcard().unwrap_or_default();
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(Some(capture.to_string().into()))?)
    }

    const ACCEPTED_QUERY_PARAMS: [&str; 2] = ["level", "parameter"];

    pub fn parse_query_string(query_string: &str) -> HashMap<String, String> {
        let mut query_params: HashMap<String, String> = HashMap::new();
        for param in query_string.split('&') {
            if let Some(pair) = param.split_once('=') {
                let key = pair.0;
                let value = pair.1;
                if !ACCEPTED_QUERY_PARAMS.contains(&key) {
                    continue;
                }
                query_params.insert(key.to_string(), value.to_string());
            }
        }
        query_params
    }
}

mod gfs {
    use super::NUM_EXPECTED_FORECASTS;
    use crate::s3_utils::{
        build_grib_key_prefix, fetch_list_of_grib_keys, parse_list_bucket_result,
    };
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
        let mut prev_start_byte: Option<i32> = None;
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
                stop_byte: prev_start_byte,
                model_run: model_run.to_string(),
                parameter: parameter.to_string(),
                level: level.to_string(),
                forecast_type: forecast_type.to_string(),
            });
            prev_start_byte = start_byte.parse::<i32>().ok();
        }
        idx_collection.records.reverse();
        Ok(idx_collection)
    }
}

mod s3_utils {
    use super::*;
    use anyhow::Result;
    use serde::{Deserialize, Serialize};
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    pub struct ListBucketResult {
        pub name: String,
        pub prefix: String,
        pub key_count: i32,
        pub max_keys: i32,
        pub is_truncated: bool,
        pub contents: Option<Vec<Contents>>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    pub struct Contents {
        pub key: String,
        pub last_modified: String,
        pub e_tag: String,
        pub size: i32,
        pub storage_class: String,
    }

    pub fn parse_list_bucket_result(xml: &str) -> Result<ListBucketResult> {
        match serde_xml_rs::from_str(xml) {
            Ok(list_result) => Ok(list_result),
            Err(e) => Err(e.into()),
        }
    }

    // Builds the prefix for a GFS grib file given a model_run as a date time
    pub fn build_grib_key_prefix(model_run: &DateTime<Utc>) -> String {
        format!(
            "{}",
            model_run.format("gfs.%Y%m%d/%H/atmos/gfs.t%Hz.pgrb2.0p25")
        )
    }

    pub fn fetch_list_of_grib_keys(grib_prefix: &str) -> Result<String> {
        let url = format!("https://{S3_BUCKET}.s3.amazonaws.com/?list-type=2&prefix={grib_prefix}");
        println!("Fetching from URL: {}", url);
        let mut resp = spin_sdk::outbound_http::send_request(
            http::Request::builder().method("GET").uri(url).body(None)?,
        )?;
        let body = resp.body_mut().take().unwrap();
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    pub fn get_s3_object(bucket: &str, key: &str) -> Result<String> {
        let url = format!("https://{bucket}.s3.amazonaws.com/{key}");
        println!("Fetching S3 object from URL: {}", url);
        let mut resp = spin_sdk::outbound_http::send_request(
            http::Request::builder().method("GET").uri(url).body(None)?,
        )?;
        let body = resp.body_mut().take().unwrap();
        Ok(String::from_utf8_lossy(&body).to_string())
    }

    pub fn build_grib_idx_key(year: i32, month: i32, day: i32, hour: i32, forecast: i32) -> String {
        format!("gfs.{year:02}{month:02}{day:02}/{hour:02}/atmos/gfs.t{hour:02}z.pgrb2.0p25.f{forecast:03}.idx")
    }
}
