use std::{collections::HashMap};
use chrono::{DateTime};
use chrono::prelude::*;
use anyhow::Result;
use http::{HeaderMap, HeaderValue};
use spin_sdk::{
    http::{Request, Response},
    http_component,
};

const MODEL_HOUR_INTERVAL: i32 = 6;
const NUM_EXPECTED_FORECASTS: i32 = 209;
const S3_BUCKET: &str = "noaa-gfs-bdp-pds";

/// A simple Spin HTTP component.
#[http_component]
fn rusty_spinwx(req: Request) -> Result<Response> {
    if let Some(query_params) = parse_query_params(req.headers()).unwrap() {
        dbg!(query_params);
    }
    let today = chrono::Utc::now();
    println!("{}", build_s3_key_prefix_for_grib(&today));
    Ok(http::Response::builder()
        .status(200)
        .header("foo", "bar")
        .body(Some("Hello, Fermyon".into()))?)
}

/// Parses the header to collect provided query parameters
fn parse_query_params(headers: &HeaderMap<HeaderValue>) -> Result<Option<HashMap<&str, &str>>> {
    let mut params = HashMap::new();
    let full_url = headers.get("spin-full-url").unwrap().to_str().unwrap();
    let host = headers.get("host").unwrap().to_str().unwrap();
    let path_info = headers.get("spin-path-info").unwrap().to_str().unwrap();
    let host_with_path = host.to_string() + path_info;
    let query_string = full_url.splitn(3, '/').collect::<Vec<&str>>()[2].trim_start_matches(&host_with_path);
    if query_string.starts_with('?') {
        for pair in query_string.trim_start_matches('?').split('&') {

            let (key, value) = pair.split_once('=').unwrap();
            params.insert(key, value);

        }
    }
    Ok(Some(params))
}

// Builds the prefix for a GFS grib file given a model_run as a date time
fn build_s3_key_prefix_for_grib(model_run: &DateTime<Utc>) -> String {
    format!("{}", model_run.format("gfs.%Y%m%d/%H/atmos/gfs.t%Hz.pgrb2.0p25"))
}

fn fetch_list_of_grib_keys(grib_prefix: &str) {
    let url = format!("https://{S3_BUCKET}.s3.amazonaws.com/?list-type=2&prefix={grib_prefix}");
}
