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
    router.any("/*", api::route_echo_wildcard);
    router.handle(req)
}

mod api {

    use std::ops::Sub;

    use super::*;
    use gfs::gfs_run_is_complete;
    use s3_utils::{build_grib_key_prefix, fetch_list_of_grib_keys};

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
                        Err(e) => return_server_error(&e.to_string()),
                    }
                }
                None => return_server_error("Unable to determine latest run"),
            }
        } else {
            return_server_error("Could not determine latest run.")
        }
    }

    pub fn route_gfs_idx(_req: Request, _params: Params) -> Result<Response> {
        let mut response: HashMap<String, String> = HashMap::new();
        let now = Utc::now();
        let latest_run = dbg!(gfs::determine_latest_possible_run(now));
        if let Some(latest_run) = latest_run {
            let grib_prefix = build_grib_key_prefix(&latest_run);
            let grib_keys = fetch_list_of_grib_keys(&grib_prefix).unwrap();
            let list_result = s3_utils::parse_list_bucket_result(&grib_keys).unwrap();
            //response.insert(String::from("grib_keys"), grib_keys);
            match serde_json::to_string(&list_result) {
                Ok(json) => Ok(http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(Some(json.into()))?),
                Err(e) => return_server_error(&e.to_string()),
            }
        } else {
            return_server_error("Could not determine latest run.")
        }
    }

    pub fn route_echo_wildcard(_req: Request, params: Params) -> Result<Response> {
        let capture = params.wildcard().unwrap_or_default();
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(Some(capture.to_string().into()))?)
    }

    pub fn return_server_error(message: &str) -> Result<Response> {
        let mut response = HashMap::new();
        response.insert("error".to_string(), message.to_string());
        let response = serde_json::to_string(&response)?;
        Ok(http::Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(Some(response.into()))?)
    }
}

mod gfs {

    use super::NUM_EXPECTED_FORECASTS;
    use crate::s3_utils::{
        build_grib_key_prefix, fetch_list_of_grib_keys, parse_list_bucket_result,
    };
    use chrono::{DateTime, Timelike, Utc};

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
}
