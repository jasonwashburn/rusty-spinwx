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
    router.get("/gfs/latest", api::gfs_latest);
    router.get("/gfs/idx", api::gfs_idx);
    router.any("/*", api::echo_wildcard);
    router.handle(req)
}

mod api {

    use super::*;

    pub fn gfs_latest(req: Request, params: Params) -> Result<Response> {
        let mut response: HashMap<String, String> = HashMap::new();
        let now = Utc::now();
        let latest_run = gfs::determine_latest_possible_run(now);
        if let Some(latest_run) = latest_run {
            response.insert(String::from("latest_run"), latest_run.to_string());
            match serde_json::to_string(&response) {
                Ok(json) => Ok(http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(Some(json.into()))?),
                Err(e) => return_server_error(&e.to_string()),
            }
        } else {
            return_server_error("Could not determine latest run.")
        }
    }

    pub fn gfs_idx(req: Request, params: Params) -> Result<Response> {
        let mut response: HashMap<String, String> = HashMap::new();
        let now = Utc::now();
        let latest_run = dbg!(gfs::determine_latest_possible_run(now));
        if let Some(latest_run) = latest_run {
            let grib_prefix = build_grib_key_prefix(&latest_run);
            let grib_keys = fetch_list_of_grib_keys(&grib_prefix).unwrap();
            let list_result = s3_utils::parse_list_bucket_result(&grib_keys).unwrap();
            let available_forecasts: Vec<_> = list_result
                .contents
                .iter()
                .filter(|content| (!content.key.ends_with(".anl") & !content.key.ends_with(".idx")))
                .collect();
            println!(
                "S3 ListBucketResult contains {} keys.",
                available_forecasts.len()
            );
            response.insert(String::from("grib_keys"), grib_keys);
            match serde_json::to_string(&response) {
                Ok(json) => Ok(http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(Some(json.into()))?),
                Err(e) => return_server_error(&e.to_string()),
            }
        } else {
            return_server_error("Could not determine latest run.")
        }
    }

    pub fn echo_wildcard(req: Request, params: Params) -> Result<Response> {
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

    use chrono::{DateTime, Timelike, Utc};

    pub fn determine_latest_possible_run(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let most_recent_run_hour = dbg!(((now.hour() / 6) * 6) - 6);
        println!("Remove this!! Latest hour fixed for troubleshooting");
        now.with_hour(most_recent_run_hour)?
            .with_minute(0)?
            .with_second(0)?
            .with_nanosecond(0)
    }
}

mod s3_utils {
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
        pub contents: Vec<Contents>,
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
}

// Builds the prefix for a GFS grib file given a model_run as a date time
fn build_grib_key_prefix(model_run: &DateTime<Utc>) -> String {
    format!(
        "{}",
        model_run.format("gfs.%Y%m%d/%H/atmos/gfs.t%Hz.pgrb2.0p25")
    )
}

fn fetch_list_of_grib_keys(grib_prefix: &str) -> Result<String> {
    let url = format!("https://{S3_BUCKET}.s3.amazonaws.com/?list-type=2&prefix={grib_prefix}");
    println!("Fetching from URL: {}", url);
    let mut resp = spin_sdk::outbound_http::send_request(
        http::Request::builder().method("GET").uri(url).body(None)?,
    )?;
    let body = resp.body_mut().take().unwrap();
    Ok(String::from_utf8_lossy(&body).to_string())
}
