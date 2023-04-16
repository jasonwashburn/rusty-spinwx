use anyhow::Result;
use chrono::prelude::*;
use chrono::DateTime;
use http::{HeaderMap, HeaderValue};
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
            return_server_error("Could not determine latest run")
        }
    }

    pub fn echo_wildcard(req: Request, params: Params) -> Result<Response> {
        let capture = params.wildcard().unwrap_or_default();
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(Some(capture.to_string().into()))?)
    }

    pub fn return_server_error(message: &str) -> Result<Response> {
        Ok(http::Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(Some(message.to_string().into()))?)
    }
}

mod gfs {
    use chrono::{DateTime, Timelike, Utc};

    pub fn determine_latest_possible_run(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let most_recent_run_hour = (now.hour() / 6) * 6;
        now.with_hour(most_recent_run_hour)?
            .with_minute(0)?
            .with_second(0)?
            .with_nanosecond(0)
    }
}

/// Parses the header to collect provided query parameters
fn parse_query_params(headers: &HeaderMap<HeaderValue>) -> Result<Option<HashMap<&str, &str>>> {
    let mut params = HashMap::new();
    let full_url = headers.get("spin-full-url").unwrap().to_str().unwrap();
    let host = headers.get("host").unwrap().to_str().unwrap();
    let path_info = headers.get("spin-path-info").unwrap().to_str().unwrap();
    let host_with_path = host.to_string() + path_info;
    let query_string =
        full_url.splitn(3, '/').collect::<Vec<&str>>()[2].trim_start_matches(&host_with_path);
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
    format!(
        "{}",
        model_run.format("gfs.%Y%m%d/%H/atmos/gfs.t%Hz.pgrb2.0p25")
    )
}

fn fetch_list_of_grib_keys(grib_prefix: &str) {
    let url = format!("https://{S3_BUCKET}.s3.amazonaws.com/?list-type=2&prefix={grib_prefix}");
}
