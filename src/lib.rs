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
    use std::ops::Sub;

    use chrono::{DateTime, Duration, Timelike, Utc};

    pub fn determine_latest_possible_run(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let most_recent_run_hour = dbg!(((now.hour() / 6) * 6) - 6);
        println!("Remove this!! Latest hour fixed for troubleshooting");
        now.with_hour(most_recent_run_hour)?
            .with_minute(0)?
            .with_second(0)?
            .with_nanosecond(0)
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
    let mut resp = spin_sdk::outbound_http::send_request(
        http::Request::builder().method("GET").uri(url).body(None)?,
    )?;
    let body = resp.body_mut().take().unwrap();
    Ok(String::from_utf8_lossy(&body).to_string())
}
