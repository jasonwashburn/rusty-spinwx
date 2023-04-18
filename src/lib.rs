use anyhow::Result;
use chrono::prelude::*;
use chrono::DateTime;
use spin_sdk::{
    http::{Params, Request, Response, Router},
    http_component,
};
use std::collections::HashMap;

mod gfs;
mod s3_utils;

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
    router.get(
        "/gfs/grib/:year/:month/:day/:hour/:forecast/:parameter/:level",
        api::route_gfs_grib,
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

        // Make me an external function
        let idx_key = build_grib_idx_key(year, month, day, hour, forecast);
        let idx_data = s3_utils::get_s3_object(S3_BUCKET, &idx_key, None).unwrap();
        let mut idx_collection = gfs::parse_idx_file(&idx_data).unwrap();

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
        //

        match serde_json::to_string(&idx_collection.records) {
            Ok(json) => Ok(http::Response::builder()
                .status(http::StatusCode::OK)
                .body(Some(json.into()))?),
            Err(_) => internal_server_error(),
        }
    }

    pub fn route_gfs_grib(req: Request, params: Params) -> Result<Response> {
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
        let level = params
            .get("level")
            .unwrap_or_default()
            .parse::<String>()
            .unwrap_or_default();
        let parameter = params
            .get("parameter")
            .unwrap_or_default()
            .parse::<String>()
            .unwrap_or_default();

        let idx_key = build_grib_idx_key(year, month, day, hour, forecast);
        let idx_data = s3_utils::get_s3_object(S3_BUCKET, &idx_key, None).unwrap();
        let mut idx_collection = gfs::parse_idx_file(&idx_data).unwrap();

        idx_collection
            .records
            .retain(|record| record.level == level.to_lowercase());
        idx_collection
            .records
            .retain(|record| record.parameter == parameter.to_uppercase());

        let grib_record = idx_collection.records.first().unwrap();
        let start_byte = grib_record.start_byte;
        let stop_byte = grib_record.stop_byte;
        let byte_range = (start_byte, stop_byte);

        let grib_key = s3_utils::build_grib_file_key(year, month, day, hour, forecast);
        let grib_data = s3_utils::get_s3_object(S3_BUCKET, &grib_key, Some(byte_range)).unwrap();
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(Some(grib_data.into()))?)
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
