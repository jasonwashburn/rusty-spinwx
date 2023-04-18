use super::*;
use anyhow::Result;
use http::request::Builder;
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

pub fn get_s3_object(
    bucket: &str,
    key: &str,
    byte_range: Option<(i32, Option<i32>)>,
) -> Result<String> {
    let url = format!("https://{bucket}.s3.amazonaws.com/{key}");
    println!("Fetching S3 object from URL: {}", url);
    let mut request: Request = http::Request::builder().method("GET").uri(url).body(None)?;
    if let Some(byte_range) = byte_range {
        request.headers_mut().insert(
            "Range",
            format!(
                "bytes={start}-{stop}",
                start = byte_range.0,
                stop = byte_range.1.unwrap()
            )
            .parse()
            .unwrap(),
        );
    }
    let mut resp = spin_sdk::outbound_http::send_request(request)?;
    let body = resp.body_mut().take().unwrap();
    Ok(String::from_utf8_lossy(&body).to_string())
}

pub fn build_grib_idx_key(year: i32, month: i32, day: i32, hour: i32, forecast: i32) -> String {
    format!("gfs.{year:02}{month:02}{day:02}/{hour:02}/atmos/gfs.t{hour:02}z.pgrb2.0p25.f{forecast:03}.idx")
}

pub fn build_grib_file_key(year: i32, month: i32, day: i32, hour: i32, forecast: i32) -> String {
    format!(
        "gfs.{year:02}{month:02}{day:02}/{hour:02}/atmos/gfs.t{hour:02}z.pgrb2.0p25.f{forecast:03}"
    )
}
