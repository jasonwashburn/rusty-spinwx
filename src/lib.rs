use std::{collections::HashMap, path};

use anyhow::Result;
use http::{HeaderMap, HeaderValue};
use spin_sdk::{
    http::{Request, Response},
    http_component,
};

/// A simple Spin HTTP component.
#[http_component]
fn rusty_spinwx(req: Request) -> Result<Response> {
    if let Some(query_params) = parse_query_params(req.headers()).unwrap() {
        dbg!(query_params);
    }
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
